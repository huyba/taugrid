# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Prototype of the TauGrid managed-storage runtime contract.

This module is what `tau.storage` in the Python SDK would provide once the
managed storage design lands. It is deliberately framework-agnostic: the
training script hands it a callback that writes files into a directory.

Contract implemented here (see docs/design/managed-storage.md, section 4.2):

* Hot tier  ($TAU_HOT_DIR, node-local): fast, may vanish with the node.
* Durable tier ($TAU_DATA_DIR or a Blob container): survives pods and nodes.
* A checkpoint is valid if and only if its commit marker exists. The marker
  is written last, with create-if-absent semantics, and lists every file
  with its size and sha256. Readers ignore any step without a valid marker,
  so torn uploads, crashed writers and zombie writers can never be resumed
  from.
* Uploads to the durable tier can run in a background thread so the training
  loop only pays for the local hot-tier write.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Protocol

MARKER = "_COMMITTED.json"
STEP_FMT = "step-{:08d}"


# --------------------------------------------------------------------------
# Durable store backends
# --------------------------------------------------------------------------


class DurableStore(Protocol):
    name: str

    def put_file(self, local: Path, key: str) -> None: ...

    def put_bytes(self, key: str, data: bytes, if_absent: bool = False) -> bool: ...

    def get_bytes(self, key: str) -> bytes | None: ...

    def get_file(self, key: str, local: Path) -> None: ...

    def list_prefix(self, prefix: str) -> list[str]: ...

    def delete(self, key: str) -> None: ...


class PosixStore:
    """Durable tier reached through a mounted filesystem (blobfuse2, Lustre, NFS).

    Single-file writes go to a temp name, are fsynced, then renamed. Creating a
    marker uses O_EXCL so that a second writer for the same step loses.
    """

    name = "posix"

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _p(self, key: str) -> Path:
        return self.root / key

    def put_file(self, local: Path, key: str) -> None:
        dst = self._p(key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_name(dst.name + f".tmp.{os.getpid()}")
        with open(local, "rb") as src, open(tmp, "wb") as out:
            shutil.copyfileobj(src, out, 8 << 20)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, dst)

    def put_bytes(self, key: str, data: bytes, if_absent: bool = False) -> bool:
        dst = self._p(key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if if_absent:
            try:
                fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            except FileExistsError:
                return False
            with os.fdopen(fd, "wb") as out:
                out.write(data)
                out.flush()
                os.fsync(out.fileno())
            return True
        tmp = dst.with_name(dst.name + f".tmp.{os.getpid()}")
        tmp.write_bytes(data)
        os.replace(tmp, dst)
        return True

    def get_bytes(self, key: str) -> bytes | None:
        try:
            return self._p(key).read_bytes()
        except FileNotFoundError:
            return None

    def get_file(self, key: str, local: Path) -> None:
        local.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self._p(key), local)

    def list_prefix(self, prefix: str) -> list[str]:
        base = self._p(prefix)
        if not base.exists():
            return []
        return sorted(
            str(p.relative_to(self.root)).replace(os.sep, "/")
            for p in base.rglob("*")
            if p.is_file() and ".tmp." not in p.name
        )

    def delete(self, key: str) -> None:
        try:
            self._p(key).unlink()
        except FileNotFoundError:
            pass


class BlobStore:
    """Durable tier reached directly through the Blob API (no FUSE).

    This is the path the CLI and the SDK take for `tau storage` operations and,
    optionally, for checkpoint uploads. Create-if-absent maps onto the
    server-enforced `If-None-Match: *` precondition, which is the same
    primitive cli/internal/cli/dataset_sdk_backend.go already uses for
    immutable dataset registry records.
    """

    name = "blob"

    def __init__(self, conn_str: str, container: str, prefix: str = ""):
        from azure.storage.blob import BlobServiceClient

        svc = BlobServiceClient.from_connection_string(conn_str)
        self.cc = svc.get_container_client(container)
        try:
            self.cc.create_container()
        except Exception:  # already exists
            pass
        self.prefix = prefix.strip("/")

    def _k(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put_file(self, local: Path, key: str) -> None:
        with open(local, "rb") as f:
            self.cc.upload_blob(self._k(key), f, overwrite=True, max_concurrency=4)

    def put_bytes(self, key: str, data: bytes, if_absent: bool = False) -> bool:
        from azure.core.exceptions import ResourceExistsError

        try:
            # overwrite=False sends If-None-Match: * (server-enforced create).
            self.cc.upload_blob(self._k(key), data, overwrite=not if_absent)
            return True
        except ResourceExistsError:
            return False

    def get_bytes(self, key: str) -> bytes | None:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            return self.cc.download_blob(self._k(key)).readall()
        except ResourceNotFoundError:
            return None

    def get_file(self, key: str, local: Path) -> None:
        local.parent.mkdir(parents=True, exist_ok=True)
        with open(local, "wb") as f:
            self.cc.download_blob(self._k(key), max_concurrency=4).readinto(f)

    def list_prefix(self, prefix: str) -> list[str]:
        full = self._k(prefix)
        cut = len(self.prefix) + 1 if self.prefix else 0
        return sorted(b.name[cut:] for b in self.cc.list_blobs(name_starts_with=full))

    def delete(self, key: str) -> None:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            self.cc.delete_blob(self._k(key))
        except ResourceNotFoundError:
            pass


def store_from_env() -> DurableStore:
    """Resolve the durable tier the way the rendered pod environment would."""
    backend = os.environ.get("TAU_STORAGE_BACKEND", "posix")
    if backend == "blob":
        return BlobStore(
            os.environ["TAU_STORAGE_CONNECTION_STRING"],
            os.environ["TAU_STORAGE_CONTAINER"],
            os.environ.get("TAU_STORAGE_PREFIX", ""),
        )
    return PosixStore(os.environ.get("TAU_DATA_DIR", "/data"))


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fsync_dir(d: Path) -> None:
    fd = os.open(d, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _maybe_crash(point: str, step: int) -> None:
    """Fault injection for the prototype scenarios.

    TAU_PROTO_CRASH="<point>:<step>" kills the process with SIGKILL semantics
    (os._exit, no cleanup, no atexit) at the named point.
    """
    spec = os.environ.get("TAU_PROTO_CRASH", "")
    if not spec:
        return
    want_point, _, want_step = spec.partition(":")
    if want_point == point and int(want_step or -1) == step:
        print(f"[fault] crashing at {point} step={step}", flush=True)
        os._exit(137)


# --------------------------------------------------------------------------
# Datasets: immutable, versioned, committed with the same marker protocol
# --------------------------------------------------------------------------


def publish_dataset(store: DurableStore, name: str, version: str, files: dict[str, Path]) -> dict:
    prefix = f"datasets/{name}/{version}"
    manifest = {"name": name, "version": version, "files": {}}
    for rel, local in files.items():
        store.put_file(local, f"{prefix}/{rel}")
        manifest["files"][rel] = {"size": local.stat().st_size, "sha256": sha256_file(local)}
    ok = store.put_bytes(f"{prefix}/{MARKER}", json.dumps(manifest, indent=2).encode(), if_absent=True)
    if not ok:
        raise FileExistsError(f"dataset {name}@{version} already committed; versions are immutable")
    return manifest


def stage_dataset(store: DurableStore, name: str, version: str, hot_dir: Path) -> tuple[Path, dict]:
    """Copy a committed dataset version into the hot tier, verifying hashes.

    A second call on the same node is a cache hit (no durable reads).
    """
    prefix = f"datasets/{name}/{version}"
    raw = store.get_bytes(f"{prefix}/{MARKER}")
    if raw is None:
        raise FileNotFoundError(f"dataset {name}@{version} has no commit marker; refusing to read it")
    manifest = json.loads(raw)
    dst = hot_dir / "datasets" / name / version
    stats = {"cache_hits": 0, "downloaded": 0}
    for rel, meta in manifest["files"].items():
        local = dst / rel
        if local.exists() and local.stat().st_size == meta["size"] and sha256_file(local) == meta["sha256"]:
            stats["cache_hits"] += 1
            continue
        store.get_file(f"{prefix}/{rel}", local)
        if sha256_file(local) != meta["sha256"]:
            raise IOError(f"dataset {name}@{version}: {rel} failed sha256 verification")
        stats["downloaded"] += 1
    return dst, stats


# --------------------------------------------------------------------------
# Checkpoints
# --------------------------------------------------------------------------


@dataclass
class CommitStats:
    step: int
    hot_write_s: float = 0.0
    durable_commit_s: float = 0.0
    blocking_s: float = 0.0
    bytes: int = 0
    committed: bool = False


@dataclass
class CheckpointManager:
    store: DurableStore
    hot_dir: Path
    run: str
    keep: int = 3
    async_commit: bool = True
    verify_on_resume: bool = True
    stats: list[CommitStats] = field(default_factory=list)

    def __post_init__(self):
        self.hot_root = Path(self.hot_dir) / "checkpoints" / self.run
        self.hot_root.mkdir(parents=True, exist_ok=True)
        self.durable_root = f"checkpoints/{self.run}"
        self._pending: threading.Thread | None = None
        self._error: BaseException | None = None

    # ---- save -------------------------------------------------------------

    def save(self, step: int, write_fn: Callable[[Path], None]) -> CommitStats:
        """Write a checkpoint to the hot tier, then commit it to durable.

        Only the hot-tier write blocks the caller when async_commit is True.
        At most one durable commit is in flight; a new save waits for the
        previous one, which bounds hot-tier usage and keeps commits ordered.
        """
        t0 = time.perf_counter()
        self.wait()
        st = CommitStats(step=step)
        name = STEP_FMT.format(step)
        partial = self.hot_root / (name + ".partial")
        final = self.hot_root / name
        shutil.rmtree(partial, ignore_errors=True)
        partial.mkdir(parents=True)
        write_fn(partial)
        for f in partial.iterdir():
            with open(f, "rb") as fh:
                os.fsync(fh.fileno())
        shutil.rmtree(final, ignore_errors=True)
        os.replace(partial, final)  # atomic on the local filesystem
        _fsync_dir(self.hot_root)
        st.hot_write_s = time.perf_counter() - t0
        st.bytes = sum(f.stat().st_size for f in final.iterdir())

        if self.async_commit:
            self._pending = threading.Thread(target=self._commit_guarded, args=(final, st), daemon=False)
            self._pending.start()
        else:
            self._commit(final, st)
        st.blocking_s = time.perf_counter() - t0
        self.stats.append(st)
        return st

    def _commit_guarded(self, final: Path, st: CommitStats) -> None:
        try:
            self._commit(final, st)
        except BaseException as e:  # surfaced on the next wait()
            self._error = e

    def _commit(self, final: Path, st: CommitStats) -> None:
        t0 = time.perf_counter()
        name = final.name
        prefix = f"{self.durable_root}/{name}"
        attempt = os.environ.get("TAU_RUN_ATTEMPT", "0")
        # Data goes to a writer-unique generation path. The commit marker is
        # the only name two writers can contend on, so a zombie or duplicate
        # writer can never clobber bytes that a committed marker points at.
        generation = f"g-{attempt}-{uuid.uuid4().hex[:8]}"
        manifest = {
            "step": st.step,
            "run": self.run,
            "attempt": attempt,
            "generation": generation,
            "created_at": time.time(),
            "files": {},
        }
        for f in sorted(final.iterdir()):
            self.store.put_file(f, f"{prefix}/{generation}/{f.name}")
            manifest["files"][f.name] = {"size": f.stat().st_size, "sha256": sha256_file(f)}
        _maybe_crash("after_upload_before_marker", st.step)
        ok = self.store.put_bytes(f"{prefix}/{MARKER}", json.dumps(manifest, indent=2).encode(), if_absent=True)
        st.committed = ok
        if not ok:
            # Another writer already committed this step (zombie or duplicate
            # attempt). Immutable markers mean we never overwrite it.
            print(f"[ckpt] step {st.step} already committed by another writer; not overwriting", flush=True)
        _maybe_crash("after_marker", st.step)
        st.durable_commit_s = time.perf_counter() - t0
        self._retain()

    def wait(self) -> None:
        if self._pending is not None:
            self._pending.join()
            self._pending = None
        if self._error is not None:
            err, self._error = self._error, None
            raise RuntimeError("durable checkpoint commit failed") from err

    def _retain(self) -> None:
        steps = self.committed_steps()
        self.gc_orphan_generations(steps[-self.keep:])
        for step in steps[: max(0, len(steps) - self.keep)]:
            prefix = f"{self.durable_root}/{STEP_FMT.format(step)}"
            # Delete the marker first: a reader then sees "not committed"
            # rather than a marker that points at missing files.
            self.store.delete(f"{prefix}/{MARKER}")
            for key in self.store.list_prefix(prefix + "/"):
                self.store.delete(key)
        for d in sorted(self.hot_root.glob("step-*")):
            if d.is_dir() and not d.name.endswith(".partial"):
                if int(d.name.split("-")[1]) not in steps[-self.keep:]:
                    shutil.rmtree(d, ignore_errors=True)

    def gc_orphan_generations(self, steps: list[int]) -> int:
        """Delete generations of committed steps that the marker does not name.

        These are leftovers from torn uploads or losing writers. Uncommitted
        steps are left alone here; a later commit of the same step or
        `tau storage gc --older-than` removes them.
        """
        removed = 0
        for step in steps:
            prefix = f"{self.durable_root}/{STEP_FMT.format(step)}"
            raw = self.store.get_bytes(f"{prefix}/{MARKER}")
            if raw is None:
                continue
            keep = json.loads(raw)["generation"]
            for key in self.store.list_prefix(prefix + "/"):
                rel = key[len(prefix) + 1:]
                if rel != MARKER and not rel.startswith(keep + "/"):
                    self.store.delete(key)
                    removed += 1
        return removed

    # ---- resume ----------------------------------------------------------

    def _step_of(self, key: str) -> tuple[int, str] | None:
        rel = key[len(self.durable_root) + 1:]
        head, _, rest = rel.partition("/")
        if not head.startswith("step-") or not rest:
            return None
        return int(head.split("-")[1]), rest

    def committed_steps(self) -> list[int]:
        steps = []
        for key in self.store.list_prefix(self.durable_root + "/"):
            parsed = self._step_of(key)
            if parsed and parsed[1] == MARKER:
                steps.append(parsed[0])
        return sorted(steps)

    def uncommitted_steps(self) -> list[int]:
        """Steps that have data in durable storage but no commit marker."""
        seen = set()
        for key in self.store.list_prefix(self.durable_root + "/"):
            parsed = self._step_of(key)
            if parsed:
                seen.add(parsed[0])
        return sorted(seen - set(self.committed_steps()))

    def restore_latest(self) -> tuple[int, Path] | None:
        """Return (step, local_dir) for the newest valid committed checkpoint.

        Prefers a verified copy already on the hot tier (same node restart)
        and otherwise downloads from durable. Invalid candidates are skipped,
        newest first.
        """
        for step in reversed(self.committed_steps()):
            name = STEP_FMT.format(step)
            prefix = f"{self.durable_root}/{name}"
            manifest = json.loads(self.store.get_bytes(f"{prefix}/{MARKER}"))
            local = self.hot_root / name
            if self._valid_local(local, manifest):
                print(f"[ckpt] resume step {step} from hot tier (cache hit)", flush=True)
                return step, local
            shutil.rmtree(local, ignore_errors=True)
            try:
                for fname in manifest["files"]:
                    self.store.get_file(f"{prefix}/{manifest['generation']}/{fname}", local / fname)
            except Exception as e:  # missing object: fall back to older step
                print(f"[ckpt] step {step} unreadable ({e}); trying older", flush=True)
                continue
            if self._valid_local(local, manifest):
                print(f"[ckpt] resume step {step} from durable tier", flush=True)
                return step, local
            print(f"[ckpt] step {step} failed verification; trying older", flush=True)
        return None

    def _valid_local(self, local: Path, manifest: dict) -> bool:
        if not local.is_dir():
            return False
        for fname, meta in manifest["files"].items():
            p = local / fname
            if not p.exists() or p.stat().st_size != meta["size"]:
                return False
            if self.verify_on_resume and sha256_file(p) != meta["sha256"]:
                return False
        return True

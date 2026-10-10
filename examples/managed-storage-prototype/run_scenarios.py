# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Drive the managed-storage prototype through success and failure paths.

Each scenario runs the real training script as a subprocess with the pod
environment TauGrid would render (TAU_DATA_DIR / TAU_HOT_DIR / backend vars),
injects faults with SIGKILL semantics, and asserts the storage contract.

Backends:
  posix  durable tier is a mounted directory (stands in for blobfuse2/Lustre)
  blob   durable tier is the Blob API, served locally by Azurite

Usage:
  python run_scenarios.py --backend posix --backend blob --out results.json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


class Env:
    def __init__(self, backend: str, work: Path):
        self.backend = backend
        self.work = work / backend
        shutil.rmtree(self.work, ignore_errors=True)
        self.work.mkdir(parents=True)
        self.durable = self.work / "durable"
        self.container = f"ws-research-{int(time.time())}"

    def base(self, hot: str) -> dict:
        e = dict(os.environ)
        e.update({
            "PYTHONPATH": str(HERE),
            "TAU_HOT_DIR": str(self.work / hot),
            "TAU_DATA_DIR": str(self.durable),
            "TAU_STORAGE_BACKEND": self.backend,
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
        })
        if self.backend == "blob":
            e.update({"TAU_STORAGE_CONNECTION_STRING": AZURITE_CONN, "TAU_STORAGE_CONTAINER": self.container})
        e.pop("TAU_PROTO_CRASH", None)
        return e

    def store(self):
        sys.path.insert(0, str(HERE))
        from tau_storage import BlobStore, PosixStore

        if self.backend == "blob":
            return BlobStore(AZURITE_CONN, self.container)
        return PosixStore(self.durable)


def run(cmd: list[str], env: dict, expect_rc: int = 0) -> tuple[int, str]:
    p = subprocess.run(cmd, env=env, cwd=HERE, capture_output=True, text=True)
    out = p.stdout + p.stderr
    if p.returncode != expect_rc:
        raise AssertionError(f"{' '.join(cmd)} exited {p.returncode}, expected {expect_rc}\n{out[-3000:]}")
    return p.returncode, out


def train(env: Env, run_name: str, hot: str, *extra: str, crash: str = "", attempt: int = 0,
          expect_rc: int = 0, torchrun: int = 0) -> str:
    e = env.base(hot)
    e["TAU_RUN_NAME"] = run_name
    e["TAU_RUN_ATTEMPT"] = str(attempt)
    if crash:
        e["TAU_PROTO_CRASH"] = crash
    base = [sys.executable, "train.py"]
    if torchrun:
        base = [sys.executable, "-m", "torch.distributed.run", "--standalone", f"--nproc_per_node={torchrun}", "train.py"]
    _, out = run(base + ["--run", run_name, *extra], e, expect_rc)
    return out


def result(env: Env, run_name: str, attempt: int) -> dict:
    raw = env.store().get_bytes(f"runs/{run_name}/result-attempt{attempt}.json")
    assert raw, f"missing result for {run_name} attempt {attempt}"
    return json.loads(raw)


def scenario(name):
    def deco(fn):
        fn.scenario = name
        return fn
    return deco


@scenario("S1 success: stage dataset, train, async commits, retention")
def s1(env: Env, ctx: dict):
    run([sys.executable, "prepare_data.py"], env.base("hot-prep"))
    train(env, "s1", "hot-s1", "--steps", "200", "--ckpt-every", "50")
    r = result(env, "s1", 0)
    assert r["committed_steps"] == [100, 150, 200], r["committed_steps"]
    assert r["log"][-1]["val_loss"] < r["log"][0]["val_loss"], "loss did not improve"
    ctx["async"] = r
    return {"committed": r["committed_steps"], "val_loss_first": round(r["log"][0]["val_loss"], 3),
            "val_loss_last": round(r["log"][-1]["val_loss"], 3),
            "median_ckpt_blocking_ms": round(1000 * statistics.median(c["blocking_s"] for c in r["commits"]), 1),
            "median_durable_commit_ms": round(1000 * statistics.median(c["durable_commit_s"] for c in r["commits"]), 1),
            "ckpt_mb": round(r["log"][0]["ckpt_bytes"] / 1e6, 2)}


@scenario("S2 torn upload: crash after data upload, before commit marker")
def s2(env: Env, ctx: dict):
    out = train(env, "s2", "hot-s2", "--steps", "200", "--ckpt-every", "50",
                crash="after_upload_before_marker:150", expect_rc=137)
    assert "[fault]" in out
    from tau_storage import CheckpointManager

    m = CheckpointManager(env.store(), env.work / "inspect", "s2")
    before = {"committed": m.committed_steps(), "torn": m.uncommitted_steps()}
    assert before["committed"] == [50, 100] and before["torn"] == [150], before
    train(env, "s2", "hot-s2", "--steps", "200", "--ckpt-every", "50", attempt=1)
    r = result(env, "s2", 1)
    assert r["resumed_from"] == 100, r["resumed_from"]
    assert r["committed_steps"] == [100, 150, 200], r["committed_steps"]
    return {"after_crash": before, "resumed_from": r["resumed_from"], "final_committed": r["committed_steps"]}


@scenario("S3 node loss: committed checkpoint, hot tier wiped, resume from durable")
def s3(env: Env, ctx: dict):
    train(env, "s3", "hot-s3a", "--steps", "200", "--ckpt-every", "50",
          crash="after_marker:100", expect_rc=137)
    # Same-node restart: verified hot copy is reused (no durable read).
    out_same = train(env, "s3", "hot-s3a", "--steps", "150", "--ckpt-every", "50", attempt=1)
    assert "from hot tier (cache hit)" in out_same, out_same[-800:]
    # New node: empty hot tier, must restore from durable.
    out_new = train(env, "s3", "hot-s3b-new-node", "--steps", "200", "--ckpt-every", "50", attempt=2)
    assert "from durable tier" in out_new, out_new[-800:]
    r = result(env, "s3", 2)
    assert r["resumed_from"] == 150, r["resumed_from"]
    return {"same_node_restart": "hot cache hit", "new_node_restart": "durable restore",
            "resumed_from": r["resumed_from"], "dataset_stage_new_node": r["dataset_stage"]}


@scenario("S4 zombie writer: second commit of an existing step is refused")
def s4(env: Env, ctx: dict):
    from tau_storage import MARKER, CheckpointManager

    store = env.store()
    m1 = CheckpointManager(store, env.work / "hot-s4a", "s4", async_commit=False)
    m1.save(10, lambda d: (d / "state.bin").write_bytes(b"writer-A" * 1000))
    os.environ["TAU_RUN_ATTEMPT"] = "zombie"
    m2 = CheckpointManager(store, env.work / "hot-s4b", "s4", async_commit=False)
    st = m2.save(10, lambda d: (d / "state.bin").write_bytes(b"writer-B" * 1000))
    os.environ.pop("TAU_RUN_ATTEMPT")
    marker = json.loads(store.get_bytes(f"checkpoints/s4/step-00000010/{MARKER}"))
    assert st.committed is False and marker["attempt"] == "0", marker
    # The zombie's bytes landed in its own generation path, so writer A's
    # committed checkpoint is intact and restores byte-for-byte.
    restored = CheckpointManager(store, env.work / "hot-s4c", "s4").restore_latest()
    assert restored and (restored[1] / "state.bin").read_bytes().startswith(b"writer-A"), restored
    # Retention already collected the zombie's generation (it is named by no
    # marker), so only the marker and writer A's generation remain.
    gens = sorted({k.split("/")[3] for k in store.list_prefix("checkpoints/s4/step-00000010/") if k.count("/") >= 4})
    assert gens == [marker["generation"]], gens
    return {"second_commit_accepted": st.committed, "marker_owner_attempt": marker["attempt"],
            "restored_content": "writer-A (intact)", "generations_left": len(gens)}


@scenario("S5 sync vs async durable commit (training stall per checkpoint)")
def s5(env: Env, ctx: dict):
    train(env, "s5", "hot-s5", "--steps", "200", "--ckpt-every", "50", "--sync-commit")
    r = result(env, "s5", 0)
    a = ctx["async"]
    sync_ms = 1000 * statistics.median(c["blocking_s"] for c in r["commits"])
    async_ms = 1000 * statistics.median(c["blocking_s"] for c in a["commits"])
    return {"median_block_sync_ms": round(sync_ms, 1), "median_block_async_ms": round(async_ms, 1)}


@scenario("S6 dataset immutability and verification")
def s6(env: Env, ctx: dict):
    _, out = run([sys.executable, "prepare_data.py"], env.base("hot-prep"))
    assert "already committed" in out, out
    from tau_storage import stage_dataset

    store = env.store()
    hot = env.work / "hot-s6"
    d, first = stage_dataset(store, "pystdlib-bytes", "v1", hot)
    _, second = stage_dataset(store, "pystdlib-bytes", "v1", hot)
    victim = sorted(d.glob("train-*.bin"))[0]
    victim.write_bytes(b"corrupted")
    _, third = stage_dataset(store, "pystdlib-bytes", "v1", hot)
    assert second["cache_hits"] == 5 and third["downloaded"] == 1
    try:
        stage_dataset(store, "pystdlib-bytes", "v2-not-committed", hot)
        raise AssertionError("uncommitted dataset must not be readable")
    except FileNotFoundError:
        pass
    return {"republish_v1": "refused", "first_stage": first, "second_stage": second,
            "after_local_corruption": third, "uncommitted_version": "refused"}


@scenario("S7 distributed (torchrun, 2 ranks, gloo): crash and resume, rank-0 writer")
def s7(env: Env, ctx: dict):
    train(env, "s7", "hot-s7", "--steps", "100", "--ckpt-every", "25",
          crash="after_upload_before_marker:75", expect_rc=1, torchrun=2)
    train(env, "s7", "hot-s7", "--steps", "100", "--ckpt-every", "25", attempt=1, torchrun=2)
    r = result(env, "s7", 1)
    assert r["world"] == 2 and r["resumed_from"] == 50, r
    return {"world": r["world"], "resumed_from": r["resumed_from"], "final_committed": r["committed_steps"]}


SCENARIOS = [s1, s2, s3, s4, s5, s6, s7]


def azurite_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 10000), timeout=1):
            return True
    except OSError:
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", action="append", choices=["posix", "blob"])
    ap.add_argument("--work", default=str(HERE / ".work"))
    ap.add_argument("--out", default=str(HERE / "results.json"))
    ap.add_argument("--only", nargs="*", default=None)
    args = ap.parse_args()
    backends = args.backend or ["posix"]
    report = {"python": sys.version.split()[0], "backends": {}}
    try:
        import torch

        report["torch"] = torch.__version__
    except ImportError:
        pass
    failed = 0
    for backend in backends:
        if backend == "blob" and not azurite_up():
            print("azurite not listening on 127.0.0.1:10000; start it with: azurite-blob --inMemoryPersistence")
            return 2
        env = Env(backend, Path(args.work))
        ctx: dict = {}
        report["backends"][backend] = {}
        for fn in SCENARIOS:
            if args.only and fn.__name__ not in args.only:
                continue
            t0 = time.perf_counter()
            try:
                detail = fn(env, ctx)
                status = "PASS"
            except Exception as e:  # report and continue
                detail, status = {"error": str(e)[-1500:]}, "FAIL"
                failed += 1
            dt = round(time.perf_counter() - t0, 1)
            report["backends"][backend][fn.__name__] = {"scenario": fn.scenario, "status": status, "seconds": dt, **detail}
            print(f"[{backend}] {status} {fn.scenario} ({dt}s)", flush=True)
    Path(args.out).write_text(json.dumps(report, indent=2))
    print(f"wrote {args.out}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

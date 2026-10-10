# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""CPU data-prep run: build a small byte-level corpus and publish it as an
immutable, versioned dataset on the durable tier.

In TauGrid this is a `engine: job` CPU run (or `tau data` ingest). The corpus is
the Python standard library source tree, which every image already contains,
so the prototype needs no network access.
"""

from __future__ import annotations

import argparse
import json
import sys
import sysconfig
import tempfile
from pathlib import Path

import numpy as np

from tau_storage import publish_dataset, store_from_env


def build_corpus(max_bytes: int) -> bytes:
    root = Path(sysconfig.get_paths()["stdlib"])
    chunks, total = [], 0
    for p in sorted(root.glob("*.py")):
        try:
            b = p.read_bytes()
        except OSError:
            continue
        chunks.append(b)
        total += len(b)
        if total >= max_bytes:
            break
    return b"".join(chunks)[:max_bytes]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", default="pystdlib-bytes")
    ap.add_argument("--version", default="v1")
    ap.add_argument("--max-bytes", type=int, default=4_000_000)
    ap.add_argument("--shards", type=int, default=4)
    args = ap.parse_args()

    data = np.frombuffer(build_corpus(args.max_bytes), dtype=np.uint8)
    split = int(len(data) * 0.95)
    store = store_from_env()
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        files = {}
        for i, shard in enumerate(np.array_split(data[:split], args.shards)):
            p = tmp / f"train-{i:03d}.bin"
            shard.tofile(p)
            files[p.name] = p
        val = tmp / "val-000.bin"
        data[split:].tofile(val)
        files[val.name] = val
        try:
            manifest = publish_dataset(store, args.name, args.version, files)
        except FileExistsError as e:
            print(f"[data] {e}")
            return 0
    print(json.dumps({"published": f"{args.name}@{args.version}", "files": len(manifest["files"]),
                      "bytes": int(len(data))}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Small decoder-only transformer trained against the managed-storage contract.

What it exercises:
  * reads an immutable dataset version from the durable tier, staged and
    sha256-verified into the hot tier ($TAU_HOT_DIR);
  * checkpoints to the hot tier and commits to the durable tier in the
    background (only the local write blocks the training loop);
  * on start, resumes from the newest *committed* checkpoint, so a crash, a
    torn upload or a lost node never resumes from partial state;
  * runs single-process or under torchrun (gloo/DDP). Only rank 0 writes
    checkpoints; every rank restores the same step.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from pathlib import Path

import numpy as np
import torch
import torch.distributed as dist
import torch.nn as nn
import torch.nn.functional as F

from tau_storage import CheckpointManager, stage_dataset, store_from_env


class Block(nn.Module):
    def __init__(self, d: int, heads: int):
        super().__init__()
        self.ln1, self.ln2 = nn.LayerNorm(d), nn.LayerNorm(d)
        self.attn = nn.MultiheadAttention(d, heads, batch_first=True)
        self.mlp = nn.Sequential(nn.Linear(d, 4 * d), nn.GELU(), nn.Linear(4 * d, d))

    def forward(self, x, mask):
        h = self.ln1(x)
        x = x + self.attn(h, h, h, attn_mask=mask, need_weights=False)[0]
        return x + self.mlp(self.ln2(x))


class TinyGPT(nn.Module):
    def __init__(self, vocab=256, ctx=128, d=128, layers=4, heads=4):
        super().__init__()
        self.ctx = ctx
        self.tok = nn.Embedding(vocab, d)
        self.pos = nn.Embedding(ctx, d)
        self.blocks = nn.ModuleList(Block(d, heads) for _ in range(layers))
        self.ln = nn.LayerNorm(d)
        self.head = nn.Linear(d, vocab, bias=False)
        self.head.weight = self.tok.weight
        self.register_buffer("mask", torch.triu(torch.ones(ctx, ctx, dtype=torch.bool), 1), persistent=False)

    def forward(self, idx):
        t = idx.shape[1]
        x = self.tok(idx) + self.pos(torch.arange(t, device=idx.device))
        for b in self.blocks:
            x = b(x, self.mask[:t, :t])
        return self.head(self.ln(x))


def load_split(ds_dir: Path, prefix: str) -> np.ndarray:
    parts = [np.fromfile(p, dtype=np.uint8) for p in sorted(ds_dir.glob(f"{prefix}-*.bin"))]
    return np.concatenate(parts)


def batch(data: np.ndarray, bs: int, ctx: int, gen: np.random.Generator):
    ix = gen.integers(0, len(data) - ctx - 1, bs)
    x = np.stack([data[i : i + ctx] for i in ix]).astype(np.int64)
    y = np.stack([data[i + 1 : i + 1 + ctx] for i in ix]).astype(np.int64)
    return torch.from_numpy(x), torch.from_numpy(y)


@torch.no_grad()
def evaluate(model, val, ctx, n=8):
    model.eval()
    gen = np.random.default_rng(1234)
    losses = []
    for _ in range(n):
        x, y = batch(val, 16, ctx, gen)
        losses.append(F.cross_entropy(model(x).flatten(0, 1), y.flatten()).item())
    model.train()
    return float(np.mean(losses))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default=os.environ.get("TAU_RUN_NAME", "tinygpt"))
    ap.add_argument("--dataset", default="pystdlib-bytes@v1")
    ap.add_argument("--steps", type=int, default=300)
    ap.add_argument("--ckpt-every", type=int, default=50)
    ap.add_argument("--keep", type=int, default=3)
    ap.add_argument("--batch-size", type=int, default=16)
    ap.add_argument("--lr", type=float, default=3e-3)
    ap.add_argument("--sync-commit", action="store_true", help="block on durable upload (baseline)")
    args = ap.parse_args()

    distributed = int(os.environ.get("WORLD_SIZE", "1")) > 1
    if distributed:
        dist.init_process_group("gloo")
    rank = dist.get_rank() if distributed else 0
    world = dist.get_world_size() if distributed else 1
    torch.set_num_threads(max(1, (os.cpu_count() or 2) // world))
    torch.manual_seed(0)

    hot = Path(os.environ.get("TAU_HOT_DIR", "/mnt")) / f"rank{rank}"
    store = store_from_env()
    name, _, version = args.dataset.partition("@")
    t0 = time.perf_counter()
    ds_dir, ds_stats = stage_dataset(store, name, version, hot)
    stage_s = time.perf_counter() - t0
    train, val = load_split(ds_dir, "train"), load_split(ds_dir, "val")

    model = TinyGPT()
    opt = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=0.1)
    ckpt = CheckpointManager(store, hot, args.run, keep=args.keep, async_commit=not args.sync_commit)

    start = 0
    restored = None
    # Rank 0 picks the step; everyone loads the same one (broadcast).
    if rank == 0:
        restored = ckpt.restore_latest()
    step_t = torch.tensor([restored[0] if restored else -1])
    if distributed:
        dist.broadcast(step_t, 0)
    if step_t.item() >= 0:
        if rank != 0:
            restored = ckpt.restore_latest()
            assert restored and restored[0] == step_t.item(), "ranks disagree on resume step"
        state = torch.load(restored[1] / "state.pt", weights_only=False)
        model.load_state_dict(state["model"])
        opt.load_state_dict(state["optim"])
        start = state["step"]
    if distributed:
        model = nn.parallel.DistributedDataParallel(model)
    raw_model = model.module if distributed else model

    gen = np.random.default_rng(10_000 * rank + start)
    sched = lambda s: args.lr * min(1.0, (s + 1) / 30) * 0.5 * (1 + math.cos(math.pi * s / args.steps))
    log = []
    loop_t0 = time.perf_counter()
    for step in range(start, args.steps):
        for g in opt.param_groups:
            g["lr"] = sched(step)
        x, y = batch(train, args.batch_size, raw_model.ctx, gen)
        loss = F.cross_entropy(model(x).flatten(0, 1), y.flatten())
        opt.zero_grad(set_to_none=True)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        opt.step()
        done = step + 1
        if done % args.ckpt_every == 0 or done == args.steps:
            val_loss = evaluate(raw_model, val, raw_model.ctx) if rank == 0 else float("nan")
            if rank == 0:
                def write(d: Path):
                    torch.save({"model": raw_model.state_dict(), "optim": opt.state_dict(), "step": done}, d / "state.pt")
                    (d / "meta.json").write_text(json.dumps({"step": done, "val_loss": val_loss, "world": world}))
                st = ckpt.save(done, write)
                log.append({"step": done, "train_loss": loss.item(), "val_loss": val_loss,
                            "blocking_s": round(st.blocking_s, 4), "ckpt_bytes": st.bytes})
                print(f"[train] step {done} loss {loss.item():.3f} val {val_loss:.3f} ckpt_block {st.blocking_s*1000:.1f}ms", flush=True)
            if distributed:
                dist.barrier()
    loop_s = time.perf_counter() - loop_t0
    if rank == 0:
        ckpt.wait()
        result = {
            "run": args.run, "world": world, "resumed_from": start, "final_step": args.steps,
            "dataset_stage_s": round(stage_s, 3), "dataset_stage": ds_stats,
            "loop_s": round(loop_s, 2), "log": log,
            "commits": [{"step": s.step, "hot_write_s": round(s.hot_write_s, 4),
                         "durable_commit_s": round(s.durable_commit_s, 4),
                         "blocking_s": round(s.blocking_s, 4), "committed": s.committed} for s in ckpt.stats],
            "committed_steps": ckpt.committed_steps(),
        }
        store.put_bytes(f"runs/{args.run}/result-attempt{os.environ.get('TAU_RUN_ATTEMPT', '0')}.json",
                        json.dumps(result, indent=2).encode())
        print("[result] " + json.dumps({k: result[k] for k in ("run", "resumed_from", "final_step", "committed_steps")}), flush=True)
    if distributed:
        dist.destroy_process_group()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

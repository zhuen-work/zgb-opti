"""Parallel parameter sweep engine for LSFVG simulator.

Mirrors sweep_fbo.py structure: each worker loads ticks + signal-TF bars once,
then iterates configs via multiprocessing.Pool.
"""
from __future__ import annotations

import hashlib
import os
import sys
import time
from dataclasses import asdict
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .scalper_v1 import SymbolMeta
from .lsfvg import LSFVGConfig
from .lsfvg_fast import simulate_fast as simulate


_W_TICKS: Optional[pd.DataFrame] = None
_W_M1: Optional[pd.DataFrame] = None
_W_SIG: Optional[pd.DataFrame] = None
_W_META: Optional[SymbolMeta] = None


def _init_worker(symbol: str, start_iso: str, end_iso: str, meta_dict: dict, signal_tf: str):
    global _W_TICKS, _W_M1, _W_SIG, _W_META
    sys.stdout = open(os.devnull, "w")

    from .tick_loader import load_ticks, load_bars

    start = datetime.fromisoformat(start_iso)
    end = datetime.fromisoformat(end_iso)
    _W_TICKS = load_ticks(symbol, start, end)
    _W_M1 = load_bars(symbol, "M1", start, end)
    _W_SIG = load_bars(symbol, signal_tf, start, end)
    _W_META = SymbolMeta(**meta_dict)


def _run_job(payload: tuple) -> dict:
    job_id, cfg_dict, initial_balance = payload
    cfg = LSFVGConfig(**cfg_dict)
    out = dict(cfg_dict)
    out["job_id"] = job_id
    t0 = time.time()
    try:
        r = simulate(_W_TICKS, _W_SIG, _W_M1, cfg, _W_META, initial_balance)
        out["net_profit"] = float(r.net_profit)
        out["profit_factor"] = float(r.profit_factor)
        out["drawdown_abs"] = float(r.max_drawdown)
        out["drawdown_pct"] = float(r.max_drawdown_pct)
        out["trades"] = int(r.trades)
        out["tp"] = int(r.tp_count)
        out["sl"] = int(r.sl_count)
        out["other"] = int(r.other_count)
        out["final_balance"] = float(r.final_balance)
        out["return_pct"] = (float(r.net_profit) / float(r.initial_balance) * 100.0
                             if r.initial_balance > 0 else 0.0)
        dd_eps = max(r.max_drawdown, 0.01)
        out["recovery_factor"] = float(r.net_profit) / dd_eps
        out["error"] = None
    except Exception as e:
        for k in ("net_profit", "profit_factor", "drawdown_abs", "drawdown_pct",
                  "final_balance", "return_pct", "recovery_factor"):
            out[k] = np.nan
        for k in ("trades", "tp", "sl", "other"):
            out[k] = 0
        out["error"] = f"{type(e).__name__}: {e}"
    out["runtime_s"] = round(time.time() - t0, 2)
    return out


def _configs_hash(configs: list[LSFVGConfig]) -> str:
    blob = "|".join(repr(sorted(asdict(c).items())) for c in configs)
    return hashlib.sha1(blob.encode()).hexdigest()[:12]


def run_sweep(
    configs: list[LSFVGConfig],
    symbol: str,
    window_start: datetime,
    window_end: datetime,
    meta: SymbolMeta,
    initial_balance: float = 10_000.0,
    n_workers: int = 4,
    cache_path: Optional[Path] = None,
    window_label: str = "",
    signal_tf: str = "M15",
) -> pd.DataFrame:
    if cache_path is not None and cache_path.exists():
        print(f"  [sweep cache hit] {cache_path.name}")
        return pd.read_parquet(cache_path)

    start_iso = window_start.isoformat()
    end_iso = window_end.isoformat()
    meta_dict = asdict(meta)

    payloads = [(i, asdict(cfg), initial_balance) for i, cfg in enumerate(configs)]
    n_workers = max(1, min(n_workers, len(configs)))

    print(f"  [sweep {window_label}] {len(configs)} configs on {n_workers} workers "
          f"({window_start.date()} -> {window_end.date()})  signal_tf={signal_tf}")
    t0 = time.time()
    results: list[dict] = []

    if n_workers == 1:
        _init_worker(symbol, start_iso, end_iso, meta_dict, signal_tf)
        try:
            for i, p in enumerate(payloads, 1):
                results.append(_run_job(p))
                if i % max(1, len(payloads) // 20) == 0 or i == len(payloads):
                    _progress(i, len(payloads), t0)
        finally:
            sys.stdout = sys.__stdout__
    else:
        with Pool(n_workers,
                  initializer=_init_worker,
                  initargs=(symbol, start_iso, end_iso, meta_dict, signal_tf)) as pool:
            try:
                for i, r in enumerate(pool.imap_unordered(_run_job, payloads), 1):
                    results.append(r)
                    if i % max(1, len(payloads) // 20) == 0 or i == len(payloads):
                        _progress(i, len(payloads), t0)
            except KeyboardInterrupt:
                pool.terminate()
                raise

    df = pd.DataFrame(results)
    if "job_id" in df.columns:
        df = df.sort_values("job_id").reset_index(drop=True)
    if "profit_factor" in df.columns:
        df["profit_factor"] = df["profit_factor"].replace([np.inf, -np.inf], 1e9)

    n_err = int(df["error"].notna().sum()) if "error" in df.columns else 0
    elapsed = time.time() - t0
    print(f"  [sweep {window_label}] done in {elapsed:.1f}s "
          f"({elapsed/len(configs):.2f}s/cfg avg), errors={n_err}")

    if cache_path is not None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_path, index=False)
        print(f"  [sweep {window_label}] cached -> {cache_path.name}")

    return df


def _progress(done: int, total: int, t0: float):
    elapsed = time.time() - t0
    rate = done / elapsed if elapsed > 0 else 0
    eta = (total - done) / rate if rate > 0 else float("inf")
    print(f"    [{done}/{total}] {rate:.2f} cfg/s  elapsed={elapsed:.0f}s  eta={eta:.0f}s",
          file=sys.__stderr__ if sys.stdout != sys.__stdout__ else sys.stdout)

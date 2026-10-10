#!/usr/bin/env python3
"""Analyze stress test CSV -> table target vs actual per fase.

Usage:
    python analyze_stress.py metrics/stress_flink_20260818_*.csv
"""
import csv
import math
import statistics
import sys


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_stress.py <stress_csv>")
        sys.exit(1)
    path = sys.argv[1]
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            try:
                rows.append({
                    "t": float(r["elapsed_seconds"]),
                    "target": int(float(r["load_target"])),
                    "tp": float(r["throughput"]),
                    "prod": float(r["produced_rate"]),
                    "lag": float(r["kafka_lag"]),
                })
            except (ValueError, KeyError):
                continue

    print(f"File: {path} | {len(rows)} sampel\n")
    print(f"{'Fase':<6}{'Target':<8}{'n':<5}{'TP aktual':<12}{'Produced':<12}{'Latency ms':<12}{'Lag':<10}")
    print("-" * 70)
    phases = {}
    for r in rows:
        phases.setdefault(r["target"], []).append(r)

    total_offered = total_actual = 0.0
    for target in sorted(phases):
        seg = phases[target]
        tps = [r["tp"] for r in seg if math.isfinite(r["tp"]) and r["tp"] > 0]
        prods = [r["prod"] for r in seg if math.isfinite(r["prod"]) and r["prod"] > 0]
        lags = [r["lag"] for r in seg if math.isfinite(r["lag"])]
        tp_mean = statistics.mean(tps) if tps else float("nan")
        prod_mean = statistics.mean(prods) if prods else float("nan")
        lag_mean = statistics.mean(lags) if lags else float("nan")
        dt = 60.0
        total_offered += target * dt
        total_actual += tp_mean * dt
        ratio = (tp_mean / target * 100) if target else 0
        print(f"{target:<6}{target:<8}{len(seg):<5}{tp_mean:<12.1f}{prod_mean:<12.1f}{lag_mean:<10.0f}  ({ratio:.0f}% dari target)")

    print("-" * 70)
    if total_offered > 0:
        print(f"Deficit keseluruhan: {(1 - total_actual / total_offered) * 100:.1f}%")


if __name__ == "__main__":
    main()

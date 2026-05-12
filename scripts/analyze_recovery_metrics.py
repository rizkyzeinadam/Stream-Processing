#!/usr/bin/env python3
"""CLI for reusable recovery analytics on existing experiment outputs."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List

from recovery_metrics import (
    RunInputs,
    compute_metric_statistics,
    pair_runs,
    serialize_results,
)


def _default_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _build_run_inputs(
    system: str,
    csv_paths: List[str],
    log_paths: List[str],
    summary_paths: List[str],
    manifest_paths: List[str],
    fault_times: List[float] | None,
) -> List[RunInputs]:
    runs: List[RunInputs] = []
    for index, csv_path in enumerate(csv_paths):
        runs.append(
            RunInputs(
                system=system,
                metrics_csv=Path(csv_path),
                log_file=Path(log_paths[index]) if index < len(log_paths) and log_paths[index] else None,
                summary_json=Path(summary_paths[index]) if index < len(summary_paths) and summary_paths[index] else None,
                manifest_json=Path(manifest_paths[index]) if index < len(manifest_paths) and manifest_paths[index] else None,
                fault_time=(fault_times[index] if fault_times and index < len(fault_times) else None),
                run_id=f"{system.lower()}-{index + 1}",
            )
        )
    return runs


def _write_csv(path: Path, rows: List[Dict[str, float]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _format_metric_table(title: str, stats: Dict[str, Dict[str, float]]) -> List[str]:
    lines = [f"## {title}", "", "| Metric | n | Mean | Std | 95% CI |", "|---|---:|---:|---:|---:|"]
    for metric, payload in stats.items():
        lines.append(
            f"| {metric} | {int(payload['n'])} | {payload['mean']:.6f} | {payload['std']:.6f} | [{payload['ci_low']:.6f}, {payload['ci_high']:.6f}] |"
        )
    lines.append("")
    return lines


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute reusable recovery metrics from existing experiment outputs")
    root = _default_project_root()
    parser.add_argument("--flink-csv", nargs="+", default=[str(root / "metrics" / "flink_metrics.csv")])
    parser.add_argument("--spark-csv", nargs="+", default=[str(root / "metrics" / "spark_metrics.csv")])
    parser.add_argument("--flink-log", nargs="+", default=[str(root / "results" / "parsed_logs" / "flink_events.log")])
    parser.add_argument("--spark-log", nargs="+", default=[str(root / "results" / "parsed_logs" / "spark_events.log")])
    parser.add_argument("--flink-summary", nargs="+", default=[str(root / "results" / "experiment_summary.json")])
    parser.add_argument("--spark-summary", nargs="+", default=[str(root / "results" / "experiment_summary.json")])
    parser.add_argument("--flink-manifest", nargs="+", default=[str(root / "results" / "publication_figure_manifest.json")])
    parser.add_argument("--spark-manifest", nargs="+", default=[str(root / "results" / "publication_figure_manifest.json")])
    parser.add_argument("--fault-time", nargs="+", type=float, default=[])
    parser.add_argument("--output-dir", default=str(root / "results" / "recovery_analytics"))
    parser.add_argument("--baseline-window", type=float, default=60.0)
    parser.add_argument("--throughput-ratio", type=float, default=0.90)
    parser.add_argument("--latency-ratio", type=float, default=1.25)
    parser.add_argument("--lag-ratio", type=float, default=1.25)
    parser.add_argument("--stable-dwell-samples", type=int, default=5)
    parser.add_argument("--confidence", type=float, default=0.95)
    args = parser.parse_args()

    if len(args.flink_csv) != len(args.spark_csv):
        raise SystemExit("--flink-csv and --spark-csv must contain the same number of runs.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    flink_runs = _build_run_inputs("Flink", args.flink_csv, args.flink_log, args.flink_summary, args.flink_manifest, args.fault_time)
    spark_runs = _build_run_inputs("Spark", args.spark_csv, args.spark_log, args.spark_summary, args.spark_manifest, args.fault_time)

    flink_results, spark_results = pair_runs(
        flink_runs,
        spark_runs,
        baseline_window=args.baseline_window,
        throughput_ratio=args.throughput_ratio,
        latency_ratio=args.latency_ratio,
        lag_ratio=args.lag_ratio,
        stable_dwell_samples=args.stable_dwell_samples,
        confidence=args.confidence,
    )

    run_rows = serialize_results(flink_results) + serialize_results(spark_results)
    _write_csv(output_dir / "recovery_run_metrics.csv", run_rows)

    flink_stats = compute_metric_statistics(flink_results, confidence=args.confidence)
    spark_stats = compute_metric_statistics(spark_results, confidence=args.confidence)

    aggregate_rows: List[Dict[str, float]] = []
    for system, stats in (("Flink", flink_stats), ("Spark", spark_stats)):
        for metric, payload in stats.items():
            aggregate_rows.append(
                {
                    "system": system,
                    "metric": metric,
                    "n": payload["n"],
                    "mean": payload["mean"],
                    "std": payload["std"],
                    "ci_low": payload["ci_low"],
                    "ci_high": payload["ci_high"],
                }
            )

    _write_csv(output_dir / "recovery_metric_summary.csv", aggregate_rows)

    markdown_lines = ["# Recovery Analytics Summary", ""]
    markdown_lines.extend(_format_metric_table("Flink", flink_stats))
    markdown_lines.extend(_format_metric_table("Spark", spark_stats))
    markdown_lines.append("## Repeated-Run Inputs")
    markdown_lines.append("")
    markdown_lines.append(f"- Flink runs: {len(flink_runs)}")
    markdown_lines.append(f"- Spark runs: {len(spark_runs)}")
    markdown_lines.append(f"- Baseline window: {args.baseline_window:.1f}s")
    markdown_lines.append(f"- Confidence interval: {args.confidence:.2f}")
    markdown_lines.append("")
    (output_dir / "recovery_metric_summary.md").write_text("\n".join(markdown_lines), encoding="utf-8")

    payload = {
        "run_metrics": run_rows,
        "flink_statistics": flink_stats,
        "spark_statistics": spark_stats,
        "configuration": {
            "baseline_window": args.baseline_window,
            "throughput_ratio": args.throughput_ratio,
            "latency_ratio": args.latency_ratio,
            "lag_ratio": args.lag_ratio,
            "stable_dwell_samples": args.stable_dwell_samples,
            "confidence": args.confidence,
        },
    }
    (output_dir / "recovery_metric_summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Wrote recovery analytics to {output_dir}")


if __name__ == "__main__":
    main()
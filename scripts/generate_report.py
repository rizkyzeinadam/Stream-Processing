#!/usr/bin/env python3
"""Generate Flink/Spark markdown reports and experiment summary from metrics CSV files."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd


def load_metrics(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing metrics file: {path}")

    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Metrics file is empty: {path}")

    numeric_cols = [
        "elapsed_seconds",
        "load_rate",
        "throughput",
        "latency",
        "cpu",
        "memory",
        "kafka_lag",
        "recovery_time",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def read_fault_start(default: float = 300.0) -> float:
    fault_yaml = Path(__file__).resolve().parent.parent / "config" / "fault.yaml"
    if not fault_yaml.exists():
        return default

    try:
        import yaml

        content = yaml.safe_load(fault_yaml.read_text(encoding="utf-8")) or {}
        faults = content.get("cascading_faults", [])
        if faults and isinstance(faults[0], dict):
            return float(faults[0].get("time", default))
    except Exception:
        pass

    return default


def compute_recovery_interval(df: pd.DataFrame) -> float:
    """Unified recovery definition used across reports.

    T1: throughput drops below 50% of the baseline.
    T2: throughput is at least 90% of the baseline for 5 consecutive samples.
    """
    if "throughput" not in df.columns or df.empty:
        return 0.0

    tp = pd.to_numeric(df["throughput"], errors="coerce").replace(0, pd.NA)
    baseline = float(tp.dropna().head(10).mean()) if tp.dropna().size else 0.0
    if not pd.notna(baseline) or baseline <= 0:
        return 0.0

    t1_candidates = tp.index[tp < 0.5 * baseline].tolist()
    if not t1_candidates:
        return 0.0

    t1_idx = t1_candidates[0]
    for idx in range(t1_idx, max(0, len(df) - 4)):
        window = tp.iloc[idx:idx + 5]
        if window.size < 5:
            break
        if (window >= 0.9 * baseline).all():
            t2_idx = idx + 4
            t1_time = float(df.iloc[t1_idx]["elapsed_seconds"])
            t2_time = float(df.iloc[t2_idx]["elapsed_seconds"])
            duration = t2_time - t1_time
            if duration >= 0:
                return duration
    return 0.0


def compute_throughput_deficit(df: pd.DataFrame) -> float:
    """Throughput deficit = 1 - total_out / total_in."""
    if "dt" not in df.columns:
        df = df.copy()
        df["dt"] = df["elapsed_seconds"].diff().fillna(1.0)

    total_in = float((df["load_rate"] * df["dt"]).sum()) if "load_rate" in df.columns else 0.0
    total_out = float((df["throughput"] * df["dt"]).sum()) if "throughput" in df.columns else 0.0
    if total_in <= 0:
        return 0.0
    return max(0.0, 1.0 - (total_out / total_in))


def compute_metrics(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    df_lat = df[df["latency"] > 0] if "latency" in df.columns else df
    if df_lat.empty:
        df_lat = df

    lat_avg = float(df_lat["latency"].mean()) if "latency" in df_lat else 0.0
    lat_peak = float(df_lat["latency"].max()) if "latency" in df_lat else 0.0
    tp_avg = float(df["throughput"].mean()) if "throughput" in df.columns else 0.0
    tp_peak = float(df["throughput"].max()) if "throughput" in df.columns else 0.0
    lag_max = float(df["kafka_lag"].max()) if "kafka_lag" in df.columns else 0.0
    cpu_avg = float(df["cpu"].mean()) if "cpu" in df.columns else 0.0
    mem_avg = float(df["memory"].mean()) if "memory" in df.columns else 0.0
    recovery_duration = compute_recovery_interval(df)
    throughput_deficit = compute_throughput_deficit(df) * 100

    if "dt" not in df.columns:
        df = df.copy()
        df["dt"] = df["elapsed_seconds"].diff().fillna(1.0)

    total_in = float((df["load_rate"] * df["dt"]).sum()) if {"load_rate", "dt"}.issubset(df.columns) else 0.0
    total_out = float((df["throughput"] * df["dt"]).sum()) if {"throughput", "dt"}.issubset(df.columns) else 0.0
    final_lag = float(df["kafka_lag"].iloc[-1]) if "kafka_lag" in df.columns else 0.0
    committed_output_events = total_out + final_lag

    if total_in > 0 and total_in > committed_output_events:
        data_loss = total_in - committed_output_events
        loss_pct = (data_loss / total_in) * 100
        duplicates = 0.0
        duplicate_pct = 0.0
        duplicate_source = "None"
        correctness = "Data loss detected after backlog stabilization."
    elif total_in > 0 and committed_output_events > total_in:
        data_loss = 0.0
        loss_pct = 0.0
        duplicates = committed_output_events - total_in
        duplicate_pct = (duplicates / total_in) * 100
        duplicate_source = (
            "Observed duplicates may include repetitive traffic patterns and transient recovery replay."
        )
        correctness = "No permanent loss detected; surplus likely reflects transient replay behavior."
    else:
        data_loss = 0.0
        loss_pct = 0.0
        duplicates = 0.0
        duplicate_pct = 0.0
        duplicate_source = "None"
        correctness = "Input/output balance indicates no measurable permanent loss or replay surplus."

    return {
        "Throughput": {"average": tp_avg, "peak": tp_peak},
        "Latency": {"average": lat_avg, "peak": lat_peak},
        "Backpressure": {"max_lag": lag_max},
        "Resource": {"cpu_avg": cpu_avg, "mem_avg": mem_avg},
        "Recovery": {"recovery_duration": recovery_duration},
        "Correctness": {
            "duplicates": int(round(duplicates)),
            "duplicate_percentage": duplicate_pct,
            "duplicate_source": duplicate_source,
            "correctness_assessment": correctness,
        },
        "DataLoss": {
            "loss_percentage": max(loss_pct, throughput_deficit),
            "total_input_events": int(round(total_in)),
            "total_output_events": int(round(total_out)),
            "missing_events": int(round(data_loss)),
        },
    }


def write_engine_report(engine: str, metrics: Dict[str, Dict[str, float]], output: Path) -> None:
    lines = [
        f"# {engine} Experiment Report",
        "",
        "## Core Metrics",
        "",
        f"- Throughput average: {metrics['Throughput']['average']:.2f} msg/s",
        f"- Throughput peak: {metrics['Throughput']['peak']:.2f} msg/s",
        f"- Latency average: {metrics['Latency']['average']:.2f} ms",
        f"- Latency peak: {metrics['Latency']['peak']:.2f} ms",
        f"- Max Kafka lag: {metrics['Backpressure']['max_lag']:.2f}",
        f"- CPU average: {metrics['Resource']['cpu_avg']:.2f}%",
        f"- Memory average: {metrics['Resource']['mem_avg']:.2f}%",
        "",
        "## Recovery and Correctness",
        "",
        f"- Threshold-based stabilization interval: {metrics['Recovery']['recovery_duration']:.2f} s",
        f"- Data loss: {metrics['DataLoss']['loss_percentage']:.2f}%",
        f"- Duplicate percentage: {metrics['Correctness']['duplicate_percentage']:.2f}%",
        f"- Correctness note: {metrics['Correctness']['correctness_assessment']}",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_comparison(analysis: Dict[str, Dict[str, Dict[str, float]]]) -> List[Dict[str, float | str]]:
    rows: List[Dict[str, float | str]] = []
    metrics_to_compare = [
        ("Throughput (Avg)", "Throughput", "average", True),
        ("Throughput (Peak)", "Throughput", "peak", True),
        ("Latency (Avg)", "Latency", "average", False),
        ("Latency (Peak)", "Latency", "peak", False),
        ("Kafka Lag (Max)", "Backpressure", "max_lag", False),
        ("CPU Usage (Avg)", "Resource", "cpu_avg", False),
        ("Memory Usage (Avg)", "Resource", "mem_avg", False),
        ("Stabilization Interval (s)", "Recovery", "recovery_duration", False),
        ("Data Loss (%)", "DataLoss", "loss_percentage", False),
        ("Duplicate Events (%)", "Correctness", "duplicate_percentage", False),
    ]

    for name, cat, key, higher_is_better in metrics_to_compare:
        f_val = float(analysis["Flink"][cat].get(key, 0.0))
        s_val = float(analysis["Spark"][cat].get(key, 0.0))

        if f_val == s_val:
            better = "Tie"
        elif higher_is_better:
            better = "Flink" if f_val > s_val else "Spark"
        else:
            better = "Flink" if f_val < s_val else "Spark"

        rows.append({
            "Metric": name,
            "Flink": round(f_val, 2),
            "Spark": round(s_val, 2),
            "Better": better,
        })

    return rows


def write_comparison_report(comparison: List[Dict[str, float | str]], output: Path) -> None:
    lines = [
        "# Flink vs Spark Comparison",
        "",
        "| Metric | Flink | Spark | Better |",
        "|---|---:|---:|---|",
    ]
    for row in comparison:
        lines.append(f"| {row['Metric']} | {row['Flink']} | {row['Spark']} | {row['Better']} |")
    lines.append("")
    lines.append("Comparisons are descriptive and depend on configuration, telemetry coverage, and threshold definitions.")

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate comparison reports from experiment metrics")
    parser.add_argument("--flink-metrics", required=True)
    parser.add_argument("--spark-metrics", required=True)
    parser.add_argument("--flink-report", required=True)
    parser.add_argument("--spark-report", required=True)
    parser.add_argument("--comparison-report", required=True)
    args = parser.parse_args()

    flink_df = load_metrics(Path(args.flink_metrics))
    spark_df = load_metrics(Path(args.spark_metrics))

    analysis = {
        "Flink": compute_metrics(flink_df),
        "Spark": compute_metrics(spark_df),
    }
    comparison = build_comparison(analysis)

    write_engine_report("Flink", analysis["Flink"], Path(args.flink_report))
    write_engine_report("Spark", analysis["Spark"], Path(args.spark_report))
    write_comparison_report(comparison, Path(args.comparison_report))

    summary_path = Path(args.comparison_report).parent / "experiment_summary.json"
    summary_payload = {
        "timestamp": datetime.now().isoformat(),
        "analysis": analysis,
        "comparison": comparison,
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    print(f"Saved {args.flink_report}")
    print(f"Saved {args.spark_report}")
    print(f"Saved {args.comparison_report}")
    print(f"Saved {summary_path}")


if __name__ == "__main__":
    main()

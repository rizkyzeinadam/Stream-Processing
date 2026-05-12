#!/usr/bin/env python3
"""Reusable recovery analytics for existing experiment outputs.

This module operates only on already-generated CSV, JSON, and log artifacts.
It does not modify the experiment pipeline or require new data collection.
"""

from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from statistics import NormalDist, mean, median, stdev
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


FAULT_LOG_PATTERNS = {
    "fault_detected": re.compile(r"(?:Throughput collapse|Fault detected).*?t=([0-9.eE+-]+)s", re.IGNORECASE),
    "latency_spike": re.compile(r"Extreme latency spike detected at t=([0-9.eE+-]+)s", re.IGNORECASE),
    "recovery_started": re.compile(r"Recovery started at t=([0-9.eE+-]+)s", re.IGNORECASE),
    "recovery_complete": re.compile(r"Recovery complete(?: at)? t=([0-9.eE+-]+)s", re.IGNORECASE),
}


@dataclass(frozen=True)
class RunInputs:
    """Input artifacts for one engine run."""

    system: str
    metrics_csv: Path
    log_file: Optional[Path] = None
    summary_json: Optional[Path] = None
    manifest_json: Optional[Path] = None
    fault_time: Optional[float] = None
    run_id: Optional[str] = None


@dataclass(frozen=True)
class RecoveryMetricResult:
    """Per-run recovery metrics derived from the existing artifacts."""

    run_id: str
    system: str
    fault_time: float
    recovery_propagation_delay: float
    recovery_stabilization_index: float
    backlog_recovery_rate: float
    replay_amplification_ratio: float
    recovery_duration: float
    baseline_throughput: float
    baseline_latency: float
    peak_lag: float
    peak_latency: float
    final_lag: float
    final_throughput: float


def load_metrics_csv(csv_path: Path) -> List[Dict[str, float]]:
    """Load the standardized metrics CSV as a list of numeric samples."""

    rows: List[Dict[str, float]] = []
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                {
                    "timestamp": row["timestamp"],
                    "elapsed_seconds": float(row["elapsed_seconds"]),
                    "load_rate": float(row["load_rate"]),
                    "throughput": float(row["throughput"]),
                    "latency": float(row["latency"]),
                    "cpu": float(row["cpu"]),
                    "memory": float(row["memory"]),
                    "kafka_lag": float(row["kafka_lag"]),
                    "recovery_time": float(row.get("recovery_time", 0.0) or 0.0),
                }
            )
    rows.sort(key=lambda item: item["elapsed_seconds"])
    return rows


def load_summary_json(summary_path: Optional[Path]) -> Dict:
    """Load the existing experiment summary JSON if available."""

    if not summary_path or not summary_path.exists():
        return {}
    with summary_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_manifest_json(manifest_path: Optional[Path]) -> Dict:
    """Load the publication manifest if it exists."""

    if not manifest_path or not manifest_path.exists():
        return {}
    with manifest_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_log_markers(log_path: Optional[Path]) -> Dict[str, float]:
    """Parse optional recovery log markers from the parsed events logs."""

    if not log_path or not log_path.exists():
        return {}

    markers: Dict[str, float] = {}
    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            for key, pattern in FAULT_LOG_PATTERNS.items():
                match = pattern.search(line)
                if match:
                    try:
                        markers[key] = float(match.group(1))
                    except ValueError:
                        continue
    return markers


def mean_std_ci(values: Sequence[float], confidence: float = 0.95) -> Dict[str, float]:
    """Return mean, sample standard deviation, and a normal-approximation CI."""

    clean_values = [value for value in values if math.isfinite(value)]
    if not clean_values:
        return {
            "n": 0,
            "mean": 0.0,
            "std": 0.0,
            "ci_low": 0.0,
            "ci_high": 0.0,
        }

    count = len(clean_values)
    avg = mean(clean_values)
    deviation = stdev(clean_values) if count > 1 else 0.0
    if count > 1:
        z_score = NormalDist().inv_cdf((1.0 + confidence) / 2.0)
        margin = z_score * deviation / math.sqrt(count)
    else:
        margin = 0.0

    return {
        "n": count,
        "mean": avg,
        "std": deviation,
        "ci_low": avg - margin,
        "ci_high": avg + margin,
    }


def _window_samples(samples: Sequence[Dict[str, float]], start: float, end: float) -> List[Dict[str, float]]:
    return [sample for sample in samples if start <= sample["elapsed_seconds"] < end]


def _stable_samples(
    samples: Sequence[Dict[str, float]],
    baseline_throughput: float,
    baseline_latency: float,
    baseline_lag: float,
    throughput_ratio: float,
    latency_ratio: float,
    lag_ratio: float,
) -> List[Dict[str, float]]:
    throughput_floor = baseline_throughput * throughput_ratio
    latency_ceiling = baseline_latency * latency_ratio
    lag_ceiling = baseline_lag * lag_ratio

    stable: List[Dict[str, float]] = []
    for sample in samples:
        if (
            sample["throughput"] >= throughput_floor
            and sample["latency"] <= latency_ceiling
            and sample["kafka_lag"] <= lag_ceiling
        ):
            stable.append(sample)
    return stable


def _first_sample_after(samples: Sequence[Dict[str, float]], threshold: float) -> Optional[Dict[str, float]]:
    for sample in samples:
        if sample["elapsed_seconds"] >= threshold:
            return sample
    return None


def _first_stable_run_start(samples: Sequence[Dict[str, float]], stable_indices: Iterable[int], dwell: int) -> Optional[float]:
    stable_index_set = set(stable_indices)
    if not stable_index_set:
        return None

    ordered_indices = sorted(stable_index_set)
    run_start = ordered_indices[0]
    run_length = 1
    for previous, current in zip(ordered_indices, ordered_indices[1:]):
        if current == previous + 1:
            run_length += 1
        else:
            if run_length >= dwell:
                return samples[run_start]["elapsed_seconds"]
            run_start = current
            run_length = 1

    if run_length >= dwell:
        return samples[run_start]["elapsed_seconds"]
    return None


def compute_replay_amplification_ratio(summary: Dict, system: str) -> float:
    """Replay amplification ratio = excess output over input, normalized by input."""

    analysis = summary.get("analysis", {}).get(system, {})
    data_loss = analysis.get("DataLoss", {})
    input_events = float(data_loss.get("total_input_events", 0.0) or 0.0)
    output_events = float(data_loss.get("total_output_events", 0.0) or 0.0)

    if input_events <= 0:
        return 0.0
    return max(output_events - input_events, 0.0) / input_events


def derive_fault_time(
    samples: Sequence[Dict[str, float]],
    log_markers: Dict[str, float],
    summary: Dict,
    manifest: Dict,
    explicit_fault_time: Optional[float] = None,
    baseline_window: float = 60.0,
) -> float:
    """Determine the first fault time from explicit input, manifest metadata, summary, or logs."""

    if explicit_fault_time is not None:
        return float(explicit_fault_time)

    for source in (manifest, summary):
        fault_times = source.get("fault_times") or []
        if fault_times:
            return float(fault_times[0])

    for key in ("fault_detected", "recovery_started", "latency_spike"):
        if key in log_markers:
            return float(log_markers[key])

    if not samples:
        return 0.0

    start = max(samples[0]["elapsed_seconds"], samples[-1]["elapsed_seconds"] - baseline_window)
    return float(start)


def _baseline_from_window(samples: Sequence[Dict[str, float]], fault_time: float, baseline_window: float) -> Dict[str, float]:
    baseline = _window_samples(samples, max(0.0, fault_time - baseline_window), fault_time)
    if not baseline:
        baseline = list(samples[: max(1, min(10, len(samples)))])

    return {
        "throughput": median(sample["throughput"] for sample in baseline),
        "latency": median(sample["latency"] for sample in baseline),
        "lag": median(sample["kafka_lag"] for sample in baseline),
    }


def compute_recovery_metrics(
    inputs: RunInputs,
    *,
    baseline_window: float = 60.0,
    throughput_ratio: float = 0.90,
    latency_ratio: float = 1.25,
    lag_ratio: float = 1.25,
    stable_dwell_samples: int = 5,
    confidence: float = 0.95,
) -> RecoveryMetricResult:
    """Compute reusable recovery metrics for a single run."""

    samples = load_metrics_csv(inputs.metrics_csv)
    summary = load_summary_json(inputs.summary_json)
    manifest = load_manifest_json(inputs.manifest_json)
    log_markers = load_log_markers(inputs.log_file)
    fault_time = derive_fault_time(samples, log_markers, summary, manifest, inputs.fault_time, baseline_window)
    baseline = _baseline_from_window(samples, fault_time, baseline_window)
    post_fault = _window_samples(samples, fault_time, samples[-1]["elapsed_seconds"] + 1.0)

    stable_samples = _stable_samples(
        post_fault,
        baseline["throughput"],
        baseline["latency"],
        baseline["lag"],
        throughput_ratio,
        latency_ratio,
        lag_ratio,
    )
    stable_fraction = (len(stable_samples) / len(post_fault)) if post_fault else 0.0

    stable_indices = [index for index, sample in enumerate(post_fault) if sample in stable_samples]
    stable_start = _first_stable_run_start(post_fault, stable_indices, stable_dwell_samples)
    recovery_start = log_markers.get("recovery_started", stable_start)
    recovery_complete = log_markers.get("recovery_complete")

    if recovery_complete is None and stable_start is not None:
        recovery_complete = stable_start

    if recovery_start is None:
        recovery_start = stable_start if stable_start is not None else fault_time

    if recovery_complete is None:
        recovery_complete = samples[-1]["elapsed_seconds"]

    peak_lag_samples = _window_samples(samples, fault_time, recovery_complete + 1.0)
    peak_lag = max((sample["kafka_lag"] for sample in peak_lag_samples), default=0.0)
    final_sample = samples[-1]
    final_lag = final_sample["kafka_lag"]
    final_throughput = final_sample["throughput"]

    backlog_duration = max(recovery_complete - fault_time, 0.001)
    backlog_recovery_rate = max(peak_lag - final_lag, 0.0) / backlog_duration

    recovery_duration = max(recovery_complete - fault_time, 0.0)
    propagation_delay = max(recovery_start - fault_time, 0.0)

    peak_latency = max(sample["latency"] for sample in samples)
    replay_amplification_ratio = compute_replay_amplification_ratio(summary, inputs.system)

    return RecoveryMetricResult(
        run_id=inputs.run_id or inputs.metrics_csv.stem,
        system=inputs.system,
        fault_time=fault_time,
        recovery_propagation_delay=propagation_delay,
        recovery_stabilization_index=stable_fraction,
        backlog_recovery_rate=backlog_recovery_rate,
        replay_amplification_ratio=replay_amplification_ratio,
        recovery_duration=recovery_duration,
        baseline_throughput=baseline["throughput"],
        baseline_latency=baseline["latency"],
        peak_lag=peak_lag,
        peak_latency=peak_latency,
        final_lag=final_lag,
        final_throughput=final_throughput,
    )


def compute_metric_statistics(results: Sequence[RecoveryMetricResult], confidence: float = 0.95) -> Dict[str, Dict[str, float]]:
    """Aggregate a list of per-run results into mean/std/CI summaries."""

    if not results:
        return {}

    grouped: Dict[str, List[float]] = {}
    for result in results:
        payload = asdict(result)
        for key, value in payload.items():
            if key in {"run_id", "system"}:
                continue
            grouped.setdefault(key, []).append(float(value))

    return {metric: mean_std_ci(values, confidence=confidence) for metric, values in grouped.items()}


def pair_runs(
    flink_inputs: Sequence[RunInputs],
    spark_inputs: Sequence[RunInputs],
    **kwargs,
) -> Tuple[List[RecoveryMetricResult], List[RecoveryMetricResult]]:
    """Compute metrics for paired Flink and Spark runs."""

    flink_results = [compute_recovery_metrics(run, **kwargs) for run in flink_inputs]
    spark_results = [compute_recovery_metrics(run, **kwargs) for run in spark_inputs]
    return flink_results, spark_results


def serialize_results(results: Sequence[RecoveryMetricResult]) -> List[Dict[str, float]]:
    return [asdict(result) for result in results]

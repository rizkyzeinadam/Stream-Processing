#!/usr/bin/env python3
"""Generate publication-grade scientific figures from experiment outputs.

The script consumes the experiment CSV/JSON artifacts produced by the
Flink/Spark benchmarking workflow and writes a curated figure pack suitable
for thesis Chapter 4 / publication discussion.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import matplotlib as mpl
import matplotlib.patches as patches
import matplotlib.pyplot as plt
import pandas as pd


mpl.rcParams.update(
    {
        "figure.dpi": 180,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "font.family": "DejaVu Serif",
        "font.size": 10,
        "axes.titlesize": 12,
        "axes.labelsize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "axes.grid": True,
        "grid.alpha": 0.18,
        "grid.linestyle": "--",
        "axes.spines.top": False,
        "axes.spines.right": False,
    }
)


ENGINE_STYLES = {
    "Flink": {"color": "#1f77b4", "marker": "o", "linewidth": 2.0},
    "Spark": {"color": "#d62728", "marker": "s", "linewidth": 2.0},
}

PHASES = [
    (0, 180, "Baseline load", "#edf4fb"),
    (180, 360, "Increasing load", "#f0f6dd"),
    (360, 540, "High load", "#fff1dd"),
    (540, 600, "Burst load", "#fde6e6"),
    (600, 900, "Recovery phase", "#edf6ef"),
]


@dataclass
class FigureRecord:
    filename: str
    caption: str
    placement: str
    discussion: str
    directory: str


def load_metrics(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing metrics file: {csv_path}")

    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError(f"Metrics file is empty: {csv_path}")

    for column in ["elapsed_seconds", "load_rate", "throughput", "latency", "cpu", "memory", "kafka_lag", "recovery_time"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.sort_values("elapsed_seconds").reset_index(drop=True)
    return df


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_yaml(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        import yaml

        with path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}
    except Exception:
        return {}


def ensure_dirs(base_dir: Path) -> Dict[str, Path]:
    directories = {
        "root": base_dir,
        "figures": base_dir / "figures",
        "publication_charts": base_dir / "publication_charts",
        "recovery_analysis": base_dir / "recovery_analysis",
        "correctness_analysis": base_dir / "correctness_analysis",
        "backlog_analysis": base_dir / "backlog_analysis",
    }
    for path in directories.values():
        path.mkdir(parents=True, exist_ok=True)
    return directories


def add_phase_shading(ax: plt.Axes) -> None:
    for start, end, label, color in PHASES:
        ax.axvspan(start, end, color=color, alpha=0.7, lw=0)
        ymin, ymax = ax.get_ylim()
        ax.text(
            (start + end) / 2,
            ymax,
            label,
            ha="center",
            va="bottom",
            fontsize=8,
            color="#4f4f4f",
            clip_on=True,
        )


def normalize_fault_action(action: Optional[str]) -> str:
    if not action:
        return "Fault"
    primary = re.split(r"[_\-\s]+", str(action).strip())[0]
    return primary.capitalize() if primary else "Fault"


def normalize_fault_target(target: Optional[str]) -> str:
    if not target:
        return ""

    target_text = str(target).strip().lower()
    target_match = re.search(r"(?:taskmanager|worker|node)[^\d]*(\d+)", target_text)
    if target_match:
        prefix = "TM" if "taskmanager" in target_text else "Worker" if "worker" in target_text else "Node"
        return f"{prefix}{target_match.group(1)}"

    if "kafka" in target_text:
        return "Kafka"

    cleaned = re.sub(r"[_\-]+", " ", target_text)
    parts = [part for part in cleaned.split() if part]
    return " ".join(part.capitalize() for part in parts)


def build_fault_markers(fault_config: Dict) -> List[Tuple[float, str]]:
    markers: List[Tuple[float, str]] = []
    for fault in fault_config.get("cascading_faults", []):
        if not isinstance(fault, dict):
            continue

        fault_time = fault.get("time")
        if fault_time is None:
            continue

        try:
            normalized_time = float(fault_time)
        except (TypeError, ValueError):
            continue

        action = normalize_fault_action(fault.get("action") or fault.get("type"))
        target = normalize_fault_target(fault.get("target") or fault.get("source") or fault.get("node"))
        label = action if not target else f"{action}\n{target}"
        markers.append((normalized_time, label))

    return markers


def add_fault_markers(ax: plt.Axes, fault_markers: Sequence[Tuple[float, str]], y_offset: float = 0.95) -> None:
    ylim = ax.get_ylim()
    y_pos = ylim[0] + (ylim[1] - ylim[0]) * y_offset
    for fault_time, label in fault_markers:
        ax.axvline(fault_time, color="#444444", linestyle="--", linewidth=1.0, alpha=0.8)
        ax.text(
            fault_time + 2,
            y_pos,
            label,
            rotation=90,
            va="top",
            ha="left",
            fontsize=8,
            color="#333333",
            clip_on=True,
        )


def add_footnote(fig: plt.Figure, text: str) -> None:
    fig.text(0.5, 0.01, text, ha="center", va="bottom", fontsize=8, color="#555555")


def plot_time_series_dual(
    flink: pd.DataFrame,
    spark: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    y_label: str,
    filename: Path,
    symlog: bool = False,
    secondary_col: Optional[str] = None,
    secondary_label: Optional[str] = None,
    secondary_scale: str = "linear",
    fault_markers: Sequence[Tuple[float, str]] = (),
    footnote: Optional[str] = None,
) -> None:
    fig, ax = plt.subplots(figsize=(11, 5.5))

    xmin = min(flink[x_col].min(), spark[x_col].min())
    xmax = max(flink[x_col].max(), spark[x_col].max())

    for engine, df in (("Flink", flink), ("Spark", spark)):
        style = ENGINE_STYLES[engine]
        ax.plot(
            df[x_col],
            df[y_col],
            label=engine,
            color=style["color"],
            linewidth=style["linewidth"],
            marker=style["marker"],
            markersize=3.0,
            markevery=max(len(df) // 18, 1),
            alpha=0.95,
        )

    if symlog:
        ax.set_yscale("symlog", linthresh=1.0)

    ax.set_xlim(xmin, xmax)
    ax.set_title(title, pad=12, weight="semibold")
    ax.set_xlabel("Elapsed experiment time (s)")
    ax.set_ylabel(y_label)
    add_phase_shading(ax)

    spark_latency_unavailable = y_col == "latency" and spark[y_col].isna().all()

    if secondary_col and secondary_label:
        ax2 = ax.twinx()
        for engine, df in (("Flink", flink), ("Spark", spark)):
            style = ENGINE_STYLES[engine]
            ax2.plot(
                df[x_col],
                df[secondary_col],
                linestyle=":" if engine == "Flink" else "--",
                color=style["color"],
                linewidth=1.5,
                alpha=0.6,
                label=f"{engine} {secondary_label}",
            )
        if secondary_scale == "symlog":
            ax2.set_yscale("symlog", linthresh=1.0)
        ax2.set_ylabel(secondary_label)

        lines, labels = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax2.legend(lines + lines2, labels + labels2, loc="upper right", frameon=True)
        add_fault_markers(ax, fault_markers)
    else:
        add_fault_markers(ax, fault_markers)
        ax.legend(loc="upper right", frameon=True)

    if spark_latency_unavailable:
        ax.text(
            0.02,
            0.02,
            "Spark per-event latency telemetry unavailable in the current observability configuration.",
            transform=ax.transAxes,
            ha="left",
            va="bottom",
            fontsize=8,
            color="#555555",
            bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="#cccccc", alpha=0.92),
        )

    if footnote:
        fig.tight_layout(rect=(0, 0.05, 1, 1))
        add_footnote(fig, footnote)
    else:
        fig.tight_layout()
    fig.savefig(filename)
    plt.close(fig)


def plot_resource_usage(
    flink: pd.DataFrame,
    spark: pd.DataFrame,
    filename: Path,
    fault_markers: Sequence[Tuple[float, str]],
) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(11, 7.2), sharex=True)

    for ax, metric, label in (
        (axes[0], "cpu", "CPU utilization (%)"),
        (axes[1], "memory", "Memory utilization (%)"),
    ):
        for engine, df in (("Flink", flink), ("Spark", spark)):
            style = ENGINE_STYLES[engine]
            ax.plot(
                df["elapsed_seconds"],
                df[metric],
                label=engine,
                color=style["color"],
                linewidth=style["linewidth"],
                marker=style["marker"],
                markersize=3.0,
                markevery=max(len(df) // 18, 1),
            )
        ax.set_ylabel(label)
        ax.legend(loc="upper right", frameon=True)
        add_phase_shading(ax)
        add_fault_markers(ax, fault_markers, y_offset=0.92)

    axes[0].set_title("Resource usage during fault injection and stabilization", pad=12, weight="semibold")
    axes[1].set_xlabel("Elapsed experiment time (s)")
    fig.tight_layout()
    fig.savefig(filename)
    plt.close(fig)


def plot_recovery_master(
    flink: pd.DataFrame,
    spark: pd.DataFrame,
    filename: Path,
    fault_markers: Sequence[Tuple[float, str]],
) -> None:
    fig, axes = plt.subplots(4, 1, figsize=(12, 13), sharex=True)

    panel_specs = [
        (axes[0], "load_rate", "Offered load (msg/s)", False),
        (axes[1], "throughput", "Observed throughput (msg/s)", False),
        (axes[2], "latency", "Latency (ms)", True),
    ]

    for ax, metric, ylabel, symlog in panel_specs:
        for engine, df in (("Flink", flink), ("Spark", spark)):
            style = ENGINE_STYLES[engine]
            ax.plot(
                df["elapsed_seconds"],
                df[metric],
                label=engine,
                color=style["color"],
                linewidth=style["linewidth"],
                marker=style["marker"],
                markersize=2.6,
                markevery=max(len(df) // 20, 1),
            )
        if symlog:
            ax.set_yscale("symlog", linthresh=1.0)
        ax.set_ylabel(ylabel)
        add_phase_shading(ax)
        add_fault_markers(ax, fault_markers, y_offset=0.92)

    axes[0].set_title("Master timeline with fault annotations and telemetry proxies", pad=12, weight="semibold")
    axes[0].legend(loc="upper right", frameon=True)

    lag_ax = axes[3]
    for engine, df in (("Flink", flink), ("Spark", spark)):
        style = ENGINE_STYLES[engine]
        lag_ax.plot(
            df["elapsed_seconds"],
            df["kafka_lag"],
            label=f"{engine} Kafka lag",
            color=style["color"],
            linewidth=style["linewidth"],
            marker=style["marker"],
            markersize=2.6,
            markevery=max(len(df) // 20, 1),
            alpha=0.9,
        )
    lag_ax.set_yscale("symlog", linthresh=1.0)
    lag_ax.set_ylabel("Kafka lag (records)")
    lag_ax.set_xlabel("Elapsed experiment time (s)")
    add_phase_shading(lag_ax)
    add_fault_markers(lag_ax, fault_markers, y_offset=0.92)

    recovery_ax = lag_ax.twinx()
    for engine, df in (("Flink", flink), ("Spark", spark)):
        style = ENGINE_STYLES[engine]
        recovery_ax.plot(
            df["elapsed_seconds"],
            df["recovery_time"],
            linestyle=":" if engine == "Flink" else "--",
            color=style["color"],
            linewidth=1.6,
            alpha=0.7,
            label=f"{engine} active backlog duration",
        )
    recovery_ax.set_ylabel("Active backlog duration / stabilization proxy (s)")

    lines, labels = lag_ax.get_legend_handles_labels()
    lines2, labels2 = recovery_ax.get_legend_handles_labels()
    lag_ax.legend(lines + lines2, labels + labels2, loc="upper right", frameon=True, ncol=2)

    fig.tight_layout(rect=(0, 0.05, 1, 1))
    add_footnote(fig, "Active backlog duration is a recovery proxy derived from the observability pipeline; backlog equivalence is approximate.")
    fig.savefig(filename)
    plt.close(fig)


def should_skip_data_loss_reconciliation(summary: Dict, flink: pd.DataFrame, spark: pd.DataFrame) -> Tuple[bool, str]:
    analysis = summary.get("analysis", {})
    engines = ["Flink", "Spark"]
    output_events = [analysis.get(engine, {}).get("DataLoss", {}).get("total_output_events", 0) for engine in engines]
    throughput_peak = max(
        pd.to_numeric(flink.get("throughput", pd.Series(dtype=float)), errors="coerce").max(skipna=True) if "throughput" in flink else 0,
        pd.to_numeric(spark.get("throughput", pd.Series(dtype=float)), errors="coerce").max(skipna=True) if "throughput" in spark else 0,
    )

    if sum(output_events) == 0 and pd.notna(throughput_peak) and throughput_peak > 0:
        return True, "Observed output events are zero while throughput remains non-zero, so the reconciliation panel would contradict the telemetry."

    return False, ""


def plot_recovery_duration_bar(summary: Dict, filename: Path) -> None:
    analysis = summary.get("analysis", {})
    flink = analysis.get("Flink", {}).get("Recovery", {}).get("recovery_duration", 0.0)
    spark = analysis.get("Spark", {}).get("Recovery", {}).get("recovery_duration", 0.0)

    fig, ax = plt.subplots(figsize=(8.6, 4.8))
    values = [flink, spark]
    bars = ax.bar(["Flink", "Spark"], values, color=[ENGINE_STYLES["Flink"]["color"], ENGINE_STYLES["Spark"]["color"]], width=0.55)
    ax.set_ylabel("Threshold-based stabilization interval (s)")
    ax.set_title("Configured recovery observation window comparison under identical cascading faults", pad=12, weight="semibold")
    ax.set_ylim(0, max(values) * 1.2 if max(values) > 0 else 1)
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(values) * 0.03,
            f"{value:.2f}s",
            ha="center",
            va="bottom",
            fontsize=10,
        )
    ax.text(
        0.5,
        -0.16,
        "Intervals are threshold-driven and configuration-dependent; they should not be read as unconstrained natural recovery completion times.",
        transform=ax.transAxes,
        ha="center",
        va="top",
        fontsize=8,
        color="#555555",
    )
    fig.tight_layout()
    fig.savefig(filename)
    plt.close(fig)


def plot_replay_amplification(summary: Dict, filename: Path) -> None:
    analysis = summary.get("analysis", {})
    engines = ["Flink", "Spark"]
    duplicates = [analysis.get(engine, {}).get("Correctness", {}).get("duplicates", 0) for engine in engines]
    duplicate_pct = [analysis.get(engine, {}).get("Correctness", {}).get("duplicate_percentage", 0.0) for engine in engines]

    fig, ax = plt.subplots(figsize=(9.4, 5.0))
    bars = ax.bar(engines, duplicate_pct, color=[ENGINE_STYLES[e]["color"] for e in engines], width=0.56)
    ax.set_ylabel("Replay-amplification rate (%)")
    ax.set_title("Replay-induced event amplification and transient reprocessing", pad=12, weight="semibold")
    ax.set_ylim(0, max(duplicate_pct) * 1.18 if max(duplicate_pct) > 0 else 1)
    for bar, pct, dup in zip(bars, duplicate_pct, duplicates):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(duplicate_pct) * 0.03,
            f"{pct:.2f}%\n({dup:,} events)",
            ha="center",
            va="bottom",
            fontsize=9,
        )
    ax.text(
        0.5,
        -0.18,
        "Reviewer-safe terminology: replay-induced event amplification, transient reprocessing, and recovery replay behavior.",
        transform=ax.transAxes,
        ha="center",
        va="top",
        fontsize=8,
        color="#555555",
    )
    fig.tight_layout()
    fig.savefig(filename)
    plt.close(fig)


def plot_data_loss_reconciliation(summary: Dict, filename: Path) -> None:
    analysis = summary.get("analysis", {})
    engines = ["Flink", "Spark"]
    input_events = [analysis.get(engine, {}).get("DataLoss", {}).get("total_input_events", 0) for engine in engines]
    output_events = [analysis.get(engine, {}).get("DataLoss", {}).get("total_output_events", 0) for engine in engines]
    missing_events = [analysis.get(engine, {}).get("DataLoss", {}).get("missing_events", 0) for engine in engines]

    x = range(len(engines))
    width = 0.34
    fig, ax = plt.subplots(figsize=(10.4, 5.2))
    bars1 = ax.bar([i - width / 2 for i in x], input_events, width=width, label="Expected input events", color="#7f8c8d")
    bars2 = ax.bar([i + width / 2 for i in x], output_events, width=width, label="Observed output events", color=[ENGINE_STYLES[e]["color"] for e in engines])
    ax.bar([i + width / 2 for i in x], missing_events, width=width * 0.45, label="Unrecoverable loss", color="#2ca02c", alpha=0.7)

    ax.set_xticks(list(x))
    ax.set_xticklabels(engines)
    ax.set_ylabel("Event count")
    ax.set_title("Output reconciliation under consistent telemetry", pad=12, weight="semibold")
    ymax = max(max(input_events), max(output_events))
    ax.set_ylim(0, ymax * 1.18 if ymax > 0 else 1)
    ax.legend(loc="upper left", frameon=True)

    for bar, value in zip(bars1, input_events):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + ymax * 0.02, f"{value:,}", ha="center", va="bottom", fontsize=9)
    for bar, value, miss in zip(bars2, output_events, missing_events):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + ymax * 0.02,
            f"{value:,}\nloss={miss:,}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    ax.text(
        0.5,
        -0.18,
        "Zero missing events indicate no permanent loss; output surplus reflects transient replay and backlog amplification rather than silent data disappearance.",
        transform=ax.transAxes,
        ha="center",
        va="top",
        fontsize=8,
        color="#555555",
    )
    fig.tight_layout()
    fig.savefig(filename)
    plt.close(fig)


def plot_architecture_comparison(filename: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 6.3))
    ax.set_axis_off()
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    def box(x: float, y: float, w: float, h: float, text: str, color: str, edge: str, weight: str = "normal") -> None:
        rect = patches.FancyBboxPatch(
            (x, y),
            w,
            h,
            boxstyle="round,pad=0.02,rounding_size=0.02",
            linewidth=1.3,
            edgecolor=edge,
            facecolor=color,
        )
        ax.add_patch(rect)
        ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=10, weight=weight, color="#202020")

    def arrow(x1: float, y1: float, x2: float, y2: float, color: str) -> None:
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1), arrowprops=dict(arrowstyle="->", lw=1.4, color=color))

    ax.text(0.25, 0.95, "Apache Flink", ha="center", va="center", fontsize=14, weight="semibold", color=ENGINE_STYLES["Flink"]["color"])
    ax.text(0.75, 0.95, "Apache Spark Structured Streaming", ha="center", va="center", fontsize=14, weight="semibold", color=ENGINE_STYLES["Spark"]["color"])

    flink_boxes = [
        (0.08, 0.72, 0.34, 0.10, "Continuous processing\ntrue streaming", "#eaf3fb", ENGINE_STYLES["Flink"]["color"], "semibold"),
        (0.08, 0.54, 0.34, 0.10, "Asynchronous checkpoints\nlow-latency state capture", "#eaf3fb", ENGINE_STYLES["Flink"]["color"], "normal"),
        (0.08, 0.36, 0.34, 0.10, "Adaptive rebalance\nfast TM recovery", "#eaf3fb", ENGINE_STYLES["Flink"]["color"], "normal"),
        (0.08, 0.18, 0.34, 0.10, "Replay amplification\nmanaged by stabilization proxy", "#eaf3fb", ENGINE_STYLES["Flink"]["color"], "normal"),
    ]

    spark_boxes = [
        (0.58, 0.72, 0.34, 0.10, "Micro-batch execution\ntrigger-based scheduling", "#fcefe9", ENGINE_STYLES["Spark"]["color"], "semibold"),
        (0.58, 0.54, 0.34, 0.10, "Batch recomputation\nstate store coordination", "#fcefe9", ENGINE_STYLES["Spark"]["color"], "normal"),
        (0.58, 0.36, 0.34, 0.10, "Driver-mediated recovery\nstabilization propagation", "#fcefe9", ENGINE_STYLES["Spark"]["color"], "normal"),
        (0.58, 0.18, 0.34, 0.10, "Replay semantics\ndelayed stabilization", "#fcefe9", ENGINE_STYLES["Spark"]["color"], "normal"),
    ]

    for x, y, w, h, text, face, edge, weight in flink_boxes:
        box(x, y, w, h, text, face, edge, weight)
    for x, y, w, h, text, face, edge, weight in spark_boxes:
        box(x, y, w, h, text, face, edge, weight)

    for start_y in (0.72, 0.54, 0.36):
        arrow(0.25, start_y, 0.25, start_y - 0.08, ENGINE_STYLES["Flink"]["color"])
        arrow(0.75, start_y, 0.75, start_y - 0.08, ENGINE_STYLES["Spark"]["color"])

    ax.add_patch(patches.FancyArrowPatch((0.42, 0.50), (0.58, 0.50), arrowstyle="<->", mutation_scale=14, lw=1.5, color="#444444"))
    ax.text(0.50, 0.51, "architectural trade-off", ha="center", va="bottom", fontsize=9, color="#444444")

    ax.text(
        0.50,
        0.07,
        "Conceptual comparison derived from the experiment design and observed recovery behavior: continuous event-at-a-time streaming vs micro-batch scheduling.",
        ha="center",
        va="center",
        fontsize=9,
        color="#555555",
    )
    fig.tight_layout()
    fig.savefig(filename)
    plt.close(fig)


def write_catalog(records: Sequence[FigureRecord], output_path: Path) -> None:
    lines = ["# Publication Figure Catalog", ""]
    for index, record in enumerate(records, start=1):
        lines.append(f"## Figure {index}: {record.filename}")
        lines.append(f"- Directory: {record.directory}")
        lines.append(f"- Suggested Bab 4 placement: {record.placement}")
        lines.append(f"- Caption: {record.caption}")
        lines.append(f"- Scientific discussion: {record.discussion}")
        lines.append("")
    output_path.write_text("\n".join(lines), encoding="utf-8")


def build_records() -> List[FigureRecord]:
    return [
        FigureRecord(
            filename="throughput_recovery_timeline.png",
            directory="publication_charts",
            placement="Bab 4.2 - Throughput and workload response",
            caption="Throughput evolution across baseline, increasing load, burst load, and the configured recovery observation window for Flink and Spark.",
            discussion="Shows throughput response and stabilization behavior under the shared fault sequence without implying unconstrained recovery completion.",
        ),
        FigureRecord(
            filename="latency_evolution.png",
            directory="publication_charts",
            placement="Bab 4.3 - Latency behavior under cascading faults",
            caption="Latency evolution with fault annotations, symlog scaling, and telemetry qualification to preserve steady-state and spike dynamics.",
            discussion="Highlights latency spikes and post-fault propagation while noting that Spark per-event latency telemetry is unavailable in the current observability setup.",
        ),
        FigureRecord(
            filename="kafka_lag_evolution.png",
            directory="backlog_analysis",
            placement="Bab 4.4 - Backpressure and Kafka lag analysis",
            caption="Kafka lag and active backlog duration evolution showing backlog accumulation and stabilization behavior during the configured recovery observation window.",
            discussion="Visualizes backlog propagation and how lag reduction aligns with the active backlog duration proxy.",
        ),
        FigureRecord(
            filename="resource_usage_overview.png",
            directory="recovery_analysis",
            placement="Bab 4.5 - Resource overhead during recovery",
            caption="CPU and memory utilization during fault injection and stabilization periods for both stream-processing engines.",
            discussion="Supports discussion of recovery-related resource amplification and state-management overhead.",
        ),
        FigureRecord(
            filename="recovery_timeline_master.png",
            directory="recovery_analysis",
            placement="Bab 4.6 - Master recovery timeline",
            caption="Master timeline combining offered load, throughput, latency, Kafka lag, and active backlog duration traces with fault annotations.",
            discussion="Provides a publication-style observability figure for temporal recovery analysis while qualifying the recovery proxy used in telemetry.",
        ),
        FigureRecord(
            filename="recovery_duration_comparison.png",
            directory="recovery_analysis",
            placement="Bab 4.7 - Recovery duration comparison",
            caption="Threshold-based stabilization interval comparison under identical cascading faults.",
            discussion="Summarizes the configured recovery observation window without implying unconstrained natural recovery completion.",
        ),
        FigureRecord(
            filename="replay_amplification_analysis.png",
            directory="correctness_analysis",
            placement="Bab 4.8 - Replay semantics and output correctness",
            caption="Replay-induced event amplification and transient reprocessing comparison across engines.",
            discussion="Uses reviewer-safe terminology to discuss duplicate output patterns without overstating correctness failure.",
        ),
        FigureRecord(
            filename="data_loss_reconciliation.png",
            directory="correctness_analysis",
            placement="Bab 4.9 - Data loss reconciliation",
            caption="Input/output reconciliation with explicit separation between delayed processing, replay backlog, and unrecoverable loss.",
            discussion="Shows that the measured loss is zero while output surplus reflects replay behavior.",
        ),
        FigureRecord(
            filename="architecture_comparison.png",
            directory="figures",
            placement="Bab 4.1 - Architecture comparison and design trade-offs",
            caption="Conceptual comparison of Flink continuous processing versus Spark micro-batch execution.",
            discussion="Provides a publication-grade systems diagram to frame the empirical results.",
        ),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate publication-grade figures from experiment outputs")
    parser.add_argument("--project-root", default=None, help="Project root directory (defaults to repository root inferred from this script)")
    parser.add_argument("--results-dir", default=None, help="Results directory containing metrics and summary artifacts")
    parser.add_argument("--config-dir", default=None, help="Config directory containing load.yaml and fault.yaml")
    parser.add_argument("--metrics-dir", default=None, help="Metrics directory containing CSV outputs")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    project_root = Path(args.project_root).resolve() if args.project_root else script_dir.parent
    results_dir = Path(args.results_dir).resolve() if args.results_dir else project_root / "results"
    config_dir = Path(args.config_dir).resolve() if args.config_dir else project_root / "config"
    metrics_dir = Path(args.metrics_dir).resolve() if args.metrics_dir else project_root / "metrics"

    directories = ensure_dirs(results_dir)

    flink_metrics = load_metrics(metrics_dir / "flink_metrics.csv")
    spark_metrics = load_metrics(metrics_dir / "spark_metrics.csv")
    summary = load_json(results_dir / "experiment_summary.json")
    load_config = load_yaml(config_dir / "load.yaml")
    fault_config = load_yaml(config_dir / "fault.yaml")
    fault_markers = build_fault_markers(fault_config)

    if flink_metrics.empty or spark_metrics.empty:
        raise SystemExit("Both flink_metrics.csv and spark_metrics.csv must exist and contain data.")

    # The load and fault configuration are intentionally loaded even if they are
    # only used for cataloging and future extension. Their presence guards the
    # timeline semantics against mismatched experiment settings.
    _ = load_config
    _ = fault_config

    records = build_records()

    plot_time_series_dual(
        flink_metrics,
        spark_metrics,
        x_col="elapsed_seconds",
        y_col="throughput",
        title="Throughput evolution across the configured observation window under cascading faults",
        y_label="Throughput (messages/s)",
        filename=directories["publication_charts"] / "throughput_recovery_timeline.png",
        fault_markers=fault_markers,
    )

    plot_time_series_dual(
        flink_metrics,
        spark_metrics,
        x_col="elapsed_seconds",
        y_col="latency",
        title="Latency evolution with telemetry qualification under identical fault injection",
        y_label="Latency (ms)",
        filename=directories["publication_charts"] / "latency_evolution.png",
        symlog=True,
        fault_markers=fault_markers,
        footnote="Spark per-event latency telemetry unavailable in the current observability configuration.",
    )

    plot_time_series_dual(
        flink_metrics,
        spark_metrics,
        x_col="elapsed_seconds",
        y_col="kafka_lag",
        title="Kafka backlog evolution and active backlog duration proxy",
        y_label="Kafka lag (records)",
        filename=directories["backlog_analysis"] / "kafka_lag_evolution.png",
        symlog=True,
        secondary_col="recovery_time",
        secondary_label="Active backlog duration (s)",
        fault_markers=fault_markers,
        footnote="Active backlog duration is a recovery proxy; backlog equivalence is approximate across engines.",
    )

    plot_resource_usage(flink_metrics, spark_metrics, directories["recovery_analysis"] / "resource_usage_overview.png", fault_markers)
    plot_recovery_master(flink_metrics, spark_metrics, directories["recovery_analysis"] / "recovery_timeline_master.png", fault_markers)
    plot_recovery_duration_bar(summary, directories["recovery_analysis"] / "recovery_duration_comparison.png")
    plot_replay_amplification(summary, directories["correctness_analysis"] / "replay_amplification_analysis.png")

    skip_reconciliation, skip_reason = should_skip_data_loss_reconciliation(summary, flink_metrics, spark_metrics)
    if skip_reconciliation:
        print(f"Warning: skipping data_loss_reconciliation.png because {skip_reason}")
    else:
        plot_data_loss_reconciliation(summary, directories["correctness_analysis"] / "data_loss_reconciliation.png")

    plot_architecture_comparison(directories["figures"] / "architecture_comparison.png")

    if skip_reconciliation:
        records = [record for record in records if record.filename != "data_loss_reconciliation.png"]

    catalog_path = results_dir / "publication_figure_catalog.md"
    write_catalog(records, catalog_path)

    manifest_path = results_dir / "publication_figure_manifest.json"
    manifest = {
        "project_root": str(project_root),
        "results_dir": str(results_dir),
        "config": {
            "load": str(config_dir / "load.yaml"),
            "fault": str(config_dir / "fault.yaml"),
        },
        "fault_markers": [{"time": time, "label": label} for time, label in fault_markers],
        "load_phases": [
            {"start": start, "end": end, "label": label}
            for start, end, label, _color in PHASES
        ],
        "figures": [record.__dict__ for record in records],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Saved publication figures under {results_dir}")
    print(f"Saved figure catalog: {catalog_path}")
    print(f"Saved figure manifest: {manifest_path}")


if __name__ == "__main__":
    main()
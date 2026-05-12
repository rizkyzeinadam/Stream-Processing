# Publication Figure Catalog

## Figure 1: throughput_recovery_timeline.png
- Directory: publication_charts
- Suggested Bab 4 placement: Bab 4.2 - Throughput and workload response
- Caption: Throughput evolution across baseline, increasing load, burst load, and fault-induced recovery windows for Flink and Spark.
- Scientific discussion: Shows throughput collapse, backlog catch-up, and stabilization under the shared fault sequence.

## Figure 2: latency_evolution.png
- Directory: publication_charts
- Suggested Bab 4 placement: Bab 4.3 - Latency behavior under cascading faults
- Caption: Latency evolution with fault annotations and symlog scaling to preserve both steady-state and spike dynamics.
- Scientific discussion: Highlights latency spikes, post-fault propagation, and the stabilization gap between engines.

## Figure 3: kafka_lag_evolution.png
- Directory: backlog_analysis
- Suggested Bab 4 placement: Bab 4.4 - Backpressure and Kafka lag analysis
- Caption: Kafka lag and recovery-state evolution showing backlog accumulation and catch-up behavior during the recovery phase.
- Scientific discussion: Visualizes backlog propagation and how lag reduction aligns with recovery state decay.

## Figure 4: resource_usage_overview.png
- Directory: recovery_analysis
- Suggested Bab 4 placement: Bab 4.5 - Resource overhead during recovery
- Caption: CPU and memory utilization during fault injection and catch-up periods for both stream-processing engines.
- Scientific discussion: Supports discussion of recovery-related resource amplification and state-management overhead.

## Figure 5: recovery_timeline_master.png
- Directory: recovery_analysis
- Suggested Bab 4 placement: Bab 4.6 - Master recovery timeline
- Caption: Master timeline combining offered load, throughput, latency, Kafka lag, and recovery-state traces with fault annotations.
- Scientific discussion: Provides a publication-style observability figure for temporal recovery analysis.

## Figure 6: recovery_duration_comparison.png
- Directory: recovery_analysis
- Suggested Bab 4 placement: Bab 4.7 - Recovery duration comparison
- Caption: Recovery duration comparison under identical cascading failures.
- Scientific discussion: Summarizes the end-to-end stabilization speed difference between Flink and Spark.

## Figure 7: replay_amplification_analysis.png
- Directory: correctness_analysis
- Suggested Bab 4 placement: Bab 4.8 - Replay semantics and output correctness
- Caption: Replay-induced event amplification and transient reprocessing comparison across engines.
- Scientific discussion: Uses reviewer-safe terminology to discuss duplicate output patterns without overstating correctness failure.

## Figure 8: data_loss_reconciliation.png
- Directory: correctness_analysis
- Suggested Bab 4 placement: Bab 4.9 - Data loss reconciliation
- Caption: Input/output reconciliation with explicit separation between delayed processing, replay backlog, and unrecoverable loss.
- Scientific discussion: Shows that the measured loss is zero while output surplus reflects replay behavior.

## Figure 9: architecture_comparison.png
- Directory: figures
- Suggested Bab 4 placement: Bab 4.1 - Architecture comparison and design trade-offs
- Caption: Conceptual comparison of Flink continuous processing versus Spark micro-batch execution.
- Scientific discussion: Provides a publication-grade systems diagram to frame the empirical results.

---
title: Observability
description: Monitor pipelines with metrics, distributed tracing, and data lineage tracking.
order: 7
---

# Observability

NPipeline provides three complementary observability dimensions, each available as a separate extension package:

- **Metrics** - per-node timing, throughput, and optional memory metrics. For lazy stream nodes with per-node observability enabled, duration is finalized at dataflow completion. Pluggable sinks let you route metrics to logging, Prometheus, or any custom backend.
- **Tracing** - OpenTelemetry integration that creates `Activity` spans for each pipeline run and node execution, compatible with Jaeger, Zipkin, and Azure Monitor.
- **Lineage** - data provenance tracking with per-item correlation IDs, sampling, and overflow policies for production use.

All three are opt-in and add zero overhead when not configured.

## In This Section

- [Metrics and Monitoring](metrics-and-monitoring.md) - collect per-node and per-pipeline metrics with pluggable sinks
- [OpenTelemetry Integration](opentelemetry.md) - distributed tracing with standard exporters
- [Data Lineage](data-lineage.md) - track data provenance with per-item correlation and sampling

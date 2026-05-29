# NPipeline Core Fix Plan: Stream Node Duration Attribution

## Summary

In Studio, stream-heavy pipelines can show very small duration values for upstream stream transform nodes (for example, AI enrichment) while downstream sink nodes show large durations. This is not a Studio rendering issue anymore. The remaining mismatch comes from how core NPipeline timing is recorded for stream nodes.

## Repro Symptom

- Stream transform node (ai-review-analysis) duration is very small (milliseconds)
- Same node shows large Avg Item (seconds)
- Downstream sink node shows long duration (seconds)

This means duration and Avg Item are being sourced from different timing windows.

## Root Cause (Core)

### 1. Node completion duration is finalized too early for stream nodes

File: src/NPipeline/Execution/Orchestration/PipelineNodeExecutionStage.cs

- Node completion is emitted after nodeExecutor.ExecuteAsync returns.
- For stream execution strategies, ExecuteAsync returns a lazy IDataStream immediately.
- Actual stream work happens later, during downstream enumeration.

Result: NodeExecutionCompleted.Duration for stream nodes often measures only setup time.

### 2. Metrics observer uses this early completion duration as authoritative duration

File: src/NPipeline.Extensions.Observability/MetricsCollectingExecutionObserver.cs

- OnNodeCompleted computes endTime as startTime + e.Duration.
- It then writes RecordNodeEnd with this short duration-derived end time.

Result: NodeMetrics.DurationMs is short for stream nodes.

### 3. Item/performance metrics are recorded later during actual stream consumption

Files:
- src/NPipeline.Extensions.AI/Execution/AIStreamPassthroughExecutionStrategy.cs
- src/NPipeline/Execution/Strategies/SequentialExecutionStrategy.cs
- src/NPipeline/Execution/Strategies/BatchingExecutionStrategy.cs
- src/NPipeline/Execution/Strategies/UnbatchingExecutionStrategy.cs

- Strategies use BeginNodeScope(nodeId) inside iterators.
- Observability scope increments processed/emitted during enumeration.
- Scope is disposed at end of enumeration, after real work.

Result: throughput/average item metrics can represent real stream runtime while DurationMs remains short.

## Why Studio Still Looks Wrong After Studio Patch

Studio now correctly refreshes node duration from final metrics snapshots at pipeline completion.

However, the core snapshot itself still carries short DurationMs for stream nodes. Studio cannot infer a reliable wall-clock duration from that field alone.

## Recommended Fix (Core)

## Phase 1 (Low Risk, Immediate)

Reconcile duration with observed item performance when performance metrics indicate a longer true runtime.

### Change A: Reconcile duration in NodeMetricsBuilder.RecordPerformanceMetrics

File: src/NPipeline.Extensions.Observability/ObservabilityCollector.cs

In NodeMetricsBuilder.RecordPerformanceMetrics:

1. Keep setting _throughputItemsPerSec and _averageItemProcessingMs.
2. If _itemsProcessed > 0 and averageItemProcessingMs > 0:
   - derivedDurationMs = _itemsProcessed * averageItemProcessingMs
3. If _durationMs is null OR derivedDurationMs > _durationMs:
   - set _durationMs = derivedDurationMs
   - if _startTime has value, set _endTime = _startTime + derivedDurationMs

Intent: allow late stream-consumption performance metrics to correct an early setup-time duration.

### Change B: Avoid overwriting better perf data in observer

File: src/NPipeline.Extensions.Observability/MetricsCollectingExecutionObserver.cs

In OnNodeCompleted:

1. Keep RecordNodeEnd for success/failure bookkeeping.
2. Before calling RecordPerformanceMetrics from DurationMs:
   - Compare with existing nodeMetrics.AverageItemProcessingMs if present.
   - Only write observer-derived perf metrics if existing value is null.

Intent: do not replace stream-lifecycle perf metrics with setup-time derived metrics.

## Phase 2 (More Correct, Architectural)

Introduce a distinct dataflow-completion timing hook so stream node duration is finalized when stream consumption finishes, not when node setup returns.

Potential direction:

1. Add a stream/dataflow completion signal emitted when node scope is disposed at end of iterator consumption.
2. Use that signal to finalize duration/end time in observer/collector.
3. Keep NodeExecutionCompleted for execution lifecycle status but not as final stream wall-clock timing source.

This removes heuristics and aligns all duration math with actual stream runtime.

## Test Plan (Core Repo)

Add tests in NPipeline core to prevent regression.

### Unit tests

1. ObservabilityCollector duration reconciliation
   - Arrange: RecordNodeStart, RecordNodeEnd(short), RecordItemMetrics(8,8), RecordPerformanceMetrics(avg=2600ms)
   - Assert: DurationMs approximately 20800ms and EndTime reflects corrected duration.

2. MetricsCollectingExecutionObserver preserves existing better perf metrics
   - Arrange existing collector state with non-null AvgItemMs from stream scope.
   - Run OnNodeCompleted with short duration.
   - Assert AvgItemMs is not downgraded.

### Integration test

3. Stream transform delayed execution attribution
   - Build a pipeline with stream transform that delays per item.
   - Ensure transform DurationMs is materially greater than setup-time milliseconds and consistent with AvgItemMs * ItemsProcessed.

## Acceptance Criteria

1. For stream transforms, DurationMs in core metrics snapshot reflects stream consumption wall-clock time, not only setup return time.
2. DurationMs and AvgItemMs/Throughput no longer diverge by orders of magnitude for the same node run.
3. Studio metrics table displays AI stream-transform duration in the expected seconds range for the batched enrichment sample.

## Notes

- This issue is fundamentally in core metrics attribution.
- Studio already applies final snapshot updates, but cannot correct core DurationMs if the snapshot itself is setup-time based.
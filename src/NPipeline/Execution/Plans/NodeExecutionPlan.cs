using NPipeline.DataFlow;
using NPipeline.Graph;
using NPipeline.Pipeline;

// PipelineContext

namespace NPipeline.Execution.Plans;

/// <summary>
///     Pre-bound, per-run execution plan for a single node eliminating reflection during hot execution path.
///     Only one delegate relevant to the node kind will be populated.
/// </summary>
public sealed record NodeExecutionPlan(
    string NodeId,
    NodeKind Kind,
    Type? InputType,
    Type? OutputType,
    Func<PipelineContext, CancellationToken, Task<IDataPipe>>? ExecuteSource = null,
    Func<IDataPipe, PipelineContext, CancellationToken, Task<IDataPipe>>? ExecuteTransform = null,
    Func<IEnumerable<IDataPipe>, PipelineContext, CancellationToken, Task<IDataPipe>>? ExecuteJoin = null,
    Func<IDataPipe, PipelineContext, CancellationToken, Task>? ExecuteSink = null,
    Func<IDataPipe, PipelineContext, CancellationToken, Task<IDataPipe>>? ExecuteAggregate = null
);

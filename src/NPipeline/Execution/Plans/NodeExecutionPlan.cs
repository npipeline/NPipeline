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
    Func<PipelineContext, CancellationToken, Task<IDataStream>>? ExecuteSource = null,
    Func<IDataStream, PipelineContext, CancellationToken, Task<IDataStream>>? ExecuteTransform = null,
    Func<IEnumerable<IDataStream>, PipelineContext, CancellationToken, Task<IDataStream>>? ExecuteJoin = null,
    Func<IDataStream, PipelineContext, CancellationToken, Task>? ExecuteSink = null,
    Func<IDataStream, PipelineContext, CancellationToken, Task<IDataStream>>? ExecuteAggregate = null,
    Func<IDataStream, string, IDataStream>? AdaptOutput = null
);

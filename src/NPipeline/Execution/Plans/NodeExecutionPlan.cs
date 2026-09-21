using NPipeline.DataFlow;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Plans;

/// <summary>
///     Compiled, instance-independent execution plan for a single node, eliminating reflection on the hot execution path.
///     Only the delegate relevant to the node kind is populated.
/// </summary>
/// <remarks>
///     <para>
///         Plans are derived purely from the <see cref="NodeDefinition" /> and are therefore safe to cache and reuse across
///         pipeline runs. The node instance is supplied as an argument on every invocation rather than being captured, so a
///         cached plan always executes the node instances belonging to the current run. The execution strategy is
///         likewise supplied per invocation, resolved from the current run's node definition.
///     </para>
///     <para>
///         Capturing the instance instead would bind a plan to the first run that built it, causing later runs to execute
///         nodes that have already been disposed.
///     </para>
/// </remarks>
public sealed record NodeExecutionPlan(
    string NodeId,
    NodeKind Kind,
    Type? InputType,
    Type? OutputType,
    Func<INode, PipelineContext, CancellationToken, Task<IDataStream>>? ExecuteSource = null,
    Func<INode, IExecutionStrategy, IDataStream, PipelineContext, CancellationToken, Task<IDataStream>>? ExecuteTransform = null,
    Func<INode, IEnumerable<IDataStream>, PipelineContext, CancellationToken, Task<IDataStream>>? ExecuteJoin = null,
    Func<INode, IDataStream, PipelineContext, CancellationToken, Task>? ExecuteSink = null,
    Func<INode, IDataStream, PipelineContext, CancellationToken, Task<IDataStream>>? ExecuteAggregate = null,
    Func<IDataStream, string, IDataStream>? AdaptOutput = null
);

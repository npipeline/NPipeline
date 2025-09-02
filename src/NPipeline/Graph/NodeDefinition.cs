using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Lineage;
using NPipeline.Lineage;
using NPipeline.Nodes;

namespace NPipeline.Graph;

/// <summary>
///     Specifies the type of a node in the pipeline.
/// </summary>
public enum NodeKind
{
    /// <summary>A source node that produces data.</summary>
    Source,

    /// <summary>A transform node that processes data.</summary>
    Transform,

    /// <summary>A sink node that consumes data.</summary>
    Sink,

    /// <summary>A join node that combines multiple streams.</summary>
    Join,

    /// <summary>An aggregate node that combines multiple data items.</summary>
    Aggregate,
}

/// <summary>
///     Represents a delegate for custom merge logic that combines multiple input data pipes into one.
/// </summary>
/// <param name="node">The node performing the merge.</param>
/// <param name="dataPipes">The input data pipes to merge.</param>
/// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
/// <returns>A task that resolves to the merged output data pipe.</returns>
public delegate Task<IDataPipe> CustomMergeDelegate(INode node, IEnumerable<IDataPipe> dataPipes, CancellationToken cancellationToken);

/// <summary>
///     Represents a delegate for unwrapping lineage information in sink nodes.
/// </summary>
/// <param name="lineageInput">The input data pipe containing lineage information.</param>
/// <param name="lineageSink">The optional lineage sink for recording lineage data.</param>
/// <param name="sinkNodeId">The ID of the sink node.</param>
/// <param name="options">Optional lineage configuration options.</param>
/// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
/// <returns>The output data pipe after unwrapping lineage.</returns>
public delegate IDataPipe SinkLineageUnwrapDelegate(IDataPipe lineageInput, ILineageSink? lineageSink, string sinkNodeId,
    LineageOptions? options, CancellationToken cancellationToken);

/// <summary>
///     Represents a pre-compiled delegate for extracting join keys from an item.
/// </summary>
/// <remarks>
///     This delegate is compiled once during the builder phase to extract join keys
///     efficiently at runtime without reflection overhead.
/// </remarks>
public delegate object? JoinKeySelectorDelegate(object item);

/// <summary>
///     Represents the definition of a node within the pipeline graph.
/// </summary>
/// <remarks>
///     Pre-compiled join key selectors are stored in JoinKeySelectorRegistry and accessed at runtime,
///     not duplicated in this record. This keeps the record lightweight and makes the registry the
///     single source of truth for cached selectors.
/// </remarks>
/// <param name="Id">The unique identifier for the node.</param>
/// <param name="Name">A descriptive name for the node.</param>
/// <param name="NodeType">The type of the pipeline node, which must implement <see cref="INode" />.</param>
/// <param name="Kind">The kind of node (Source, Transform, Sink, Join, or Aggregate).</param>
/// <param name="ExecutionStrategy">The execution strategy for the node. If null, a default strategy will be used.</param>
/// <param name="ErrorHandlerType">The type of the error handler for the node. If null, no specific error handler is attached.</param>
/// <param name="InputType">The type of input data for the node.</param>
/// <param name="OutputType">The type of output data for the node.</param>
/// <param name="MergeStrategy">The merge strategy for nodes that combine multiple inputs.</param>
/// <param name="HasCustomMerge">Whether this node has custom merge logic.</param>
/// <param name="DeclaredCardinality">The declared cardinality transformation of the node.</param>
/// <param name="LineageAdapter">Optional delegate for transforming lineage data.</param>
/// <param name="LineageMapperType">The type of the lineage mapper for non-1:1 transformations.</param>
/// <param name="IsJoin">Whether this node is a join node.</param>
/// <param name="CustomMerge">Optional custom merge delegate for merging multiple input streams.</param>
/// <param name="SinkLineageUnwrap">Optional delegate for unwrapping lineage in sink nodes.</param>
public sealed record NodeDefinition(
    string Id,
    string Name,
    Type NodeType,
    NodeKind Kind,
    IExecutionStrategy? ExecutionStrategy = null,
    Type? ErrorHandlerType = null,
    Type? InputType = null,
    Type? OutputType = null,
    MergeType? MergeStrategy = null,
    bool HasCustomMerge = false,
    TransformCardinality? DeclaredCardinality = null,
    LineageAdapterDelegate? LineageAdapter = null,
    Type? LineageMapperType = null,
    bool IsJoin = false,
    CustomMergeDelegate? CustomMerge = null,
    SinkLineageUnwrapDelegate? SinkLineageUnwrap = null);

using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Lineage;
using NPipeline.Nodes;

namespace NPipeline.Graph.PipelineDelegates;

/// <summary>
///     Represents a delegate for custom merge logic that combines multiple input data pipes into one.
/// </summary>
/// <param name="node">The node performing merge.</param>
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
///     Delegate produced at build time that, given a transform node output pipe,
///     rewraps it in lineage packets using the original input lineage stream and declared cardinality.
/// </summary>
/// <param name="transformInput">Original (lineage-wrapped) transform input pipe.</param>
/// <param name="nodeId">Node id for traversal path extension.</param>
/// <param name="declaredCardinality">Declared transform cardinality.</param>
/// <param name="options">Lineage options (strict / warn config).</param>
/// <param name="cancellationToken">Cancellation token for downstream enumeration.</param>
/// <returns>Tuple of (unwrappedInput, rewrapOutputFunc).</returns>
public delegate (IDataPipe unwrappedInput, Func<IDataPipe, IDataPipe> rewrapOutput) LineageAdapterDelegate(
    IDataPipe transformInput,
    string nodeId,
    TransformCardinality declaredCardinality,
    LineageOptions? options,
    CancellationToken cancellationToken);

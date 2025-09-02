using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.DataFlow;

namespace NPipeline.Execution.Lineage;

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

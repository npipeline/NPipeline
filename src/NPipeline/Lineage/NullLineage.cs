using System.Runtime.CompilerServices;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Graph;
using NPipeline.Graph.PipelineDelegates;
using NPipeline.Pipeline;

namespace NPipeline.Lineage;

/// <summary>
///     Null object implementation of <see cref="ILineage" />.
/// </summary>
public sealed class NullLineage : ILineage
{
    /// <summary>
    ///     Shared singleton instance.
    /// </summary>
    public static readonly NullLineage Instance = new();

    /// <inheritdoc />
    public bool SupportsItemLevelLineage => false;

    /// <inheritdoc />
    public LineageAdapterDelegate? BuildLineageAdapter<TIn, TOut>(Type? lineageMapperType)
    {
        return null;
    }

    /// <inheritdoc />
    public SinkLineageUnwrapDelegate? BuildSinkLineageUnwrapDelegate<TIn>()
    {
        return null;
    }

    /// <inheritdoc />
    public LineageAdapterDelegate? BuildLineageAdapter(Type? inType, Type? outType, Type? lineageMapperType)
    {
        return null;
    }

    /// <inheritdoc />
    public SinkLineageUnwrapDelegate? BuildSinkLineageUnwrap(Type? inType)
    {
        return null;
    }

    /// <inheritdoc />
    public IDataStream WrapSourceStream(IDataStream sourcePipe, string nodeId, Guid pipelineId, string? pipelineName, LineageOptions? options)
    {
        return sourcePipe;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<object> UnwrapLineageStream(IAsyncEnumerable<object?> source, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
            yield return item!;
    }

    /// <inheritdoc />
    public (IDataStream unwrappedInput, IAsyncEnumerable<object?> inputLineageContext) PrepareInputWithLineageContext(
        IDataStream source,
        CancellationToken ct = default)
    {
        return (source, source.ToAsyncEnumerable(ct));
    }

    /// <inheritdoc />
    public IDataStream WrapNodeOutput(IDataStream output, string currentNodeId, Guid pipelineId, string? pipelineName, LineageOptions? options,
        LineageOutcomeReason outcome, CancellationToken ct = default)
    {
        return output;
    }

    /// <inheritdoc />
    public IDataStream WrapNodeOutputFromInputLineage(IDataStream output, IAsyncEnumerable<object?> inputLineageContext,
        string currentNodeId, Guid pipelineId, string? pipelineName, LineageOptions? options, LineageOutcomeReason outcome,
        Type? lineageMapperType = null, CancellationToken ct = default)
    {
        return output;
    }

    /// <inheritdoc />
    public Task RecordPipelineAsync(Type definitionType, PipelineGraph graph, PipelineContext context, IPipelineLineageSink? pipelineLineageSink)
    {
        return Task.CompletedTask;
    }
}
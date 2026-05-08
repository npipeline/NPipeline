using System.Runtime.CompilerServices;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Lineage;

namespace NPipeline.Execution.Services;

public sealed class NullLineageService : ILineageService
{
    public static readonly NullLineageService Instance = new();

    public IDataStream WrapSourceStream(IDataStream sourcePipe, string nodeId, Guid pipelineId, string? pipelineName, LineageOptions? options)
        => sourcePipe;

    public async IAsyncEnumerable<object> UnwrapLineageStream(IAsyncEnumerable<object?> source, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
            yield return item!;
    }

    public (IDataStream unwrappedInput, IAsyncEnumerable<object?> inputLineageContext) PrepareInputWithLineageContext(
        IDataStream source, CancellationToken ct = default)
        => (source, source.ToAsyncEnumerable(ct));

    public IDataStream WrapNodeOutput(IDataStream output, string currentNodeId, Guid pipelineId, string? pipelineName,
        LineageOptions? options, LineageOutcomeReason outcome, CancellationToken ct = default)
        => output;

    public IDataStream WrapNodeOutputFromInputLineage(IDataStream output, IAsyncEnumerable<object?> inputLineageContext,
        string currentNodeId, Guid pipelineId, string? pipelineName, LineageOptions? options, LineageOutcomeReason outcome,
        Type? lineageMapperType = null, CancellationToken ct = default)
        => output;
}

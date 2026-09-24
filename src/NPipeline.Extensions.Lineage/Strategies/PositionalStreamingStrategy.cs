using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;

namespace NPipeline.Lineage;

internal sealed class PositionalStreamingStrategy<TIn, TOut> : LineageMappingStrategyBase, ILineageMappingStrategy<TIn, TOut>
{
    public static readonly PositionalStreamingStrategy<TIn, TOut> Instance = new();

    private PositionalStreamingStrategy()
    {
    }

    public IAsyncEnumerable<LineagePacket<TOut>> MapAsync(IAsyncEnumerable<LineagePacket<TIn>> inputStream, IAsyncEnumerable<TOut> outputStream,
        string nodeId, Guid pipelineId, string? pipelineName, TransformCardinality cardinality, LineageOptions? options, Type? lineageMapperType,
        ILineageMapper? mapperInstance, CancellationToken ct) =>
        PositionalStreamingMap(inputStream, outputStream, nodeId, pipelineId, pipelineName, cardinality, options, ct);
}

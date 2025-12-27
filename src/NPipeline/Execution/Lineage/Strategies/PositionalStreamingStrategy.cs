using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.Lineage;

namespace NPipeline.Execution.Lineage.Strategies;

internal sealed class PositionalStreamingStrategy<TIn, TOut> : LineageMappingStrategyBase, ILineageMappingStrategy<TIn, TOut>
{
    public static readonly PositionalStreamingStrategy<TIn, TOut> Instance = new();

    private PositionalStreamingStrategy()
    {
    }

    public IAsyncEnumerable<LineagePacket<TOut>> MapAsync(IAsyncEnumerable<LineagePacket<TIn>> inputStream, IAsyncEnumerable<TOut> outputStream, string nodeId,
        TransformCardinality cardinality, LineageOptions? options, Type? lineageMapperType, ILineageMapper? mapperInstance, CancellationToken ct)
    {
        return PositionalStreamingMap(inputStream, outputStream, nodeId, cardinality, options, ct);
    }
}

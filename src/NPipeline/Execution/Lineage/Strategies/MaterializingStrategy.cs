using System.Runtime.CompilerServices;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.Lineage;

namespace NPipeline.Execution.Lineage.Strategies;

internal sealed class MaterializingStrategy<TIn, TOut> : LineageMappingStrategyBase, ILineageMappingStrategy<TIn, TOut>
{
    public static readonly MaterializingStrategy<TIn, TOut> Instance = new();

    private MaterializingStrategy()
    {
    }

    public async IAsyncEnumerable<LineagePacket<TOut>> MapAsync(IAsyncEnumerable<LineagePacket<TIn>> inputStream, IAsyncEnumerable<TOut> outputStream,
        string nodeId, TransformCardinality cardinality, LineageOptions? options, Type? lineageMapperType, ILineageMapper? mapperInstance,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var inputs = new List<LineagePacket<TIn>>();

        await foreach (var ip in inputStream.WithCancellation(ct).ConfigureAwait(false))
        {
            inputs.Add(ip);
        }

        var outputs = new List<TOut>();

        await foreach (var op in outputStream.WithCancellation(ct).ConfigureAwait(false))
        {
            outputs.Add(op);
        }

        foreach (var packet in MapMaterialized(inputs, outputs, nodeId, cardinality, options, lineageMapperType,
                     mapperInstance))
        {
            yield return packet;
        }
    }
}

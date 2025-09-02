using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.Lineage;

namespace NPipeline.Execution.Lineage.Strategies;

/// <summary>
///     Strategy abstraction for mapping lineage-wrapped input packets to lineage-wrapped output packets.
///     Implementations encapsulate distinct mapping behaviors (streaming 1:1, materializing, cap-aware, positional fallback).
/// </summary>
/// <typeparam name="TIn">Input item type.</typeparam>
/// <typeparam name="TOut">Output item type.</typeparam>
/// <summary>
///     Interface for a lineage mapping strategy transforming an input lineage packet stream + raw output stream
///     into a lineage packet output stream while preserving mismatch semantics.
/// </summary>
internal interface ILineageMappingStrategy<TIn, TOut>
{
    IAsyncEnumerable<LineagePacket<TOut>> MapAsync(
        IAsyncEnumerable<LineagePacket<TIn>> inputStream,
        IAsyncEnumerable<TOut> outputStream,
        string nodeId,
        TransformCardinality cardinality,
        LineageOptions? options,
        Type? lineageMapperType,
        ILineageMapper? mapperInstance,
        CancellationToken ct);
}

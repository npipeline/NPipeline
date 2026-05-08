using NPipeline.Graph.PipelineDelegates;

namespace NPipeline.Lineage;

public sealed class NullLineageAdapterBuilder : ILineageAdapterBuilder
{
    public static readonly NullLineageAdapterBuilder Instance = new();

    public LineageAdapterDelegate? BuildLineageAdapter<TIn, TOut>(Type? lineageMapperType) => null;
    public SinkLineageUnwrapDelegate? BuildSinkLineageUnwrapDelegate<TIn>() => null;
}

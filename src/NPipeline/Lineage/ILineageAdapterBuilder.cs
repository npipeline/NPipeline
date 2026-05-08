using NPipeline.Graph.PipelineDelegates;

namespace NPipeline.Lineage;

public interface ILineageAdapterBuilder
{
    LineageAdapterDelegate? BuildLineageAdapter<TIn, TOut>(Type? lineageMapperType);
    SinkLineageUnwrapDelegate? BuildSinkLineageUnwrapDelegate<TIn>();
}

using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;

namespace NPipeline.Execution.Lineage.Strategies;

internal static class LineageMappingStrategySelector
{
    internal static ILineageMappingStrategy<TIn, TOut> Select<TIn, TOut>(Type? mapperType, TransformCardinality cardinality, LineageOptions? options)
    {
        if (mapperType is null && cardinality == TransformCardinality.OneToOne)
            return StreamingOneToOneStrategy<TIn, TOut>.Instance;

        var cap = options?.MaterializationCap;

        if (cap is not null && cap > 0)
            return CapAwareMaterializingStrategy<TIn, TOut>.Instance;

        return MaterializingStrategy<TIn, TOut>.Instance;
    }
}

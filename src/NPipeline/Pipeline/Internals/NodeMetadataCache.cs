using System.Collections.Concurrent;
using NPipeline.Attributes.Lineage;
using NPipeline.Attributes.Nodes;
using NPipeline.Execution;
using NPipeline.Lineage;
using NPipeline.Nodes;

namespace NPipeline.Pipeline.Internals;

/// <summary>
///     Immutable metadata snapshot describing node behavior discovered via reflection.
/// </summary>
internal sealed record NodeMeta(
    MergeType? MergeStrategy,
    bool HasCustomMerge,
    TransformCardinality? DeclaredCardinality,
    Type? LineageMapperType);

/// <summary>
///     Caches reflection-derived metadata about node types so we only scan attributes / interfaces once.
/// </summary>
internal static class NodeMetadataCache
{
    private static readonly ConcurrentDictionary<Type, NodeMeta> Cache = new();

    public static NodeMeta Get(Type nodeType)
    {
        return Cache.GetOrAdd(nodeType, static t =>
        {
            var mergeAttr = t.GetCustomAttributes(typeof(MergeStrategyAttribute), false).FirstOrDefault() as MergeStrategyAttribute;
            var mergeStrategy = mergeAttr?.MergeType;

            var hasCustomMerge = t.GetInterfaces().Any(i =>
                (i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICustomMergeNode<>)) || i == typeof(ICustomMergeNodeUntyped));

            var cardinalityAttr = t.GetCustomAttributes(typeof(TransformCardinalityAttribute), false).FirstOrDefault() as TransformCardinalityAttribute;
            var declaredCardinality = cardinalityAttr?.Cardinality;
            var mapperAttr = t.GetCustomAttributes(typeof(LineageMapperAttribute), false).FirstOrDefault() as LineageMapperAttribute;
            var mapperType = mapperAttr?.MapperType;
            return new NodeMeta(mergeStrategy, hasCustomMerge, declaredCardinality, mapperType);
        });
    }
}

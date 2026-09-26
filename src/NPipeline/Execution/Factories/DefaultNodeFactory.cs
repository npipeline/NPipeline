using System.Collections.Concurrent;
using System.Linq.Expressions;
using NPipeline.Execution.Caching;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Execution.Factories;

/// <summary>
///     Default in-core implementation of <see cref="INodeFactory" /> for non-DI scenarios.
///     Uses compiled expression delegates for optimized node instantiation with parameterless constructors.
///     Falls back to Activator.CreateInstance for types without parameterless constructors.
///     For complex dependency injection scenarios, use DIContainerNodeFactory or pre-configured instances.
/// </summary>
public sealed class DefaultNodeFactory : INodeFactory
{
    // Cache of compiled factory delegates for fast instantiation. The delegate depends only on the node type, so the
    // cache is process-wide rather than per factory instance.
    private static readonly ConcurrentDictionary<Type, Func<INode>?> CompiledFactories = new();

    /// <inheritdoc />
    public INode Create(NodeDefinition nodeDefinition, PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(nodeDefinition);

        // Pre-configured instance takes precedence.
        if (graph.PreconfiguredNodeInstances.TryGetValue(nodeDefinition.Id, out var preconfigured))
            return preconfigured;

        // Try to get or create a compiled factory delegate
        var nodeType = nodeDefinition.NodeType;
        var factory = CollectibleAwareCache.GetOrAdd(CompiledFactories, nodeType, nodeType.IsCollectible, BuildCompiledFactory);

        // If we successfully compiled a factory, use it (fast path)
        if (factory != null)
            return factory();

        // Fall back to Activator.CreateInstance (slow path for types without parameterless constructors)
        try
        {
            var instance = Activator.CreateInstance(nodeDefinition.NodeType)
                           ?? throw new InvalidOperationException(
                               $"Failed to create node instance of type '{nodeDefinition.NodeType.FullName}'. Activator returned null.");

            return (INode)instance;
        }
        catch (MissingMethodException)
        {
            throw new InvalidOperationException(
                $"Failed to create node instance of type '{nodeDefinition.NodeType.FullName}'. " +
                "Ensure a public parameterless constructor exists, or register a pre-configured instance using PipelineBuilder.AddPreconfiguredNodeInstance(). " +
                "For dependency injection scenarios, use DIContainerNodeFactory from NPipeline.Extensions.DependencyInjection or provide a custom INodeFactory implementation.");
        }
    }

    /// <summary>
    ///     Builds a compiled factory delegate for fast instantiation of types with parameterless constructors.
    ///     Returns null if the type doesn't have a public parameterless constructor.
    /// </summary>
    /// <param name="nodeType">The type to create a factory for.</param>
    /// <returns>A compiled factory delegate, or null if the type doesn't have a parameterless constructor.</returns>
    private static Func<INode>? BuildCompiledFactory(Type nodeType)
    {
        // Check if the type has a public parameterless constructor
        var constructor = nodeType.GetConstructor(Type.EmptyTypes);

        if (constructor == null)
            return null; // No parameterless constructor, fall back to Activator

        try
        {
            // Build an expression: () => new TNode()
            var newExpression = Expression.New(constructor);
            var castExpression = Expression.Convert(newExpression, typeof(INode));
            var lambda = Expression.Lambda<Func<INode>>(castExpression);

            // Compile the expression into a delegate
            return lambda.Compile();
        }
        catch
        {
            // If compilation fails for any reason, fall back to Activator
            return null;
        }
    }
}

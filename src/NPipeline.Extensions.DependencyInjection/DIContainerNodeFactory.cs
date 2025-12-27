using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Extensions.DependencyInjection;

/// <summary>
///     An implementation of <see cref="INodeFactory" /> that uses an <see cref="IServiceProvider" /> to create node instances.
///     Uses compiled expression trees for efficient instance creation in hot paths.
/// </summary>
/// <param name="serviceProvider">The service provider to resolve nodes from.</param>
/// <param name="errorHandlerFactory">The factory to create error handlers.</param>
internal sealed class DiContainerNodeFactory(IServiceProvider serviceProvider, IErrorHandlerFactory errorHandlerFactory) : INodeFactory
{
    private static readonly ConcurrentDictionary<Type, Func<IServiceProvider, object?>> _constructorCache = new();

    /// <inheritdoc />
    public INode Create(NodeDefinition nodeDefinition, PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(nodeDefinition);

        // Check for a pre-configured instance first.
        if (graph.PreconfiguredNodeInstances.TryGetValue(nodeDefinition.Id, out var preconfiguredInstance))
            return preconfiguredInstance;

        // First, try to get the node from the service provider directly.
        // This allows for singleton registrations in tests or specific application scenarios.
        var node = serviceProvider.GetService(nodeDefinition.NodeType);

        // If the node is not registered as a service, fall back to creating a new instance,
        // allowing the container to resolve its dependencies using compiled expression trees.
        if (node is null)
        {
            var constructor = _constructorCache.GetOrAdd(nodeDefinition.NodeType, BuildConstructor);
            node = constructor(serviceProvider);
        }

        if (node is null)
        {
            throw new InvalidOperationException(
                $"Failed to create an instance of node type '{nodeDefinition.NodeType.FullName}'. Ensure it is registered in the service provider or its dependencies are resolvable.");
        }

        if (node is ITransformNode transformNode)
        {
            if (nodeDefinition.ExecutionStrategy is not null)
                transformNode.ExecutionStrategy = nodeDefinition.ExecutionStrategy;

            if (nodeDefinition.ErrorHandlerType is not null)
                transformNode.ErrorHandler = errorHandlerFactory.CreateNodeErrorHandler(nodeDefinition.ErrorHandlerType);
        }

        return (INode)node;
    }

    /// <summary>
    ///     Builds a compiled expression tree constructor for the given type.
    ///     This avoids reflection overhead on subsequent calls for the same type.
    /// </summary>
    private static Func<IServiceProvider, object?> BuildConstructor(Type type)
    {
        var constructors = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance);

        if (constructors.Length == 0)
            return _ => null;

        // Select the constructor with the most parameters (greedy selection)
        // This matches ActivatorUtilities behavior
        var constructor = constructors.OrderByDescending(c => c.GetParameters().Length).First();
        var parameters = constructor.GetParameters();

        var serviceProviderParam = Expression.Parameter(typeof(IServiceProvider), "serviceProvider");
        var parameterExpressions = new Expression[parameters.Length];

        // Get the GetService method from IServiceProvider
        var getServiceMethod = typeof(IServiceProvider).GetMethod("GetService", [typeof(Type)])!;

        for (var i = 0; i < parameters.Length; i++)
        {
            var parameterType = parameters[i].ParameterType;

            // Call serviceProvider.GetService(parameterType) and cast the result
            parameterExpressions[i] = Expression.Convert(
                Expression.Call(
                    serviceProviderParam,
                    getServiceMethod,
                    Expression.Constant(parameterType)
                ),
                parameterType
            );
        }

        // Create the constructor call expression
        var newExpression = Expression.New(constructor, parameterExpressions);

        var lambda = Expression.Lambda<Func<IServiceProvider, object>>(
            Expression.Convert(newExpression, typeof(object)),
            serviceProviderParam
        );

        var compiled = lambda.Compile();

        return sp =>
        {
            try
            {
                return compiled(sp);
            }
            catch
            {
                // Fall back to ActivatorUtilities if compilation-based instantiation fails
                return ActivatorUtilities.CreateInstance(sp, type);
            }
        };
    }
}

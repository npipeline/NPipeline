using Microsoft.Extensions.DependencyInjection;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Extensions.DependencyInjection;

/// <summary>
///     An implementation of <see cref="INodeFactory" /> that uses an <see cref="IServiceProvider" /> to create node instances.
/// </summary>
/// <param name="serviceProvider">The service provider to resolve nodes from.</param>
/// <param name="errorHandlerFactory">The factory to create error handlers.</param>
internal sealed class DiContainerNodeFactory(IServiceProvider serviceProvider, IErrorHandlerFactory errorHandlerFactory) : INodeFactory
{
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
        // allowing the container to resolve its dependencies.
        if (node is null)
            node = ActivatorUtilities.CreateInstance(serviceProvider, nodeDefinition.NodeType);

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
}

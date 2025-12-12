using NPipeline.ErrorHandling;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Execution.Factories;

/// <summary>
///     Default in-core implementation of <see cref="INodeFactory" /> for non-DI scenarios.
///     Uses Activator.CreateInstance with parameterless constructors.
///     For complex dependency injection scenarios, use DIContainerNodeFactory or pre-configured instances.
/// </summary>
public sealed class DefaultNodeFactory(IErrorHandlerFactory? errorHandlerFactory = null) : INodeFactory
{
    private readonly IErrorHandlerFactory _errorHandlerFactory = errorHandlerFactory ?? new DefaultErrorHandlerFactory();

    /// <inheritdoc />
    public INode Create(NodeDefinition nodeDefinition, PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(nodeDefinition);

        // Pre-configured instance takes precedence.
        if (graph.PreconfiguredNodeInstances.TryGetValue(nodeDefinition.Id, out var preconfigured))
            return preconfigured;

        // Try simple parameterless constructor
        try
        {
            var instance = Activator.CreateInstance(nodeDefinition.NodeType)
                           ?? throw new InvalidOperationException(
                               $"Failed to create node instance of type '{nodeDefinition.NodeType.FullName}'. Activator returned null.");

            return ConfigureNode((INode)instance, nodeDefinition);
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
    ///     Configures a node instance with execution strategy and error handler if applicable.
    /// </summary>
    /// <param name="instance">The node instance to configure.</param>
    /// <param name="nodeDefinition">The node definition containing configuration.</param>
    /// <returns>The configured node instance.</returns>
    private INode ConfigureNode(INode instance, NodeDefinition nodeDefinition)
    {
        if (instance is ITransformNode transformNode)
        {
            // Apply execution strategy if specified, falling back to SequentialExecutionStrategy.
            transformNode.ExecutionStrategy = nodeDefinition.ExecutionStrategy ?? new SequentialExecutionStrategy();

            // Apply error handler if specified.
            if (nodeDefinition.ErrorHandlerType is not null)
                transformNode.ErrorHandler = _errorHandlerFactory.CreateNodeErrorHandler(nodeDefinition.ErrorHandlerType);
        }

        return instance;
    }
}

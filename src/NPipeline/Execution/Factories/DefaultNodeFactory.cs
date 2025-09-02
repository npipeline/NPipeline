using NPipeline.ErrorHandling;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Execution.Factories;

/// <summary>
///     Default in-core implementation of <see cref="INodeFactory" /> for non-DI scenarios.
///     Uses Activator.CreateInstance and applies builder-specified execution strategy / error handler metadata.
///     Keeps the core dependency-free while providing a ready-to-use manual factory.
/// </summary>
public sealed class DefaultNodeFactory : INodeFactory
{
    private readonly IErrorHandlerFactory _errorHandlerFactory;

    /// <summary>
    ///     Creates a new <see cref="DefaultNodeFactory" />.
    /// </summary>
    /// <param name="errorHandlerFactory">Optional error handler factory for creating node error handlers; defaults to <see cref="DefaultErrorHandlerFactory" />.</param>
    public DefaultNodeFactory(IErrorHandlerFactory? errorHandlerFactory = null)
    {
        _errorHandlerFactory = errorHandlerFactory ?? new DefaultErrorHandlerFactory();
    }

    /// <inheritdoc />
    public INode Create(NodeDefinition nodeDefinition, PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(nodeDefinition);

        // Pre-configured instance takes precedence.
        if (graph.PreconfiguredNodeInstances.TryGetValue(nodeDefinition.Id, out var preconfigured))
            return preconfigured;

        object? instance = null;

        // First try simple parameterless path
        if (nodeDefinition.NodeType.GetConstructor(Type.EmptyTypes) is not null)
            instance = Activator.CreateInstance(nodeDefinition.NodeType);
        else
        {
            // Attempt heuristic-based ctor resolution: pick the public ctor with the fewest parameters where all parameters are either optional
            // or of a trivially satisfiable known type (currently IErrorHandlerFactory). This keeps DefaultNodeFactory lightweight without full DI.
            var ctors = nodeDefinition.NodeType.GetConstructors();
            var ordered = ctors.OrderBy(c => c.GetParameters().Length).ToList();

            foreach (var ctor in ordered)
            {
                var parms = ctor.GetParameters();
                var args = new object?[parms.Length];
                var viable = true;

                for (var i = 0; i < parms.Length; i++)
                {
                    var p = parms[i];

                    if (p.HasDefaultValue)
                    {
                        args[i] = p.DefaultValue;
                        continue;
                    }

                    // Known injectable types
                    if (p.ParameterType.IsAssignableFrom(typeof(IErrorHandlerFactory)))
                    {
                        args[i] = _errorHandlerFactory;
                        continue;
                    }

                    // Give up on this constructor
                    viable = false;
                    break;
                }

                if (!viable)
                    continue;

                try
                {
                    instance = ctor.Invoke(args);
                    break;
                }
                catch
                {
                    // Try next ctor
                }
            }
        }

        instance ??= Activator.CreateInstance(nodeDefinition.NodeType) ?? throw new InvalidOperationException(
            $"Failed to create node instance of type '{nodeDefinition.NodeType.FullName}'. Ensure a public parameterless constructor exists, provide optional parameters, or register a pre-configured instance.");

        if (instance is ITransformNode transformNode)
        {
            // Apply execution strategy if specified.
            if (nodeDefinition.ExecutionStrategy is not null)
                transformNode.ExecutionStrategy = nodeDefinition.ExecutionStrategy;

            // Apply error handler if specified.
            if (nodeDefinition.ErrorHandlerType is not null)
                transformNode.ErrorHandler = _errorHandlerFactory.CreateNodeErrorHandler(nodeDefinition.ErrorHandlerType);
        }

        return (INode)instance;
    }
}

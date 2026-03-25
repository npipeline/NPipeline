using NPipeline.Pipeline;

namespace NPipeline.Extensions.DependencyInjection;

/// <summary>
/// Registry of all IPipelineDefinition types discovered during service registration.
/// Used by NPipeline.Studio to enumerate pipeline definitions for graph extraction.
/// </summary>
public sealed class PipelineDefinitionRegistry
{
    private readonly object _gate = new();
    private readonly List<Type> _definitionTypes = [];
    private readonly HashSet<Type> _registeredTypes = [];

    /// <summary>
    /// Gets all registered pipeline definition types.
    /// </summary>
    public IReadOnlyList<Type> DefinitionTypes
    {
        get
        {
            lock (_gate)
            {
                return _definitionTypes.ToArray();
            }
        }
    }

    /// <summary>
    /// Registers a pipeline definition type.
    /// </summary>
    /// <param name="type">The pipeline definition type to register.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="type"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="type"/> does not implement <see cref="IPipelineDefinition"/>.</exception>
    internal void Register(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        if (!typeof(IPipelineDefinition).IsAssignableFrom(type))
        {
            throw new ArgumentException(
                $"Type '{type.FullName}' does not implement '{typeof(IPipelineDefinition).FullName}'.",
                nameof(type));
        }

        lock (_gate)
        {
            if (_registeredTypes.Add(type))
            {
                _definitionTypes.Add(type);
            }
        }
    }
}

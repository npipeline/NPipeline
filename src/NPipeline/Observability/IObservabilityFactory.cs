namespace NPipeline.Observability;

/// <summary>
///     A factory for resolving observability-related components.
/// </summary>
public interface IObservabilityFactory
{
    /// <summary>
    ///     Resolves an optional observability collector for tracking performance metrics.
    /// </summary>
    /// <returns>An <see cref="IObservabilityCollector" /> instance or null if observability is not enabled.</returns>
    IObservabilityCollector? ResolveObservabilityCollector();
}

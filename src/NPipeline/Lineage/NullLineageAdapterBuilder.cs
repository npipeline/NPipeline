using NPipeline.Graph.PipelineDelegates;

namespace NPipeline.Lineage;

/// <summary>
/// A null implementation of <see cref="ILineageAdapterBuilder"/> that does not build any lineage adapters.
/// </summary>
public sealed class NullLineageAdapterBuilder : ILineageAdapterBuilder
{
    /// <summary>
    /// Gets the singleton instance of <see cref="NullLineageAdapterBuilder"/>.
    /// </summary>
    public static readonly NullLineageAdapterBuilder Instance = new();

    /// <summary>
    /// Builds a lineage adapter delegate (always returns null in null implementation).
    /// </summary>
    /// <typeparam name="TIn">The input type for the lineage data.</typeparam>
    /// <typeparam name="TOut">The output type for the lineage data.</typeparam>
    /// <param name="lineageMapperType">The optional type of the lineage mapper to use.</param>
    /// <returns>Always returns null.</returns>
    public LineageAdapterDelegate? BuildLineageAdapter<TIn, TOut>(Type? lineageMapperType) => null;
    
    /// <summary>
    /// Builds a delegate for unwrapping lineage data at sink nodes (always returns null in null implementation).
    /// </summary>
    /// <typeparam name="TIn">The input type of lineage data to unwrap.</typeparam>
    /// <returns>Always returns null.</returns>
    public SinkLineageUnwrapDelegate? BuildSinkLineageUnwrapDelegate<TIn>() => null;
}

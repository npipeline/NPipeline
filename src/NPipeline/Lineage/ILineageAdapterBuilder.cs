using NPipeline.Graph.PipelineDelegates;

namespace NPipeline.Lineage;

/// <summary>
/// Builds lineage adapter delegates for transforming lineage data between different types.
/// </summary>
public interface ILineageAdapterBuilder
{
    /// <summary>
    /// Builds a lineage adapter delegate that transforms lineage data from one type to another.
    /// </summary>
    /// <typeparam name="TIn">The input type for the lineage data.</typeparam>
    /// <typeparam name="TOut">The output type for the lineage data.</typeparam>
    /// <param name="lineageMapperType">The optional type of the lineage mapper to use.</param>
    /// <returns>A lineage adapter delegate, or null if no adapter can be built.</returns>
    LineageAdapterDelegate? BuildLineageAdapter<TIn, TOut>(Type? lineageMapperType);
    
    /// <summary>
    /// Builds a delegate for unwrapping lineage data at sink nodes.
    /// </summary>
    /// <typeparam name="TIn">The input type of lineage data to unwrap.</typeparam>
    /// <returns>A sink lineage unwrap delegate, or null if no delegate can be built.</returns>
    SinkLineageUnwrapDelegate? BuildSinkLineageUnwrapDelegate<TIn>();
}

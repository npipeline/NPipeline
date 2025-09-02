using NPipeline.Configuration;

namespace NPipeline.Lineage;

/// <summary>
///     Defines the contract for collecting data lineage information during pipeline execution.
/// </summary>
public interface ILineageCollector
{
    /// <summary>
    ///     Creates a new lineage packet for a data item entering the pipeline.
    /// </summary>
    /// <typeparam name="T">The type of the data item.</typeparam>
    /// <param name="item">The data item.</param>
    /// <param name="sourceNodeId">The ID of the source node.</param>
    /// <returns>A lineage packet wrapping the item.</returns>
    LineagePacket<T> CreateLineagePacket<T>(T item, string sourceNodeId);

    /// <summary>
    ///     Records a hop in the lineage trail for an item.
    /// </summary>
    /// <param name="lineageId">The unique ID of the item being tracked.</param>
    /// <param name="hop">The lineage hop to record.</param>
    void RecordHop(Guid lineageId, LineageHop hop);

    /// <summary>
    ///     Determines if lineage should be collected for a given item based on sampling settings.
    /// </summary>
    /// <param name="lineageId">The unique ID of the item.</param>
    /// <param name="options">The lineage options containing sampling configuration.</param>
    /// <returns>True if lineage should be collected for this item.</returns>
    bool ShouldCollectLineage(Guid lineageId, LineageOptions? options);
}

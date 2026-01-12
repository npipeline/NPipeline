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

    /// <summary>
    ///     Gets the lineage information for a specific item.
    /// </summary>
    /// <param name="lineageId">The unique ID of the item.</param>
    /// <returns>The lineage information, or null if not found.</returns>
    LineageInfo? GetLineageInfo(Guid lineageId);

    /// <summary>
    ///     Gets all collected lineage information.
    /// </summary>
    /// <returns>A read-only collection of all lineage information.</returns>
    IReadOnlyList<LineageInfo> GetAllLineageInfo();

    /// <summary>
    ///     Clears all collected lineage information.
    /// </summary>
    /// <remarks>
    ///     Use this to reset the collector between pipeline runs or to free memory.
    /// </remarks>
    void Clear();
}

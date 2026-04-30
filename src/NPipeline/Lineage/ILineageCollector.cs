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
    ///     Records a lineage event.
    /// </summary>
    /// <param name="record">Record to persist.</param>
    void Record(LineageRecord record);

    /// <summary>
    ///     Determines if lineage should be collected for a given item based on sampling settings.
    /// </summary>
    /// <param name="correlationId">The unique ID of the item.</param>
    /// <param name="options">The lineage options containing sampling configuration.</param>
    /// <returns>True if lineage should be collected for this item.</returns>
    bool ShouldCollectLineage(Guid correlationId, LineageOptions? options);

    /// <summary>
    ///     Gets the complete event history for a correlation.
    /// </summary>
    /// <param name="correlationId">The unique ID of the item.</param>
    /// <returns>Ordered event history for the correlation.</returns>
    IReadOnlyList<LineageRecord> GetCorrelationHistory(Guid correlationId);

    /// <summary>
    ///     Gets the final terminal reason for a correlation, when present.
    /// </summary>
    /// <param name="correlationId">The unique ID of the item.</param>
    /// <returns>Terminal reason or null when unresolved.</returns>
    LineageOutcomeReason? GetTerminalReason(Guid correlationId);

    /// <summary>
    ///     Gets all collected lineage records across all correlations.
    /// </summary>
    /// <returns>A read-only collection of all lineage records.</returns>
    IReadOnlyList<LineageRecord> GetAllRecords();

    /// <summary>
    ///     Gets correlation ids that do not have a terminal lineage record.
    /// </summary>
    /// <returns>Correlation ids with unresolved terminal state.</returns>
    IReadOnlyList<Guid> GetUnresolvedCorrelations();

    /// <summary>
    ///     Clears all collected lineage information.
    /// </summary>
    /// <remarks>
    ///     Use this to reset the collector between pipeline runs or to free memory.
    /// </remarks>
    void Clear();
}

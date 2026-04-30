namespace NPipeline.Lineage;

/// <summary>
///     Explicit terminal and non-terminal outcomes for correlation-scoped lineage records.
/// </summary>
public enum LineageOutcomeReason
{
    /// <summary>
    ///     The item produced output that continued in the pipeline.
    /// </summary>
    Emitted = 0,

    /// <summary>
    ///     The item was consumed by a terminal node with no downstream emission.
    /// </summary>
    ConsumedWithoutEmission = 1,

    /// <summary>
    ///     The item was filtered or skipped.
    /// </summary>
    FilteredOut = 2,

    /// <summary>
    ///     The item was redirected to a dead-letter sink.
    /// </summary>
    DeadLettered = 3,

    /// <summary>
    ///     The item failed processing.
    /// </summary>
    Error = 4,

    /// <summary>
    ///     The item was dropped due to queue backpressure policy.
    /// </summary>
    DroppedByBackpressure = 5,

    /// <summary>
    ///     The item was emitted by a join operation.
    /// </summary>
    Joined = 6,

    /// <summary>
    ///     The item was emitted by an aggregate operation.
    /// </summary>
    Aggregated = 7,
}

/// <summary>
///     A lineage event record for a specific correlation at a specific node.
/// </summary>
/// <param name="CorrelationId">Source-assigned correlation identifier.</param>
/// <param name="NodeId">Node identifier where this record was produced.</param>
/// <param name="PipelineId">Pipeline identity for the current run.</param>
/// <param name="OutcomeReason">Outcome reason for this record.</param>
/// <param name="IsTerminal">True when this record represents a terminal outcome for the correlation at this branch.</param>
/// <param name="TraversalPath">Qualified traversal path known at record time.</param>
/// <param name="PipelineName">Optional pipeline name.</param>
/// <param name="TimestampUtc">UTC timestamp for record creation.</param>
/// <param name="RetryCount">Optional retry count associated with this outcome.</param>
/// <param name="ContributorCorrelationIds">Optional contributing correlation ids for many-to-one mappings.</param>
/// <param name="ContributorInputIndices">Optional contributing input indices for mapper/materialized mappings.</param>
/// <param name="InputContributorCount">Optional count of contributing inputs.</param>
/// <param name="OutputEmissionCount">Optional count of outputs emitted by the contributor(s).</param>
/// <param name="Cardinality">Observed cardinality at this hop.</param>
/// <param name="InputSnapshot">Optional input snapshot.</param>
/// <param name="OutputSnapshot">Optional output snapshot.</param>
/// <param name="Data">Optional payload data (typically redacted in production).</param>
public sealed record LineageRecord(
    Guid CorrelationId,
    string NodeId,
    Guid PipelineId,
    LineageOutcomeReason OutcomeReason,
    bool IsTerminal,
    IReadOnlyList<string> TraversalPath,
    string? PipelineName = null,
    DateTimeOffset TimestampUtc = default,
    int? RetryCount = null,
    IReadOnlyList<Guid>? ContributorCorrelationIds = null,
    IReadOnlyList<int>? ContributorInputIndices = null,
    int? InputContributorCount = null,
    int? OutputEmissionCount = null,
    ObservedCardinality Cardinality = ObservedCardinality.Unknown,
    object? InputSnapshot = null,
    object? OutputSnapshot = null,
    object? Data = null)
{
    /// <summary>
    ///     Indicates lineage emission was truncated due to configured per-item cap.
    /// </summary>
    public bool Truncated { get; init; }

    /// <summary>
    ///     Returns a record with a normalized timestamp and deterministic contributor ordering.
    /// </summary>
    public LineageRecord Normalize()
    {
        var normalizedTime = TimestampUtc == default
            ? DateTimeOffset.UtcNow
            : TimestampUtc;

        IReadOnlyList<Guid>? normalizedContributors = null;

        if (ContributorCorrelationIds is { Count: > 0 })
        {
            normalizedContributors = ContributorCorrelationIds
                .Distinct()
                .OrderBy(static id => id)
                .ToArray();
        }

        IReadOnlyList<int>? normalizedIndices = null;

        if (ContributorInputIndices is { Count: > 0 })
        {
            normalizedIndices = ContributorInputIndices
                .Distinct()
                .OrderBy(static i => i)
                .ToArray();
        }

        return this with
        {
            TimestampUtc = normalizedTime,
            ContributorCorrelationIds = normalizedContributors,
            ContributorInputIndices = normalizedIndices,
        };
    }
}

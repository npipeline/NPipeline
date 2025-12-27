namespace NPipeline.Lineage;

/// <summary>
///     Represents a single hop through a node for data lineage tracking.
///     Contains only lineage-specific information, not performance metrics.
/// </summary>
/// <param name="NodeId">The node identifier for this hop.</param>
/// <param name="Outcome">Decision flags describing the hop outcome.</param>
/// <param name="Cardinality">Observed cardinality for this item at the hop.</param>
/// <param name="InputContributorCount">Number of input items contributing to this output (materialized/mapper paths).</param>
/// <param name="OutputEmissionCount">Number of outputs emitted for the contributing input(s).</param>
/// <param name="AncestryInputIndices">Optional input indices from the materialized mapping when available.</param>
/// <param name="Truncated">True when per-item hop record count exceeded configured cap and was truncated.</param>
public sealed record LineageHop(
    string NodeId,
    HopDecisionFlags Outcome,
    ObservedCardinality Cardinality,
    int? InputContributorCount,
    int? OutputEmissionCount,
    IReadOnlyList<int>? AncestryInputIndices,
    bool Truncated);

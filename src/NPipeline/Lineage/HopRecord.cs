namespace NPipeline.Lineage;

// Compact, allocation-conscious per-hop lineage enrichment for deep debugging.
/// <summary>
///     Flags describing the decisions taken for a single processing hop in the pipeline,
///     used for compact, allocation-conscious per-hop lineage enrichment for deep debugging.
/// </summary>
[Flags]
public enum HopDecisionFlags
{
    /// <summary>No flags set.</summary>
    None = 0,

    /// <summary>A value was emitted by the hop.</summary>
    Emitted = 1 << 0,

    /// <summary>The item was filtered out by the hop.</summary>
    FilteredOut = 1 << 1,

    /// <summary>The hop performed a join.</summary>
    Joined = 1 << 2,

    /// <summary>The hop performed aggregation.</summary>
    Aggregated = 1 << 3,

    /// <summary>The hop's operation was retried.</summary>
    Retried = 1 << 4,

    /// <summary>An error occurred while processing the item.</summary>
    Error = 1 << 5,

    /// <summary>The item was sent to a dead-letter queue.</summary>
    DeadLettered = 1 << 6,
}

/// <summary>
///     Observed cardinality of the output from a single processing hop.
/// </summary>
public enum ObservedCardinality
{
    /// <summary>Cardinality is not known.</summary>
    Unknown = 0,

    /// <summary>No items were observed.</summary>
    Zero = 1,

    /// <summary>Exactly one item was observed.</summary>
    One = 2,

    /// <summary>More than one item was observed.</summary>
    Many = 3,
}

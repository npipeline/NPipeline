namespace NPipeline.Lineage;

// Compact, allocation-conscious per-hop lineage enrichment for deep debugging.
[Flags]
public enum HopDecisionFlags
{
    None = 0,
    Emitted = 1 << 0,
    FilteredOut = 1 << 1,
    Joined = 1 << 2,
    Aggregated = 1 << 3,
    Retried = 1 << 4,
    Error = 1 << 5,
    DeadLettered = 1 << 6,
}

public enum ObservedCardinality
{
    Unknown = 0,
    Zero = 1,
    One = 2,
    Many = 3,
}

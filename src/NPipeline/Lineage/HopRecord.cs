namespace NPipeline.Lineage;

/// <summary>
///     Observed cardinality for a correlation record at a node.
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

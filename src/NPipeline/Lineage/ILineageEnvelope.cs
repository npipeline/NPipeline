namespace NPipeline.Lineage;

/// <summary>
///     Minimal envelope abstraction for lineage-wrapped data items enabling unwrap without reflection.
/// </summary>
public interface ILineageEnvelope
{
    /// <summary>The underlying data object carried by the envelope.</summary>
    object? Data { get; }

    /// <summary>Stable lineage identifier assigned at the source.</summary>
    Guid LineageId { get; }

    /// <summary>Indicates whether this item was selected for lineage collection (sampling).</summary>
    bool Collect { get; }
}

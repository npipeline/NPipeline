using System.Collections.Immutable;

namespace NPipeline.Lineage;

/// <summary>
///     Minimal envelope abstraction for lineage-wrapped data items enabling unwrap without reflection.
/// </summary>
public interface ILineageEnvelope
{
    /// <summary>The underlying data object carried by the envelope.</summary>
    object? Data { get; }

    /// <summary>Stable correlation identifier assigned at the source.</summary>
    Guid CorrelationId { get; }

    /// <summary>Indicates whether this item was selected for lineage collection (sampling).</summary>
    bool Collect { get; }

    /// <summary>Traversal path accumulated for this item.</summary>
    ImmutableList<string> TraversalPath { get; }

    /// <summary>Recorded lineage hops accumulated for this item.</summary>
    ImmutableList<LineageHop> LineageHops { get; }
}

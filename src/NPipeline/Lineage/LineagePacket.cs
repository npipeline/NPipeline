using System.Collections.Immutable;

namespace NPipeline.Lineage;

/// <summary>
///     Internal wrapper to carry lineage information alongside the data.
///     This is intentionally internal to hide the implementation detail from the user.
/// </summary>
/// <typeparam name="T">The type of the data being carried.</typeparam>
/// <param name="Data">The actual data item.</param>
/// <param name="LineageId">A unique ID assigned at the source for this item.</param>
/// <param name="TraversalPath">List of node IDs it has passed through.</param>
public sealed record LineagePacket<T>(
    T Data,
    Guid LineageId,
    ImmutableList<string> TraversalPath)
    : ILineageEnvelope
{
    /// <summary>
    ///     Collected lineage-specific hop information.
    /// </summary>
    public ImmutableList<LineageHop> LineageHops { get; init; } = ImmutableList<LineageHop>.Empty;

    /// <summary>
    ///     Whether this item is selected for lineage collection (sampling). Defaults to true.
    /// </summary>
    public bool Collect { get; init; } = true;

    // Explicit interface implementation returns boxed Data for generic transparency
    object? ILineageEnvelope.Data => Data;
}

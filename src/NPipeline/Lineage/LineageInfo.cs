namespace NPipeline.Lineage;

/// <summary>
///     Represents a completed lineage record for a single data item.
///     This is the public data transfer object sent to an <see cref="ILineageSink" />.
/// </summary>
/// <param name="Data">The final data item at the end of a pipeline path (nullable when redacted).</param>
/// <param name="LineageId">The unique ID assigned to the item at its source.</param>
/// <param name="TraversalPath">The complete, ordered list of node IDs the item passed through.</param>
/// <param name="LineageHops">Optional per-hop lineage records collected along the path.</param>
public sealed record LineageInfo(
    object? Data,
    Guid LineageId,
    IReadOnlyList<string> TraversalPath,
    IReadOnlyList<LineageHop> LineageHops);

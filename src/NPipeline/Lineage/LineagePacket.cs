using System.Collections.Immutable;

namespace NPipeline.Lineage;

/// <summary>
///     Internal wrapper to carry lineage information alongside the data.
///     This is intentionally internal to hide the implementation detail from the user.
/// </summary>
/// <remarks>
///     Both collections are <see cref="ImmutableArray{T}" />: a struct over a flat array, so an append
///     allocates one array rather than a tree spine and enumeration allocates nothing. The properties
///     normalise a default (uninitialised) <see cref="ImmutableArray{T}" /> to empty, so a packet is
///     always safe to enumerate.
/// </remarks>
/// <typeparam name="T">The type of the data being carried.</typeparam>
/// <param name="Data">The actual data item.</param>
/// <param name="CorrelationId">A unique correlation ID assigned at the source for this item.</param>
/// <param name="TraversalPath">Node IDs it has passed through.</param>
public sealed record LineagePacket<T>(
    T Data,
    Guid CorrelationId,
    ImmutableArray<string> TraversalPath)
    : ILineageEnvelope
{
    private readonly ImmutableArray<LineageRecord> _lineageRecords = [];

    private readonly ImmutableArray<string> _traversalPath = TraversalPath.IsDefault
        ? []
        : TraversalPath;

    /// <summary>
    ///     Node IDs this item has passed through.
    /// </summary>
    public ImmutableArray<string> TraversalPath
    {
        get => _traversalPath;
        init => _traversalPath = value.IsDefault
            ? []
            : value;
    }

    /// <summary>
    ///     Collected lineage event information.
    /// </summary>
    public ImmutableArray<LineageRecord> LineageRecords
    {
        get => _lineageRecords;
        init => _lineageRecords = value.IsDefault
            ? []
            : value;
    }

    /// <summary>
    ///     Whether this item is selected for lineage collection (sampling). Defaults to true.
    /// </summary>
    public bool Collect { get; init; } = true;

    // Explicit interface implementation returns boxed Data for generic transparency
    object? ILineageEnvelope.Data => Data;
}

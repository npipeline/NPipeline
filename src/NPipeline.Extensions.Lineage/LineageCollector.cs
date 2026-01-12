using System.Collections.Concurrent;
using System.Collections.Immutable;
using NPipeline.Configuration;

namespace NPipeline.Lineage;

/// <summary>
///     Thread-safe collector for comprehensive data lineage tracking during pipeline execution.
/// </summary>
public sealed class LineageCollector : ILineageCollector
{
    private readonly ConcurrentDictionary<Guid, LineageTrail> _lineageTrails = new();
    private readonly Random _random = new();

    /// <summary>
    ///     Initializes a new instance of the <see cref="LineageCollector" /> class.
    /// </summary>
    public LineageCollector()
    {
    }

    /// <summary>
    ///     Creates a new lineage packet for a data item entering the pipeline.
    /// </summary>
    /// <typeparam name="T">The type of the data item.</typeparam>
    /// <param name="item">The data item.</param>
    /// <param name="sourceNodeId">The ID of the source node.</param>
    /// <returns>A lineage packet wrapping the item.</returns>
    public LineagePacket<T> CreateLineagePacket<T>(T item, string sourceNodeId)
    {
        ArgumentNullException.ThrowIfNull(sourceNodeId);

        var lineageId = Guid.NewGuid();
        var traversalPath = ImmutableList.Create(sourceNodeId);

        // Initialize the lineage trail
        _ = _lineageTrails.TryAdd(lineageId, new LineageTrail(lineageId, item, traversalPath));

        return new LineagePacket<T>(item, lineageId, traversalPath);
    }

    /// <summary>
    ///     Records a hop in the lineage trail for an item.
    /// </summary>
    /// <param name="lineageId">The unique ID of the item being tracked.</param>
    /// <param name="hop">The lineage hop to record.</param>
    public void RecordHop(Guid lineageId, LineageHop hop)
    {
        if (_lineageTrails.TryGetValue(lineageId, out var trail))
            trail.AddHop(hop);
    }

    /// <summary>
    ///     Determines if lineage should be collected for a given item based on sampling settings.
    /// </summary>
    /// <param name="lineageId">The unique ID of the item.</param>
    /// <param name="options">The lineage options containing sampling configuration.</param>
    /// <returns>True if lineage should be collected for this item.</returns>
    public bool ShouldCollectLineage(Guid lineageId, LineageOptions? options)
    {
        // If no options provided, default to collecting all lineage
        if (options == null)
            return true;

        // SampleEvery of 1 means collect all items
        if (options.SampleEvery <= 1)
            return true;

        // Use deterministic sampling if enabled
        if (options.DeterministicSampling)
        {
            // Hash the lineage ID and use modulo to determine if this item should be sampled
            var hash = lineageId.GetHashCode();
            return Math.Abs(hash) % options.SampleEvery == 0;
        }

        // Non-deterministic random sampling
        lock (_random)
        {
            return _random.Next(options.SampleEvery) == 0;
        }
    }

    /// <summary>
    ///     Gets the lineage information for a specific item.
    /// </summary>
    /// <param name="lineageId">The unique ID of the item.</param>
    /// <returns>The lineage information, or null if not found.</returns>
    public LineageInfo? GetLineageInfo(Guid lineageId)
    {
        return _lineageTrails.TryGetValue(lineageId, out var trail)
            ? trail.ToLineageInfo()
            : null;
    }

    /// <summary>
    ///     Gets all collected lineage information.
    /// </summary>
    /// <returns>A collection of all lineage information.</returns>
    public IReadOnlyList<LineageInfo> GetAllLineageInfo()
    {
        return [.. _lineageTrails.Values.Select(static trail => trail.ToLineageInfo())];
    }

    /// <summary>
    ///     Clears all collected lineage information.
    /// </summary>
    public void Clear()
    {
        _lineageTrails.Clear();
    }

    /// <summary>
    ///     Internal representation of a lineage trail for a single data item.
    /// </summary>
    private sealed class LineageTrail
    {
        private readonly object? _data;
        private readonly List<LineageHop> _hops = [];
        private readonly Guid _lineageId;
        private readonly object _lock = new();
        private readonly ImmutableList<string>.Builder _traversalPathBuilder;

        public LineageTrail(Guid lineageId, object? data, ImmutableList<string> initialPath)
        {
            _lineageId = lineageId;
            _data = data;
            _traversalPathBuilder = ImmutableList.CreateBuilder<string>();
            _traversalPathBuilder.AddRange(initialPath);
        }

        public void AddHop(LineageHop hop)
        {
            lock (_lock)
            {
                _hops.Add(hop);

                // Add the node ID to the traversal path if not already present
                if (!_traversalPathBuilder.Contains(hop.NodeId))
                    _traversalPathBuilder.Add(hop.NodeId);
            }
        }

        public LineageInfo ToLineageInfo()
        {
            lock (_lock)
            {
                return new LineageInfo(
                    _data,
                    _lineageId,
                    _traversalPathBuilder.ToImmutable(),
                    [.. _hops]);
            }
        }
    }
}

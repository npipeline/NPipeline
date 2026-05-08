using System.Collections.Concurrent;
using System.Collections.Immutable;
using NPipeline.Configuration;

namespace NPipeline.Lineage;

/// <summary>
///     Thread-safe collector for comprehensive data lineage tracking during pipeline execution.
/// </summary>
public sealed class LineageCollector : ILineageCollector
{
    private readonly ConcurrentDictionary<Guid, CorrelationTrail> _lineageTrails = new();
    private long _globalSequence;

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

        var correlationId = Guid.NewGuid();
        var traversalPath = ImmutableList.Create(sourceNodeId);

        _ = _lineageTrails.TryAdd(correlationId, new CorrelationTrail(correlationId, traversalPath));

        return new LineagePacket<T>(item, correlationId, traversalPath);
    }

    /// <summary>
    ///     Records an event for a lineage correlation.
    /// </summary>
    /// <param name="record">Event record.</param>
    public void Record(LineageRecord record)
    {
        var normalized = record.Normalize();

        var trail = _lineageTrails.GetOrAdd(
            normalized.CorrelationId,
            id => new CorrelationTrail(id, [.. normalized.TraversalPath]));

        var sequence = Interlocked.Increment(ref _globalSequence);
        trail.AddRecord(normalized, sequence);
    }

    /// <summary>
    ///     Determines if lineage should be collected for a given item based on sampling settings.
    /// </summary>
    /// <param name="correlationId">The unique ID of the item.</param>
    /// <param name="options">The lineage options containing sampling configuration.</param>
    /// <returns>True if lineage should be collected for this item.</returns>
    public bool ShouldCollectLineage(Guid correlationId, LineageOptions? options)
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
            // Hash the correlation ID and use modulo to determine if this item should be sampled
            var hash = correlationId.GetHashCode();
            return Math.Abs(hash) % options.SampleEvery == 0;
        }

        // Non-deterministic random sampling
        return Random.Shared.Next(options.SampleEvery) == 0;
    }

    /// <summary>
    ///     Gets event history for a specific correlation.
    /// </summary>
    /// <param name="correlationId">The unique ID of the item.</param>
    /// <returns>Ordered records for the correlation.</returns>
    public IReadOnlyList<LineageRecord> GetCorrelationHistory(Guid correlationId)
    {
        return _lineageTrails.TryGetValue(correlationId, out var trail)
            ? trail.GetOrderedRecords()
            : [];
    }

    /// <summary>
    ///     Gets final terminal reason for a correlation, when available.
    /// </summary>
    /// <param name="correlationId">The unique ID of the item.</param>
    /// <returns>Terminal reason or null when unresolved.</returns>
    public LineageOutcomeReason? GetTerminalReason(Guid correlationId)
    {
        return _lineageTrails.TryGetValue(correlationId, out var trail)
            ? trail.GetTerminalReason()
            : null;
    }

    /// <summary>
    ///     Gets all collected lineage records.
    /// </summary>
    /// <returns>All lineage records in deterministic order.</returns>
    public IReadOnlyList<LineageRecord> GetAllRecords()
    {
        return [.. _lineageTrails.Values
            .SelectMany(static trail => trail.GetOrderedEntries())
            .OrderBy(static entry => entry.Sequence)
            .Select(static entry => entry.Record)];
    }

    /// <summary>
    ///     Gets correlations that currently have no terminal record.
    /// </summary>
    /// <returns>Unresolved correlation ids.</returns>
    public IReadOnlyList<Guid> GetUnresolvedCorrelations()
    {
        return [.. _lineageTrails.Values
            .Where(static trail => !trail.HasTerminalRecord)
            .Select(static trail => trail.CorrelationId)
            .OrderBy(static id => id)];
    }

    /// <summary>
    ///     Clears all collected lineage information.
    /// </summary>
    public void Clear()
    {
        _lineageTrails.Clear();
    }

    /// <summary>
    ///     Internal representation of a correlation timeline.
    /// </summary>
    private sealed class CorrelationTrail
    {
        private readonly Guid _correlationId;
        private readonly List<LineageRecordEntry> _records = [];
        private readonly object _lock = new();
        private readonly ImmutableList<string>.Builder _traversalPathBuilder;
        private LineageOutcomeReason? _terminalReason;

        public CorrelationTrail(Guid correlationId, ImmutableList<string> initialPath)
        {
            _correlationId = correlationId;
            _traversalPathBuilder = ImmutableList.CreateBuilder<string>();
            _traversalPathBuilder.AddRange(initialPath);
        }

        public Guid CorrelationId => _correlationId;

        public bool HasTerminalRecord
        {
            get
            {
                lock (_lock)
                {
                    return _terminalReason is not null;
                }
            }
        }

        public void AddRecord(LineageRecord record, long sequence)
        {
            lock (_lock)
            {
                var normalizedPath = record.TraversalPath is ImmutableList<string> immutable
                    ? immutable
                    : [.. record.TraversalPath];

                _records.Add(new LineageRecordEntry(sequence, record with { TraversalPath = normalizedPath }));

                if (record.IsTerminal)
                    _terminalReason = record.OutcomeReason;

                foreach (var pathSegment in normalizedPath)
                {
                    if (!_traversalPathBuilder.Contains(pathSegment))
                        _traversalPathBuilder.Add(pathSegment);
                }
            }
        }

        public IReadOnlyList<LineageRecord> GetOrderedRecords()
        {
            lock (_lock)
            {
                return [.. _records
                    .OrderBy(static record => record.Sequence)
                    .Select(static record => record.Record)];
            }
        }

        public IReadOnlyList<LineageRecordEntry> GetOrderedEntries()
        {
            lock (_lock)
            {
                return [.. _records.OrderBy(static record => record.Sequence)];
            }
        }

        public LineageOutcomeReason? GetTerminalReason()
        {
            lock (_lock)
            {
                return _terminalReason;
            }
        }
    }

    private sealed record LineageRecordEntry(long Sequence, LineageRecord Record);
}

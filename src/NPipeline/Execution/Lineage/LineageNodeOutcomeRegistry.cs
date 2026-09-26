using System.Collections.Concurrent;
using NPipeline.Lineage;

namespace NPipeline.Execution.Lineage;

internal readonly record struct LineageItemOutcome(LineageOutcomeReason OutcomeReason, int RetryCount);

/// <summary>
///     The lineage of one input item of a transform: the correlation id of the packet it came in, and the contributor
///     indices of the hop that produced it.
/// </summary>
internal readonly record struct LineageInputMetadata(Guid CorrelationId, int[]? AncestryInputIndices);

/// <summary>
///     What an execution strategy or node reports about one input item, in the order its outputs are yielded.
/// </summary>
internal enum LineageProvenanceKind
{
    /// <summary>The item produced one output, the one being yielded, and nothing more.</summary>
    Output,

    /// <summary>The item produced an output, the one being yielded, and may produce more.</summary>
    PartialOutput,

    /// <summary>The item is finished. Its outputs, if any, were reported as <see cref="PartialOutput" />.</summary>
    Done,
}

/// <summary>
///     One provenance report: which input item an output came from, or that an input item is finished.
/// </summary>
/// <param name="InputIndex">The item's index in the node's input.</param>
/// <param name="Kind">What is reported.</param>
/// <param name="Outcome">
///     For <see cref="LineageProvenanceKind.Done" />, why the item ended: an outcome such as
///     <see cref="LineageOutcomeReason.FilteredOut" /> when it produced no output.
/// </param>
internal readonly record struct LineageProvenance(long InputIndex, LineageProvenanceKind Kind, LineageOutcomeReason Outcome = LineageOutcomeReason.Emitted);

/// <summary>
///     One transform's lineage state for a run, keyed by input index: the lineage of each input item, registered by the
///     lineage adapter as it reads the input, each item's outcome, recorded by the execution strategy, and, when the
///     strategy reports it, the provenance of each output.
/// </summary>
/// <remarks>
///     <para>
///         Everything is keyed by the item's index in the node's input, which each strategy knows and which a node
///         restart preserves: a replayed item has the same index, so it finds the same lineage.
///     </para>
///     <para>
///         A strategy that reports provenance adds a <see cref="LineageProvenance" /> for each output just before it
///         yields the output, so the lineage mapper, which reads the outputs in the order they are yielded, finds the
///         report for each output already queued. Items that end without an output are reported as
///         <see cref="LineageProvenanceKind.Done" />. The mapper removes an item's entries once it is finished, so the
///         state stays bounded by the items in flight.
///     </para>
/// </remarks>
internal sealed class LineageNodeState(bool reportsProvenance = false, ILineageSink? sink = null)
{
    public ConcurrentDictionary<long, LineageItemOutcome> Outcomes { get; } = new();

    public ConcurrentDictionary<long, LineageInputMetadata> Inputs { get; } = new();

    public ConcurrentQueue<LineageProvenance> Provenance { get; } = new();

    /// <summary>
    ///     Whether the node's execution strategy (or, for a stream transform, the node) reports the provenance of each
    ///     output. When it does not, the lineage mapper pairs outputs with inputs by position.
    /// </summary>
    public bool ReportsProvenance { get; } = reportsProvenance;

    /// <summary>
    ///     Where the lineage of an item that ends in this node without an output is recorded. Such an item has no packet
    ///     to carry its lineage to a sink node.
    /// </summary>
    public ILineageSink? Sink { get; } = sink;
}

/// <summary>
///     One run's lineage state, per transform node. Owned by the run's <see cref="Pipeline.PipelineContext" />: a fresh
///     registry per run, so nothing outlives the run or crosses into another.
/// </summary>
internal sealed class LineageNodeOutcomeRegistry
{
    private readonly ConcurrentDictionary<string, LineageNodeState> _nodes = new(StringComparer.Ordinal);

    /// <summary>
    ///     Starts a node's lineage state, replacing any left from an earlier attempt.
    /// </summary>
    public void BeginNode(string nodeId, bool reportsProvenance = false, ILineageSink? sink = null) =>
        _nodes[nodeId] = new LineageNodeState(reportsProvenance, sink);

    /// <summary>
    ///     Returns the node's lineage state, starting one without provenance if the node has none.
    /// </summary>
    public LineageNodeOutcomeWriter GetOrBeginNode(string nodeId) =>
        new(_nodes.GetOrAdd(nodeId, static _ => new LineageNodeState()));

    public void Record(string nodeId, long inputIndex, LineageOutcomeReason outcomeReason, int retryCount) =>
        RecordInto(_nodes.GetOrAdd(nodeId, static _ => new LineageNodeState()).Outcomes, inputIndex, outcomeReason, retryCount);

    /// <summary>
    ///     Resolves a writer bound to a single node's outcome store so the per-item hot path can record outcomes without
    ///     a dictionary lookup per item, which is multiplied by the degree of parallelism on parallel execution paths.
    ///     Returns an inactive writer when the node is not tracking lineage.
    /// </summary>
    public LineageNodeOutcomeWriter GetWriter(string nodeId) =>
        _nodes.TryGetValue(nodeId, out var node)
            ? new LineageNodeOutcomeWriter(node)
            : default;

    public bool IsTracking(string nodeId) => _nodes.ContainsKey(nodeId);

    /// <summary>
    ///     Releases a node's state once its output has been fully mapped.
    /// </summary>
    public void ClearNode(string nodeId) => _ = _nodes.TryRemove(nodeId, out _);

    /// <summary>
    ///     Releases every node's state when the run ends, including nodes whose output was never enumerated and so never
    ///     reached their own release.
    /// </summary>
    public void Clear() => _nodes.Clear();

    internal static void RecordInto(ConcurrentDictionary<long, LineageItemOutcome> nodeOutcomes, long inputIndex,
        LineageOutcomeReason outcomeReason, int retryCount)
    {
        var normalizedRetryCount = Math.Max(0, retryCount);

        _ = nodeOutcomes.AddOrUpdate(
            inputIndex,
            static (_, state) => new LineageItemOutcome(state.OutcomeReason, state.RetryCount),
            static (_, existing, state) => new LineageItemOutcome(
                // Error is recorded only by the item executor, just before it throws. The only later outcome for the
                // same index is a replay after a restart, and a replay that succeeds supersedes the failed run.
                existing.OutcomeReason == LineageOutcomeReason.Error && state.OutcomeReason != LineageOutcomeReason.Error
                    ? state.OutcomeReason
                    : MergeOutcome(existing.OutcomeReason, state.OutcomeReason),
                Math.Max(existing.RetryCount, state.RetryCount)),
            (OutcomeReason: outcomeReason, RetryCount: normalizedRetryCount));
    }

    private static LineageOutcomeReason MergeOutcome(LineageOutcomeReason current, LineageOutcomeReason candidate) =>
        Priority(candidate) >= Priority(current)
            ? candidate
            : current;

    private static int Priority(LineageOutcomeReason reason)
    {
        return reason switch
        {
            LineageOutcomeReason.DeadLettered => 700,
            LineageOutcomeReason.Error => 600,
            LineageOutcomeReason.FilteredOut => 500,
            LineageOutcomeReason.DroppedByBackpressure => 400,
            LineageOutcomeReason.Aggregated => 300,
            LineageOutcomeReason.Joined => 200,
            LineageOutcomeReason.ConsumedWithoutEmission => 150,
            _ => 100,
        };
    }
}

/// <summary>
///     A lightweight handle bound to a single node's lineage outcome store, resolved once per node
///     execution. Recording through this handle skips the per-item node lookup that
///     <see cref="LineageNodeOutcomeRegistry.Record" /> performs, which is significant on parallel hot
///     paths where the call is multiplied by the degree of parallelism.
/// </summary>
internal readonly struct LineageNodeOutcomeWriter
{
    private readonly LineageNodeState? _node;

    internal LineageNodeOutcomeWriter(LineageNodeState? node)
    {
        _node = node;
    }

    /// <summary>
    ///     Gets a value indicating whether this writer is bound to an active outcome store.
    /// </summary>
    public bool IsActive => _node is not null;

    /// <summary>
    ///     Records an outcome for the supplied input index. No-op when the writer is inactive.
    /// </summary>
    public void Record(long inputIndex, LineageOutcomeReason outcomeReason, int retryCount)
    {
        if (_node is null)
            return;

        LineageNodeOutcomeRegistry.RecordInto(_node.Outcomes, inputIndex, outcomeReason, retryCount);
    }

    /// <summary>
    ///     Gets the outcome recorded for the input item at <paramref name="inputIndex" />, if any.
    /// </summary>
    public bool TryGetOutcome(long inputIndex, out LineageItemOutcome outcome)
    {
        if (_node is not null && _node.Outcomes.TryGetValue(inputIndex, out outcome))
            return true;

        outcome = default;
        return false;
    }

    /// <summary>
    ///     Registers the lineage of the input item at <paramref name="inputIndex" />. No-op when the writer is inactive.
    /// </summary>
    public void RegisterInput(long inputIndex, Guid correlationId, int[]? ancestryInputIndices)
    {
        if (_node is not null)
            _node.Inputs[inputIndex] = new LineageInputMetadata(correlationId, ancestryInputIndices);
    }

    /// <summary>
    ///     Gets the lineage of the input item at <paramref name="inputIndex" />.
    /// </summary>
    public bool TryGetInput(long inputIndex, out LineageInputMetadata metadata)
    {
        if (_node is not null && _node.Inputs.TryGetValue(inputIndex, out metadata))
            return true;

        metadata = default;
        return false;
    }

    /// <summary>
    ///     Gets a value indicating whether the node reports the provenance of its outputs.
    /// </summary>
    public bool ReportsProvenance => _node?.ReportsProvenance == true;

    /// <summary>
    ///     How many provenance reports are queued and not yet taken. A node that reports provenance and is mapped by a
    ///     declared mapper never dequeues them, so this must stay at zero.
    /// </summary>
    internal int PendingProvenanceCount => _node?.Provenance.Count ?? 0;

    /// <summary>
    ///     Gets the sink for the lineage of items that end in this node without an output.
    /// </summary>
    public ILineageSink? Sink => _node?.Sink;

    /// <summary>
    ///     Reports that the output about to be yielded is the only output of the input item at
    ///     <paramref name="inputIndex" />. Call it just before the <c>yield return</c>. No-op unless the node reports
    ///     provenance.
    /// </summary>
    public void ReportOutput(long inputIndex)
    {
        if (_node is { ReportsProvenance: true })
            _node.Provenance.Enqueue(new LineageProvenance(inputIndex, LineageProvenanceKind.Output));
    }

    /// <summary>
    ///     Reports that the output about to be yielded is one of possibly several outputs of the input item at
    ///     <paramref name="inputIndex" />. Call it just before the <c>yield return</c>, and call
    ///     <see cref="ReportDone" /> once the item is finished. No-op unless the node reports provenance.
    /// </summary>
    public void ReportPartialOutput(long inputIndex)
    {
        if (_node is { ReportsProvenance: true })
            _node.Provenance.Enqueue(new LineageProvenance(inputIndex, LineageProvenanceKind.PartialOutput));
    }

    /// <summary>
    ///     Reports that the input item at <paramref name="inputIndex" /> is finished: it produced no output, for the
    ///     reason <paramref name="outcome" />, or it produced outputs reported by <see cref="ReportPartialOutput" />.
    ///     No-op unless the node reports provenance.
    /// </summary>
    public void ReportDone(long inputIndex, LineageOutcomeReason outcome)
    {
        if (_node is { ReportsProvenance: true })
            _node.Provenance.Enqueue(new LineageProvenance(inputIndex, LineageProvenanceKind.Done, outcome));
    }

    /// <summary>
    ///     Takes the next provenance report, in the order the reports were made.
    /// </summary>
    public bool TryTakeProvenance(out LineageProvenance provenance)
    {
        if (_node is not null && _node.Provenance.TryDequeue(out provenance))
            return true;

        provenance = default;
        return false;
    }

    /// <summary>
    ///     Forgets a finished input item's lineage and outcome, once its lineage has been mapped.
    /// </summary>
    public void Forget(long inputIndex)
    {
        if (_node is null)
            return;

        _ = _node.Inputs.TryRemove(inputIndex, out _);
        _ = _node.Outcomes.TryRemove(inputIndex, out _);
    }
}

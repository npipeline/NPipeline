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
///     One transform's lineage state for a run, keyed by input index: the lineage of each input item, registered by the
///     lineage adapter as it reads the input, and each item's outcome, recorded by the execution strategy.
/// </summary>
/// <remarks>
///     Everything is keyed by the item's index in the node's input, which each strategy knows and which a node restart
///     preserves: a replayed item has the same index, so it finds the same lineage.
/// </remarks>
internal sealed class LineageNodeState
{
    public ConcurrentDictionary<long, LineageItemOutcome> Outcomes { get; } = new();

    public ConcurrentDictionary<long, LineageInputMetadata> Inputs { get; } = new();
}

internal static class LineageNodeOutcomeRegistry
{
    private static readonly ConcurrentDictionary<(Guid PipelineId, string NodeId), LineageNodeState> Nodes = new();

    public static void BeginNode(Guid pipelineId, string nodeId)
    {
        Nodes[(pipelineId, nodeId)] = new LineageNodeState();
    }

    public static void Record(Guid pipelineId, string nodeId, long inputIndex, LineageOutcomeReason outcomeReason, int retryCount)
    {
        var node = Nodes.GetOrAdd((pipelineId, nodeId), static _ => new LineageNodeState());
        RecordInto(node.Outcomes, inputIndex, outcomeReason, retryCount);
    }

    /// <summary>
    ///     Resolves a writer bound to a single node's outcome store so the per-item hot path can record
    ///     outcomes without repeating the outer (pipeline, node) dictionary lookup. This lookup is otherwise
    ///     multiplied by the degree of parallelism on parallel execution paths. Returns an inactive writer
    ///     when the node is not currently tracking lineage.
    /// </summary>
    public static LineageNodeOutcomeWriter GetWriter(Guid pipelineId, string nodeId)
    {
        return Nodes.TryGetValue((pipelineId, nodeId), out var node)
            ? new LineageNodeOutcomeWriter(node)
            : default;
    }

    internal static void RecordInto(ConcurrentDictionary<long, LineageItemOutcome> nodeOutcomes, long inputIndex,
        LineageOutcomeReason outcomeReason, int retryCount)
    {
        var normalizedRetryCount = Math.Max(0, retryCount);

        _ = nodeOutcomes.AddOrUpdate(
            inputIndex,
            static (_, state) => new LineageItemOutcome(state.OutcomeReason, state.RetryCount),
            static (_, existing, state) => new LineageItemOutcome(
                MergeOutcome(existing.OutcomeReason, state.OutcomeReason),
                Math.Max(existing.RetryCount, state.RetryCount)),
            (OutcomeReason: outcomeReason, RetryCount: normalizedRetryCount));
    }

    private static LineageOutcomeReason MergeOutcome(LineageOutcomeReason current, LineageOutcomeReason candidate)
    {
        return Priority(candidate) >= Priority(current)
            ? candidate
            : current;
    }

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

    public static bool TryGet(Guid pipelineId, string nodeId, long inputIndex, out LineageItemOutcome outcome)
    {
        if (Nodes.TryGetValue((pipelineId, nodeId), out var node) && node.Outcomes.TryGetValue(inputIndex, out outcome))
            return true;

        outcome = default;
        return false;
    }

    public static bool IsTracking(Guid pipelineId, string nodeId)
    {
        return Nodes.ContainsKey((pipelineId, nodeId));
    }

    public static void ClearNode(Guid pipelineId, string nodeId)
    {
        _ = Nodes.TryRemove((pipelineId, nodeId), out _);
    }
}

/// <summary>
///     A lightweight handle bound to a single node's lineage outcome store, resolved once per node
///     execution. Recording through this handle skips the per-item outer (pipeline, node) lookup that
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
        {
            return;
        }

        LineageNodeOutcomeRegistry.RecordInto(_node.Outcomes, inputIndex, outcomeReason, retryCount);
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
}

using System.Collections.Concurrent;
using NPipeline.Lineage;

namespace NPipeline.Execution.Lineage;

internal readonly record struct LineageItemOutcome(HopDecisionFlags OutcomeFlags, int RetryCount);

internal static class LineageNodeOutcomeRegistry
{
    private static readonly ConcurrentDictionary<(Guid PipelineId, string NodeId), ConcurrentDictionary<long, LineageItemOutcome>> Outcomes = new();

    public static void BeginNode(Guid pipelineId, string nodeId)
    {
        Outcomes[(pipelineId, nodeId)] = new ConcurrentDictionary<long, LineageItemOutcome>();
    }

    public static void Record(Guid pipelineId, string nodeId, long inputIndex, HopDecisionFlags outcomeFlags, int retryCount)
    {
        var nodeOutcomes = Outcomes.GetOrAdd((pipelineId, nodeId), static _ => new ConcurrentDictionary<long, LineageItemOutcome>());
        var normalizedRetryCount = Math.Max(0, retryCount);

        _ = nodeOutcomes.AddOrUpdate(
            inputIndex,
            static (_, state) => new LineageItemOutcome(state.OutcomeFlags, state.RetryCount),
            static (_, existing, state) => new LineageItemOutcome(
                existing.OutcomeFlags | state.OutcomeFlags,
                Math.Max(existing.RetryCount, state.RetryCount)),
            (OutcomeFlags: outcomeFlags, RetryCount: normalizedRetryCount));
    }

    public static bool TryGet(Guid pipelineId, string nodeId, long inputIndex, out LineageItemOutcome outcome)
    {
        if (Outcomes.TryGetValue((pipelineId, nodeId), out var nodeOutcomes) && nodeOutcomes.TryGetValue(inputIndex, out outcome))
            return true;

        outcome = default;
        return false;
    }

    public static bool IsTracking(Guid pipelineId, string nodeId)
    {
        return Outcomes.ContainsKey((pipelineId, nodeId));
    }

    public static void ClearNode(Guid pipelineId, string nodeId)
    {
        _ = Outcomes.TryRemove((pipelineId, nodeId), out _);
    }
}

using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Helper extensions to retrieve <see cref="ParallelExecutionMetrics" /> captured for nodes using drop policies.
/// </summary>
public static class ParallelMetricsExtensions
{
    private static string Key(string nodeId)
    {
        return $"parallel.metrics::{nodeId}";
    }

    /// <summary>
    ///     Tries to get metrics for a node.
    /// </summary>
    public static bool TryGetParallelMetrics(this PipelineContext context, string nodeId, out ParallelExecutionMetrics metrics)
    {
        if (context.Items.TryGetValue(Key(nodeId), out var value) && value is ParallelExecutionMetrics m)
        {
            metrics = m;
            return true;
        }

        metrics = null!;
        return false;
    }

    /// <summary>
    ///     Gets metrics for a node or throws if not present (e.g., policy was Block or node not parallelized).
    /// </summary>
    public static ParallelExecutionMetrics GetParallelMetrics(this PipelineContext context, string nodeId)
    {
        if (TryGetParallelMetrics(context, nodeId, out var m))
            return m;

        throw new InvalidOperationException($"No parallel metrics found for node '{nodeId}'. Ensure a drop policy was configured and the node executed.");
    }
}

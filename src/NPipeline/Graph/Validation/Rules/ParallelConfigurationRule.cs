using System.Collections.Immutable;

namespace NPipeline.Graph.Validation.Rules;

/// <summary>
///     Validates that parallel execution nodes are properly configured.
///     Detects potentially problematic configurations that could cause unexpected behavior.
/// </summary>
/// <remarks>
///     <para>
///         This rule checks for:
///         <list type="number">
///             <item>Unbounded queue lengths with high parallelism (memory risk)</item>
///             <item>Order-preserving modes with high parallelism (latency/buffering risk)</item>
///             <item>Suspicious queue policy choices</item>
///         </list>
///     </para>
///     <para>
///         These are informational warnings that help users understand potential performance implications
///         of their parallelism configuration, especially in high-throughput scenarios.
///     </para>
/// </remarks>
internal sealed class ParallelConfigurationRule : IGraphRule
{
    /// <inheritdoc />
    public string Name => "ParallelConfiguration";

    /// <inheritdoc />
    public bool StopOnError => false;

    /// <inheritdoc />
    public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
    {
        var graph = context.Graph;
        var issues = ImmutableList.CreateBuilder<ValidationIssue>();

        // Check each node's execution options
        if (graph.ExecutionOptions.NodeExecutionAnnotations is null || graph.ExecutionOptions.NodeExecutionAnnotations.Count == 0)
            return issues.ToImmutable();

        var nodeMap = graph.Nodes.ToDictionary(n => n.Id);

        foreach (var (nodeId, annotation) in graph.ExecutionOptions.NodeExecutionAnnotations)
        {
            // Check if this is a ParallelOptions annotation by type name
            // (avoiding direct reference to avoid assembly dependency)
            if (annotation.GetType().Name != "ParallelOptions")
                continue;

            if (!nodeMap.TryGetValue(nodeId, out var node))
                continue;

            // Use reflection to safely access ParallelOptions properties
            var optionType = annotation.GetType();
            var maxDopProp = optionType.GetProperty("MaxDegreeOfParallelism");
            var maxQueueProp = optionType.GetProperty("MaxQueueLength");
            var preserveOrderProp = optionType.GetProperty("PreserveOrdering");
            var queuePolicyProp = optionType.GetProperty("QueuePolicy");

            if (maxDopProp is null || maxQueueProp is null || preserveOrderProp is null || queuePolicyProp is null)
                continue; // Not a ParallelOptions, skip

            var maxDop = (int?)maxDopProp.GetValue(annotation);
            var maxQueue = (int?)maxQueueProp.GetValue(annotation);
            var preserveOrder = (bool)preserveOrderProp.GetValue(annotation)!;
            var queuePolicy = queuePolicyProp.GetValue(annotation);

            // Warn about unbounded queue with high parallelism
            if (maxQueue is null && maxDop > 4)
            {
                issues.Add(new ValidationIssue(
                    ValidationSeverity.Warning,
                    $"Node '{node.Name}' has high parallelism ({maxDop}) " +
                    $"but no queue limit (MaxQueueLength is null). This could cause unbounded memory growth " +
                    $"if the upstream producer is faster than this node can process. " +
                    $"Consider setting MaxQueueLength to prevent memory issues.",
                    "Parallelism"));
            }

            // Warn about ordering with high parallelism
            if (preserveOrder && maxDop > 8)
            {
                issues.Add(new ValidationIssue(
                    ValidationSeverity.Warning,
                    $"Node '{node.Name}' preserves ordering with high parallelism ({maxDop}). " +
                    $"This may cause significant output buffering and latency as items must be reordered. " +
                    $"If ordering is not critical, consider .AllowUnorderedOutput() to improve throughput.",
                    "Parallelism"));
            }

            // Warn about drop policies without queue length
            if ((queuePolicy?.ToString() == "DropOldest" || queuePolicy?.ToString() == "DropNewest")
                && maxQueue is null)
            {
                issues.Add(new ValidationIssue(
                    ValidationSeverity.Warning,
                    $"Node '{node.Name}' uses drop queue policy ({queuePolicy}) " +
                    $"but MaxQueueLength is not set. The drop policy will have no effect without a bounded queue. " +
                    $"Either set MaxQueueLength or use BoundedQueuePolicy.Block (the default).",
                    "Parallelism"));
            }

            // Warn about very high parallelism (potential thread explosion)
            if (maxDop > Environment.ProcessorCount * 4)
            {
                issues.Add(new ValidationIssue(
                    ValidationSeverity.Warning,
                    $"Node '{node.Name}' has very high parallelism ({maxDop}), " +
                    $"which is {(double)maxDop / Environment.ProcessorCount:F1}x the processor count. " +
                    $"This may indicate an error in configuration or be intentional for I/O-bound workloads. " +
                    $"Verify this is the intended parallelism level.",
                    "Parallelism"));
            }
        }

        return issues.ToImmutable();
    }
}

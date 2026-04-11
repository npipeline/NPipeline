using NPipeline.Observability;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.Observability.Tests;

/// <summary>
///     Shared test helpers for observability integration tests.
/// </summary>
internal static class TestHelpers
{
    /// <summary>
    ///     Looks up node metrics by node ID without knowing the runtime pipeline ID.
    ///     Used in integration tests where the pipeline assigns its own <see cref="Guid" /> at execution time.
    ///     Performs a case-insensitive exact match first, then progressively wider fuzzy matches.
    /// </summary>
    internal static INodeMetrics? GetNodeMetricsById(IObservabilityCollector collector, string nodeId)
    {
        static string Normalize(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;

            return string.Concat(value.Where(char.IsLetterOrDigit)).ToLowerInvariant();
        }

        var allMetrics = collector.GetNodeMetrics();

        var direct = allMetrics.FirstOrDefault(m => string.Equals(m.NodeId, nodeId, StringComparison.OrdinalIgnoreCase));

        if (direct != null)
            return direct;

        var normalizedTarget = Normalize(nodeId);

        var normalizedExact = allMetrics.FirstOrDefault(m => Normalize(m.NodeId) == normalizedTarget);

        if (normalizedExact != null)
            return normalizedExact;

        var normalizedContains = allMetrics.FirstOrDefault(m =>
        {
            var normalizedCandidate = Normalize(m.NodeId);

            return normalizedCandidate.Contains(normalizedTarget, StringComparison.Ordinal) ||
                   normalizedTarget.Contains(normalizedCandidate, StringComparison.Ordinal);
        });

        if (normalizedContains != null)
            return normalizedContains;

        var roleToken = normalizedTarget.Contains("unbatch", StringComparison.Ordinal)
            ? "unbatch"
            : normalizedTarget.Contains("batch", StringComparison.Ordinal)
                ? "batch"
                : normalizedTarget.Contains("transform", StringComparison.Ordinal)
                    ? "transform"
                    : normalizedTarget.Contains("source", StringComparison.Ordinal)
                        ? "source"
                        : normalizedTarget.Contains("sink", StringComparison.Ordinal)
                            ? "sink"
                            : normalizedTarget;

        var roleMatch = allMetrics.FirstOrDefault(m => Normalize(m.NodeId).Contains(roleToken, StringComparison.Ordinal));

        if (roleMatch != null)
            return roleMatch;

        if (string.Equals(roleToken, "transform", StringComparison.Ordinal))
        {
            return allMetrics.FirstOrDefault(m =>
            {
                var normalizedCandidate = Normalize(m.NodeId);

                return !normalizedCandidate.Contains("source", StringComparison.Ordinal) &&
                       !normalizedCandidate.Contains("sink", StringComparison.Ordinal);
            });
        }

        return null;
    }
}

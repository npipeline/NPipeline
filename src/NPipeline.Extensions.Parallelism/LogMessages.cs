using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;

namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Source-generated logging methods for parallel execution strategy operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class ParallelExecutionStrategyLogMessages
{
    [LoggerMessage(1, LogLevel.Debug, "Node {NodeId}, Final MaxRetries: {MaxRetries}")]
    public static partial void FinalMaxRetries(ILogger logger, string nodeId, int maxRetries);

    [LoggerMessage(2, LogLevel.Warning, "Node {NodeId}, Failed to enqueue item {Item} after {MaxAttempts} drop attempts")]
    public static partial void EnqueueFailed(ILogger logger, string nodeId, string? item, int maxAttempts);
}

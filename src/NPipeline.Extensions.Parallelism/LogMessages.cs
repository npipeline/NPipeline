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

    [LoggerMessage(3, LogLevel.Debug, "Node {NodeId}, Found per-node retry options: MaxRetries={MaxRetries}")]
    public static partial void PerNodeRetryOptionsFound(ILogger logger, string nodeId, int maxRetries);

    [LoggerMessage(4, LogLevel.Debug, "Node {NodeId}, Using global retry options: MaxItemRetries={MaxRetries}")]
    public static partial void GlobalRetryOptionsUsed(ILogger logger, string nodeId, int maxRetries);

    [LoggerMessage(5, LogLevel.Debug, "Node {NodeId}, Using context retry options: MaxItemRetries={MaxRetries}")]
    public static partial void ContextRetryOptionsUsed(ILogger logger, string nodeId, int maxRetries);

    [LoggerMessage(6, LogLevel.Debug, "Node {NodeId} failed on attempt {Attempt}.")]
    public static partial void NodeFailure(ILogger logger, Exception exception, string nodeId, int attempt);
}

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Observability.Metrics;

namespace NPipeline.Observability;

/// <summary>
///     Sink that logs node metrics to an ILogger.
/// </summary>
public sealed class LoggingMetricsSink : IMetricsSink
{
    private readonly ILogger _logger;

    // LoggerMessage delegates for high-performance logging
    private static readonly Action<ILogger, string, long, long, long, double, double, Exception?> s_logSuccessWithAverage =
        LoggerMessage.Define<string, long, long, long, double, double>(
            LogLevel.Information,
            new EventId(1, nameof(LoggingMetricsSink)),
            "Node {NodeId} completed successfully. Processed {ItemsProcessed} items, emitted {ItemsEmitted} items in {DurationMs}ms. Throughput: {Throughput:F2} items/sec (Avg: {AverageMs:F2} ms/item)");

    private static readonly Action<ILogger, string, long, long, long, double, Exception?> s_logSuccessWithoutAverage =
        LoggerMessage.Define<string, long, long, long, double>(
            LogLevel.Information,
            new EventId(2, nameof(LoggingMetricsSink)),
            "Node {NodeId} completed successfully. Processed {ItemsProcessed} items, emitted {ItemsEmitted} items in {DurationMs}ms. Throughput: {Throughput:F2} items/sec");

    private static readonly Action<ILogger, string, long, string, Exception?> s_logFailure =
        LoggerMessage.Define<string, long, string>(
            LogLevel.Warning,
            new EventId(3, nameof(LoggingMetricsSink)),
            "Node {NodeId} failed. Processed {ItemsProcessed} items before failure. Exception: {ExceptionMessage}");

    private static readonly Action<ILogger, string, int, Exception?> s_logRetryCount =
        LoggerMessage.Define<string, int>(
            LogLevel.Information,
            new EventId(4, nameof(LoggingMetricsSink)),
            "Node {NodeId} required {RetryCount} retry attempts");

    private static readonly Action<ILogger, string, long, Exception?> s_logPeakMemory =
        LoggerMessage.Define<string, long>(
            LogLevel.Debug,
            new EventId(5, nameof(LoggingMetricsSink)),
            "Node {NodeId} peak memory usage: {PeakMemoryMb}MB");

    private static readonly Action<ILogger, string, long, Exception?> s_logProcessorTime =
        LoggerMessage.Define<string, long>(
            LogLevel.Debug,
            new EventId(6, nameof(LoggingMetricsSink)),
            "Node {NodeId} processor time: {ProcessorTimeMs}ms");

    private static readonly Action<ILogger, string, double, Exception?> s_logAverageItemTime =
        LoggerMessage.Define<string, double>(
            LogLevel.Debug,
            new EventId(7, nameof(LoggingMetricsSink)),
            "Node {NodeId} average item time: {AverageMs:F2} ms");

    /// <summary>
    ///     Initializes a new instance of the <see cref="LoggingMetricsSink" /> class.
    /// </summary>
    /// <param name="logger">The logger to write metrics to.</param>
    public LoggingMetricsSink(ILogger<LoggingMetricsSink>? logger = null)
    {
        _logger = logger ?? NullLogger<LoggingMetricsSink>.Instance;
    }

    /// <summary>
    ///     Asynchronously records node metrics.
    /// </summary>
    /// <param name="nodeMetrics">The node metrics to record.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(nodeMetrics);

        using (_logger.BeginScope(new Dictionary<string, object?>
        {
            ["NodeId"] = nodeMetrics.NodeId,
            ["Success"] = nodeMetrics.Success,
            ["ItemsProcessed"] = nodeMetrics.ItemsProcessed,
            ["ItemsEmitted"] = nodeMetrics.ItemsEmitted,
            ["DurationMs"] = nodeMetrics.DurationMs,
            ["RetryCount"] = nodeMetrics.RetryCount,
            ["ThreadId"] = nodeMetrics.ThreadId,
            ["AverageItemProcessingMs"] = nodeMetrics.AverageItemProcessingMs,
        }))
        {
            if (nodeMetrics.Success)
            {
                if (nodeMetrics.AverageItemProcessingMs.HasValue)
                {
                    s_logSuccessWithAverage(
                        _logger,
                        nodeMetrics.NodeId,
                        nodeMetrics.ItemsProcessed,
                        nodeMetrics.ItemsEmitted,
                        nodeMetrics.DurationMs ?? 0,
                        nodeMetrics.ThroughputItemsPerSec ?? 0,
                        nodeMetrics.AverageItemProcessingMs.Value,
                        null);
                }
                else
                {
                    s_logSuccessWithoutAverage(
                        _logger,
                        nodeMetrics.NodeId,
                        nodeMetrics.ItemsProcessed,
                        nodeMetrics.ItemsEmitted,
                        nodeMetrics.DurationMs ?? 0,
                        nodeMetrics.ThroughputItemsPerSec ?? 0,
                        null);
                }
            }
            else
            {
                s_logFailure(
                    _logger,
                    nodeMetrics.NodeId,
                    nodeMetrics.ItemsProcessed,
                    nodeMetrics.Exception?.Message ?? "Unknown error",
                    null);
            }

            if (nodeMetrics.RetryCount > 0)
            {
                s_logRetryCount(_logger, nodeMetrics.NodeId, nodeMetrics.RetryCount, null);
            }

            if (nodeMetrics.PeakMemoryUsageMb.HasValue)
            {
                s_logPeakMemory(_logger, nodeMetrics.NodeId, nodeMetrics.PeakMemoryUsageMb.Value, null);
            }

            if (nodeMetrics.ProcessorTimeMs.HasValue)
            {
                s_logProcessorTime(_logger, nodeMetrics.NodeId, nodeMetrics.ProcessorTimeMs.Value, null);
            }

            if (nodeMetrics.AverageItemProcessingMs.HasValue)
            {
                s_logAverageItemTime(_logger, nodeMetrics.NodeId, nodeMetrics.AverageItemProcessingMs.Value, null);
            }
        }

        return Task.CompletedTask;
    }
}

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

        var logLevel = nodeMetrics.Success ? LogLevel.Information : LogLevel.Warning;

        using (_logger.BeginScope(new Dictionary<string, object?>
        {
            ["NodeId"] = nodeMetrics.NodeId,
            ["Success"] = nodeMetrics.Success,
            ["ItemsProcessed"] = nodeMetrics.ItemsProcessed,
            ["ItemsEmitted"] = nodeMetrics.ItemsEmitted,
            ["DurationMs"] = nodeMetrics.DurationMs,
            ["RetryCount"] = nodeMetrics.RetryCount,
            ["ThreadId"] = nodeMetrics.ThreadId,
            ["AverageItemProcessingMs"] = nodeMetrics.AverageItemProcessingMs
        }))
        {
            if (nodeMetrics.Success)
            {
                if (nodeMetrics.AverageItemProcessingMs.HasValue)
                {
                    _logger.Log(
                        logLevel,
                        "Node {NodeId} completed successfully. Processed {ItemsProcessed} items, emitted {ItemsEmitted} items in {DurationMs}ms. Throughput: {Throughput:F2} items/sec (Avg: {AverageMs:F2} ms/item)",
                        nodeMetrics.NodeId,
                        nodeMetrics.ItemsProcessed,
                        nodeMetrics.ItemsEmitted,
                        nodeMetrics.DurationMs,
                        nodeMetrics.ThroughputItemsPerSec ?? 0,
                        nodeMetrics.AverageItemProcessingMs.Value);
                }
                else
                {
                    _logger.Log(
                        logLevel,
                        "Node {NodeId} completed successfully. Processed {ItemsProcessed} items, emitted {ItemsEmitted} items in {DurationMs}ms. Throughput: {Throughput:F2} items/sec",
                        nodeMetrics.NodeId,
                        nodeMetrics.ItemsProcessed,
                        nodeMetrics.ItemsEmitted,
                        nodeMetrics.DurationMs,
                        nodeMetrics.ThroughputItemsPerSec ?? 0);
                }
            }
            else
            {
                _logger.Log(
                    logLevel,
                    "Node {NodeId} failed. Processed {ItemsProcessed} items before failure. Exception: {ExceptionMessage}",
                    nodeMetrics.NodeId,
                    nodeMetrics.ItemsProcessed,
                    nodeMetrics.Exception?.Message ?? "Unknown error");
            }

            if (nodeMetrics.RetryCount > 0)
            {
                _logger.Log(
                    LogLevel.Information,
                    "Node {NodeId} required {RetryCount} retry attempts",
                    nodeMetrics.NodeId,
                    nodeMetrics.RetryCount);
            }

            if (nodeMetrics.PeakMemoryUsageMb.HasValue)
            {
                _logger.Log(
                    LogLevel.Debug,
                    "Node {NodeId} peak memory usage: {PeakMemoryMb}MB",
                    nodeMetrics.NodeId,
                    nodeMetrics.PeakMemoryUsageMb.Value);
            }

            if (nodeMetrics.ProcessorTimeMs.HasValue)
            {
                _logger.Log(
                    LogLevel.Debug,
                    "Node {NodeId} processor time: {ProcessorTimeMs}ms",
                    nodeMetrics.NodeId,
                    nodeMetrics.ProcessorTimeMs.Value);
            }

            if (nodeMetrics.AverageItemProcessingMs.HasValue)
            {
                _logger.Log(
                    LogLevel.Debug,
                    "Node {NodeId} average item time: {AverageMs:F2} ms",
                    nodeMetrics.NodeId,
                    nodeMetrics.AverageItemProcessingMs.Value);
            }
        }

        return Task.CompletedTask;
    }
}
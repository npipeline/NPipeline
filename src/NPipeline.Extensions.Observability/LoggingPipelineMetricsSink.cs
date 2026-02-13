using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Observability.Metrics;

namespace NPipeline.Observability;

/// <summary>
///     Sink that logs pipeline metrics to an ILogger.
/// </summary>
public sealed class LoggingPipelineMetricsSink : IPipelineMetricsSink
{
    private readonly ILogger _logger;

    // LoggerMessage delegates for high-performance logging - pipeline level
    private static readonly Action<ILogger, string, Guid, long, long, Exception?> s_logPipelineSuccess =
        LoggerMessage.Define<string, Guid, long, long>(
            LogLevel.Information,
            new EventId(1, nameof(LoggingPipelineMetricsSink)),
            "Pipeline {PipelineName} (RunId: {RunId}) completed successfully. Processed {TotalItemsProcessed} items in {DurationMs}ms");

    private static readonly Action<ILogger, string, Guid, long, string, Exception?> s_logPipelineFailure =
        LoggerMessage.Define<string, Guid, long, string>(
            LogLevel.Error,
            new EventId(2, nameof(LoggingPipelineMetricsSink)),
            "Pipeline {PipelineName} (RunId: {RunId}) failed. Processed {TotalItemsProcessed} items before failure. Exception: {ExceptionMessage}");

    private static readonly Action<ILogger, string, long, long, long, Exception?> s_logNodeSuccess =
        LoggerMessage.Define<string, long, long, long>(
            LogLevel.Information,
            new EventId(3, nameof(LoggingPipelineMetricsSink)),
            "  Node {NodeId}: Processed {ItemsProcessed} items, emitted {ItemsEmitted} items in {DurationMs}ms");

    private static readonly Action<ILogger, string, long, string, Exception?> s_logNodeFailure =
        LoggerMessage.Define<string, long, string>(
            LogLevel.Warning,
            new EventId(4, nameof(LoggingPipelineMetricsSink)),
            "  Node {NodeId}: Failed after processing {ItemsProcessed} items. Exception: {ExceptionMessage}");

    private static readonly Action<ILogger, string, int, Exception?> s_logNodeRetryCount =
        LoggerMessage.Define<string, int>(
            LogLevel.Information,
            new EventId(5, nameof(LoggingPipelineMetricsSink)),
            "    Node {NodeId} required {RetryCount} retry attempts");

    private static readonly Action<ILogger, string, double, Exception?> s_logNodeThroughput =
        LoggerMessage.Define<string, double>(
            LogLevel.Debug,
            new EventId(6, nameof(LoggingPipelineMetricsSink)),
            "    Node {NodeId} throughput: {Throughput:F2} items/sec");

    private static readonly Action<ILogger, string, double, Exception?> s_logNodeAverageTime =
        LoggerMessage.Define<string, double>(
            LogLevel.Debug,
            new EventId(7, nameof(LoggingPipelineMetricsSink)),
            "    Node {NodeId} average item time: {AverageMs:F2} ms");

    private static readonly Action<ILogger, double, Exception?> s_logOverallThroughput =
        LoggerMessage.Define<double>(
            LogLevel.Information,
            new EventId(8, nameof(LoggingPipelineMetricsSink)),
            "Overall pipeline throughput: {Throughput:F2} items/sec");

    /// <summary>
    ///     Initializes a new instance of the <see cref="LoggingPipelineMetricsSink" /> class.
    /// </summary>
    /// <param name="logger">The logger to write metrics to.</param>
    public LoggingPipelineMetricsSink(ILogger<LoggingPipelineMetricsSink>? logger = null)
    {
        _logger = logger ?? NullLogger<LoggingPipelineMetricsSink>.Instance;
    }

    /// <summary>
    ///     Asynchronously records pipeline metrics.
    /// </summary>
    /// <param name="pipelineMetrics">The pipeline metrics to record.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    public Task RecordAsync(IPipelineMetrics pipelineMetrics, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(pipelineMetrics);

        using (_logger.BeginScope(new Dictionary<string, object?>
        {
            ["PipelineName"] = pipelineMetrics.PipelineName,
            ["RunId"] = pipelineMetrics.RunId,
            ["Success"] = pipelineMetrics.Success,
            ["TotalItemsProcessed"] = pipelineMetrics.TotalItemsProcessed,
            ["DurationMs"] = pipelineMetrics.DurationMs,
        }))
        {
            if (pipelineMetrics.Success)
            {
                s_logPipelineSuccess(
                    _logger,
                    pipelineMetrics.PipelineName,
                    pipelineMetrics.RunId,
                    pipelineMetrics.TotalItemsProcessed,
                    pipelineMetrics.DurationMs ?? 0,
                    null);
            }
            else
            {
                s_logPipelineFailure(
                    _logger,
                    pipelineMetrics.PipelineName,
                    pipelineMetrics.RunId,
                    pipelineMetrics.TotalItemsProcessed,
                    pipelineMetrics.Exception?.Message ?? "Unknown error",
                    null);
            }

            // Log node-level metrics
            foreach (var nodeMetric in pipelineMetrics.NodeMetrics)
            {
                if (nodeMetric.Success)
                {
                    s_logNodeSuccess(
                        _logger,
                        nodeMetric.NodeId,
                        nodeMetric.ItemsProcessed,
                        nodeMetric.ItemsEmitted,
                        nodeMetric.DurationMs ?? 0,
                        null);
                }
                else
                {
                    s_logNodeFailure(
                        _logger,
                        nodeMetric.NodeId,
                        nodeMetric.ItemsProcessed,
                        nodeMetric.Exception?.Message ?? "Unknown error",
                        null);
                }

                if (nodeMetric.RetryCount > 0)
                {
                    s_logNodeRetryCount(_logger, nodeMetric.NodeId, nodeMetric.RetryCount, null);
                }

                if (nodeMetric.ThroughputItemsPerSec.HasValue)
                {
                    s_logNodeThroughput(_logger, nodeMetric.NodeId, nodeMetric.ThroughputItemsPerSec.Value, null);
                }

                if (nodeMetric.AverageItemProcessingMs.HasValue)
                {
                    s_logNodeAverageTime(_logger, nodeMetric.NodeId, nodeMetric.AverageItemProcessingMs.Value, null);
                }
            }

            // Calculate and log overall throughput
            if (pipelineMetrics.DurationMs.HasValue && pipelineMetrics.DurationMs.Value > 0)
            {
                var overallThroughput = pipelineMetrics.TotalItemsProcessed / (pipelineMetrics.DurationMs.Value / 1000.0);

                s_logOverallThroughput(_logger, overallThroughput, null);
            }
        }

        return Task.CompletedTask;
    }
}

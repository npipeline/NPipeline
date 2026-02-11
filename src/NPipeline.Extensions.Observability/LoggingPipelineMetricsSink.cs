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

        var logLevel = pipelineMetrics.Success
            ? LogLevel.Information
            : LogLevel.Error;

        // Early exit if logging is disabled for this level
        if (!_logger.IsEnabled(logLevel) && !_logger.IsEnabled(LogLevel.Debug))
        {
            return Task.CompletedTask;
        }

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
                if (_logger.IsEnabled(logLevel))
                {
                    _logger.Log(
                        logLevel,
                        "Pipeline {PipelineName} (RunId: {RunId}) completed successfully. Processed {TotalItemsProcessed} items in {DurationMs}ms",
                        pipelineMetrics.PipelineName,
                        pipelineMetrics.RunId,
                        pipelineMetrics.TotalItemsProcessed,
                        pipelineMetrics.DurationMs);
                }
            }
            else
            {
                if (_logger.IsEnabled(logLevel))
                {
                    _logger.Log(
                        logLevel,
                        "Pipeline {PipelineName} (RunId: {RunId}) failed. Processed {TotalItemsProcessed} items before failure. Exception: {ExceptionMessage}",
                        pipelineMetrics.PipelineName,
                        pipelineMetrics.RunId,
                        pipelineMetrics.TotalItemsProcessed,
                        pipelineMetrics.Exception?.Message ?? "Unknown error");
                }
            }

            // Log node-level metrics
            foreach (var nodeMetric in pipelineMetrics.NodeMetrics)
            {
                var nodeLogLevel = nodeMetric.Success
                    ? LogLevel.Information
                    : LogLevel.Warning;

                if (nodeMetric.Success)
                {
                    if (_logger.IsEnabled(nodeLogLevel))
                    {
                        _logger.Log(
                            nodeLogLevel,
                            "  Node {NodeId}: Processed {ItemsProcessed} items, emitted {ItemsEmitted} items in {DurationMs}ms",
                            nodeMetric.NodeId,
                            nodeMetric.ItemsProcessed,
                            nodeMetric.ItemsEmitted,
                            nodeMetric.DurationMs);
                    }
                }
                else
                {
                    if (_logger.IsEnabled(nodeLogLevel))
                    {
                        _logger.Log(
                            nodeLogLevel,
                            "  Node {NodeId}: Failed after processing {ItemsProcessed} items. Exception: {ExceptionMessage}",
                            nodeMetric.NodeId,
                            nodeMetric.ItemsProcessed,
                            nodeMetric.Exception?.Message ?? "Unknown error");
                    }
                }

                if (nodeMetric.RetryCount > 0 && _logger.IsEnabled(LogLevel.Information))
                {
                    _logger.Log(
                        LogLevel.Information,
                        "    Node {NodeId} required {RetryCount} retry attempts",
                        nodeMetric.NodeId,
                        nodeMetric.RetryCount);
                }

                if (nodeMetric.ThroughputItemsPerSec.HasValue && _logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Log(
                        LogLevel.Debug,
                        "    Node {NodeId} throughput: {Throughput:F2} items/sec",
                        nodeMetric.NodeId,
                        nodeMetric.ThroughputItemsPerSec.Value);
                }

                if (nodeMetric.AverageItemProcessingMs.HasValue && _logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.Log(
                        LogLevel.Debug,
                        "    Node {NodeId} average item time: {AverageMs:F2} ms",
                        nodeMetric.NodeId,
                        nodeMetric.AverageItemProcessingMs.Value);
                }
            }

            // Calculate and log overall throughput
            if (pipelineMetrics.DurationMs.HasValue && pipelineMetrics.DurationMs.Value > 0 && _logger.IsEnabled(LogLevel.Information))
            {
                var overallThroughput = pipelineMetrics.TotalItemsProcessed / (pipelineMetrics.DurationMs.Value / 1000.0);

                _logger.Log(
                    LogLevel.Information,
                    "Overall pipeline throughput: {Throughput:F2} items/sec",
                    overallThroughput);
            }
        }

        return Task.CompletedTask;
    }
}

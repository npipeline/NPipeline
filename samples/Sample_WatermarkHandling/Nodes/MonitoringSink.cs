using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_WatermarkHandling.Models;

namespace Sample_WatermarkHandling.Nodes;

/// <summary>
///     Sink node that provides comprehensive monitoring, metrics collection, and alerting.
///     This node demonstrates sophisticated monitoring capabilities for IoT manufacturing platforms.
/// </summary>
public class MonitoringSink : SinkNode<ProcessingStats>
{
    private readonly ConcurrentQueue<AlertEvent> _alerts = new();
    private readonly MonitoringConfiguration _config = new();
    private readonly ILogger<MonitoringSink> _logger;
    private readonly object _reportLock = new();
    private readonly ConcurrentQueue<ProcessingStats> _statsHistory = new();
    private DateTime _lastReportTime = DateTime.MinValue;

    /// <summary>
    ///     Initializes a new instance of the MonitoringSink class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public MonitoringSink(ILogger<MonitoringSink> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        InitializeMonitoringConfiguration();
    }

    /// <summary>
    ///     Processes processing statistics and provides comprehensive monitoring output.
    /// </summary>
    /// <param name="input">The input data pipe containing processing statistics.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the sink operation.</returns>
    public override async Task ExecuteAsync(
        IDataPipe<ProcessingStats> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting Monitoring Sink with comprehensive metrics and alerting");

        try
        {
            await foreach (var stats in input.WithCancellation(cancellationToken))
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                await ProcessStatisticsWithMonitoring(stats, cancellationToken);
            }

            // Generate final report
            await GenerateFinalReport(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Monitoring Sink cancelled");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in Monitoring Sink");
        }
    }

    /// <summary>
    ///     Initializes monitoring configuration.
    /// </summary>
    private void InitializeMonitoringConfiguration()
    {
        _config.ReportInterval = TimeSpan.FromSeconds(10);
        _config.EnableDetailedLogging = true;
        _config.EnableAlerting = true;
        _config.EnablePerformanceMetrics = true;
        _config.EnableResourceMonitoring = true;

        _config.AlertThresholds = new AlertThresholds
        {
            ErrorRateThreshold = 0.05, // 5% error rate
            LatencyThresholdMs = 1000, // 1 second latency
            ThroughputThresholdPerSecond = 10, // 10 events/second
            ResourceUtilizationThreshold = 0.8, // 80% resource utilization
        };
    }

    /// <summary>
    ///     Processes statistics with comprehensive monitoring.
    /// </summary>
    /// <param name="stats">The processing statistics to process.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task ProcessStatisticsWithMonitoring(
        ProcessingStats stats,
        CancellationToken cancellationToken)
    {
        // Store statistics in history
        _statsHistory.Enqueue(stats);

        // Check for alerts
        if (_config.EnableAlerting)
            await CheckForAlerts(stats, cancellationToken);

        // Generate periodic reports
        if (ShouldGenerateReport())
            await GenerateMonitoringReport(stats, cancellationToken);

        // Log detailed information if enabled
        if (_config.EnableDetailedLogging)
            await LogDetailedStatistics(stats, cancellationToken);
    }

    /// <summary>
    ///     Checks for alerts based on statistics.
    /// </summary>
    /// <param name="stats">The processing statistics.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task CheckForAlerts(
        ProcessingStats stats,
        CancellationToken cancellationToken)
    {
        var alerts = new List<AlertEvent>();

        // Check error rate
        if (stats.ErrorRate > _config.AlertThresholds.ErrorRateThreshold)
        {
            alerts.Add(new AlertEvent(
                AlertLevel.Critical,
                "High Error Rate",
                $"Error rate is {stats.ErrorRate:P2}, threshold is {_config.AlertThresholds.ErrorRateThreshold:P2}",
                DateTime.UtcNow));
        }

        // Check latency
        if (stats.AverageProcessingLatency.TotalMilliseconds > _config.AlertThresholds.LatencyThresholdMs)
        {
            alerts.Add(new AlertEvent(
                AlertLevel.Warning,
                "High Latency",
                $"Average latency is {stats.AverageProcessingLatency.TotalMilliseconds:F0}ms, threshold is {_config.AlertThresholds.LatencyThresholdMs}ms",
                DateTime.UtcNow));
        }

        // Check throughput
        if (stats.SystemThroughput < _config.AlertThresholds.ThroughputThresholdPerSecond)
        {
            alerts.Add(new AlertEvent(
                AlertLevel.Warning,
                "Low Throughput",
                $"Throughput is {stats.SystemThroughput:F1}/s, threshold is {_config.AlertThresholds.ThroughputThresholdPerSecond}/s",
                DateTime.UtcNow));
        }

        // Check resource utilization
        if (stats.ResourceUtilization.CpuUsage > _config.AlertThresholds.ResourceUtilizationThreshold ||
            stats.ResourceUtilization.MemoryUsage > _config.AlertThresholds.ResourceUtilizationThreshold)
        {
            alerts.Add(new AlertEvent(
                AlertLevel.Critical,
                "High Resource Utilization",
                $"CPU: {stats.ResourceUtilization.CpuUsage:P0}, Memory: {stats.ResourceUtilization.MemoryUsage:P0}, threshold: {_config.AlertThresholds.ResourceUtilizationThreshold:P0}",
                DateTime.UtcNow));
        }

        // Process alerts
        foreach (var alert in alerts)
        {
            _alerts.Enqueue(alert);
            await LogAlert(alert, cancellationToken);
        }
    }

    /// <summary>
    ///     Determines if a report should be generated.
    /// </summary>
    /// <returns>True if a report should be generated.</returns>
    private bool ShouldGenerateReport()
    {
        lock (_reportLock)
        {
            var timeSinceLastReport = DateTime.UtcNow - _lastReportTime;

            if (timeSinceLastReport >= _config.ReportInterval)
            {
                _lastReportTime = DateTime.UtcNow;
                return true;
            }

            return false;
        }
    }

    /// <summary>
    ///     Generates a monitoring report.
    /// </summary>
    /// <param name="stats">The current processing statistics.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task GenerateMonitoringReport(
        ProcessingStats stats,
        CancellationToken cancellationToken)
    {
        var report = GenerateReportContent(stats);

        _logger.LogInformation(
            "=== WATERMARK HANDLING MONITORING REPORT ===\n{Report}\n=== END REPORT ===",
            report);

        await Task.CompletedTask;
    }

    /// <summary>
    ///     Generates report content.
    /// </summary>
    /// <param name="stats">The processing statistics.</param>
    /// <returns>The formatted report content.</returns>
    private string GenerateReportContent(ProcessingStats stats)
    {
        var report = new StringBuilder();

        report.AppendLine($"Timestamp: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}");
        report.AppendLine($"Pipeline Status: {(stats.IsHealthy ? "HEALTHY" : "DEGRADED")}");
        report.AppendLine($"Performance: {(stats.IsOptimal ? "OPTIMAL" : "SUBOPTIMAL")}");
        report.AppendLine();

        report.AppendLine("=== PERFORMANCE METRICS ===");
        report.AppendLine($"Total Events: {stats.TotalEventsProcessed:N0}");
        report.AppendLine($"Throughput: {stats.SystemThroughput:F1} events/second");
        report.AppendLine($"Average Latency: {stats.AverageProcessingLatency.TotalMilliseconds:F0}ms");
        report.AppendLine($"Error Rate: {stats.ErrorRate:P2}");
        report.AppendLine($"Processing Duration: {stats.ProcessingDuration:hh\\:mm\\:ss}");
        report.AppendLine();

        if (_config.EnableResourceMonitoring)
        {
            report.AppendLine("=== RESOURCE UTILIZATION ===");
            report.AppendLine($"CPU: {stats.ResourceUtilization.CpuUsage:P0}");
            report.AppendLine($"Memory: {stats.ResourceUtilization.MemoryUsage:P0}");
            report.AppendLine($"Network: {stats.ResourceUtilization.NetworkUsage:P0}");
            report.AppendLine($"Disk: {stats.ResourceUtilization.DiskUsage:P0}");
            report.AppendLine();
        }

        report.AppendLine("=== DEVICE STATISTICS ===");

        foreach (var deviceStats in stats.DeviceStats.Values.Take(5)) // Show top 5 devices
        {
            report.AppendLine($"{deviceStats}");
        }

        report.AppendLine();

        report.AppendLine("=== WINDOW STATISTICS ===");
        report.AppendLine($"Total Windows: {stats.WindowStats.TotalWindows}");
        report.AppendLine($"Completed Windows: {stats.WindowStats.CompletedWindows}");
        report.AppendLine($"Completion Rate: {stats.WindowStats.CompletionRate:P0}");
        report.AppendLine($"Average Window Size: {stats.WindowStats.AverageWindowSize.TotalSeconds:F1}s");
        report.AppendLine();

        report.AppendLine("=== ALERT STATISTICS ===");
        report.AppendLine($"{stats.AlertStats}");

        return report.ToString();
    }

    /// <summary>
    ///     Logs detailed statistics.
    /// </summary>
    /// <param name="stats">The processing statistics.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task LogDetailedStatistics(
        ProcessingStats stats,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug(
            "Detailed Stats - Events: {Events}, Latency: {Latency}ms, Error Rate: {ErrorRate:P2}, Health: {Health}",
            stats.TotalEventsProcessed,
            stats.AverageProcessingLatency.TotalMilliseconds,
            stats.ErrorRate,
            stats.IsHealthy
                ? "Healthy"
                : "Degraded");

        await Task.CompletedTask;
    }

    /// <summary>
    ///     Logs an alert event.
    /// </summary>
    /// <param name="alert">The alert event.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task LogAlert(AlertEvent alert, CancellationToken cancellationToken)
    {
        var levelText = alert.Level switch
        {
            AlertLevel.Info => "INFO",
            AlertLevel.Warning => "WARNING",
            AlertLevel.Critical => "CRITICAL",
            _ => "UNKNOWN",
        };

        _logger.LogWarning(
            "ALERT [{Level}] {Title}: {Message}",
            levelText,
            alert.Title,
            alert.Message);

        await Task.CompletedTask;
    }

    /// <summary>
    ///     Generates a final report.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task GenerateFinalReport(CancellationToken cancellationToken)
    {
        var finalStats = _statsHistory.TryPeek(out var stats)
            ? stats
            : new ProcessingStats();

        _logger.LogInformation(
            "=== FINAL WATERMARK HANDLING REPORT ===\n{Report}\n=== END FINAL REPORT ===",
            GenerateReportContent(finalStats));

        await Task.CompletedTask;
    }
}

/// <summary>
///     Represents monitoring configuration.
/// </summary>
public class MonitoringConfiguration
{
    /// <summary>
    ///     Gets or sets the report interval.
    /// </summary>
    public TimeSpan ReportInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    ///     Gets or sets whether detailed logging is enabled.
    /// </summary>
    public bool EnableDetailedLogging { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether alerting is enabled.
    /// </summary>
    public bool EnableAlerting { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether performance metrics are enabled.
    /// </summary>
    public bool EnablePerformanceMetrics { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether resource monitoring is enabled.
    /// </summary>
    public bool EnableResourceMonitoring { get; set; } = true;

    /// <summary>
    ///     Gets or sets the alert thresholds.
    /// </summary>
    public AlertThresholds AlertThresholds { get; set; } = new();
}

/// <summary>
///     Represents alert thresholds.
/// </summary>
public class AlertThresholds
{
    /// <summary>
    ///     Gets or sets the error rate threshold.
    /// </summary>
    public double ErrorRateThreshold { get; set; } = 0.05;

    /// <summary>
    ///     Gets or sets the latency threshold in milliseconds.
    /// </summary>
    public double LatencyThresholdMs { get; set; } = 1000.0;

    /// <summary>
    ///     Gets or sets the throughput threshold per second.
    /// </summary>
    public double ThroughputThresholdPerSecond { get; set; } = 10.0;

    /// <summary>
    ///     Gets or sets the resource utilization threshold.
    /// </summary>
    public double ResourceUtilizationThreshold { get; set; } = 0.8;
}

/// <summary>
///     Represents an alert event.
/// </summary>
public class AlertEvent
{
    /// <summary>
    ///     Initializes a new instance of the AlertEvent class.
    /// </summary>
    /// <param name="level">The alert level.</param>
    /// <param name="title">The alert title.</param>
    /// <param name="message">The alert message.</param>
    /// <param name="timestamp">The alert timestamp.</param>
    public AlertEvent(AlertLevel level, string title, string message, DateTime timestamp)
    {
        Level = level;
        Title = title;
        Message = message;
        Timestamp = timestamp;
    }

    /// <summary>
    ///     Gets the alert level.
    /// </summary>
    public AlertLevel Level { get; }

    /// <summary>
    ///     Gets the alert title.
    /// </summary>
    public string Title { get; }

    /// <summary>
    ///     Gets the alert message.
    /// </summary>
    public string Message { get; }

    /// <summary>
    ///     Gets the alert timestamp.
    /// </summary>
    public DateTime Timestamp { get; }

    /// <summary>
    ///     Returns a string representation of the alert event.
    /// </summary>
    /// <returns>String representation of the alert event.</returns>
    public override string ToString()
    {
        return $"[{Level}] {Title}: {Message} at {Timestamp:HH:mm:ss}";
    }
}

/// <summary>
///     Represents alert levels.
/// </summary>
public enum AlertLevel
{
    /// <summary>
    ///     Information level alert.
    /// </summary>
    Info,

    /// <summary>
    ///     Warning level alert.
    /// </summary>
    Warning,

    /// <summary>
    ///     Critical level alert.
    /// </summary>
    Critical,
}

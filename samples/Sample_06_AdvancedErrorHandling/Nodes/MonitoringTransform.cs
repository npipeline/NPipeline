using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_06_AdvancedErrorHandling.Nodes;

/// <summary>
///     Transform node that tracks error rates and implements alerting on thresholds.
///     This node demonstrates how to monitor pipeline health and trigger alerts when error rates exceed thresholds.
/// </summary>
public class MonitoringTransform : TransformNode<SourceData, SourceData>
{
    private readonly List<string> _errorLog = new();
    private readonly DateTime _startTime = DateTime.UtcNow;
    private int _totalErrors;
    private int _totalProcessed;

    /// <summary>
    ///     Processes the input data while monitoring error rates and triggering alerts.
    /// </summary>
    /// <param name="item">The input SourceData to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the processed SourceData.</returns>
    public override async Task<SourceData> ExecuteAsync(SourceData item, PipelineContext context, CancellationToken cancellationToken)
    {
        _totalProcessed++;

        try
        {
            Console.WriteLine($"[MONITORING] Processing item: {item.Id} (Total: {_totalProcessed})");

            // Process the item with potential monitoring-related failures
            var result = await ProcessWithMonitoring(item, cancellationToken);

            // Check error rate and alert if needed
            CheckErrorRateAndAlert();

            return result;
        }
        catch (Exception ex)
        {
            _totalErrors++;
            var errorMessage = $"Error processing item {item.Id}: {ex.Message}";
            _errorLog.Add(errorMessage);

            Console.WriteLine($"[MONITORING] {errorMessage}");
            Console.WriteLine($"[MONITORING] Error rate: {CalculateErrorRate():P2} ({_totalErrors}/{_totalProcessed})");

            // Check error rate and alert if needed
            CheckErrorRateAndAlert();

            // Don't re-throw - let the item pass through to demonstrate other features
            return item with { Content = $"{item.Content} (monitoring-failed-but-continued)" };
        }
    }

    /// <summary>
    ///     Simulates processing with monitoring capabilities.
    /// </summary>
    /// <param name="item">The item to process.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The processed item.</returns>
    private async Task<SourceData> ProcessWithMonitoring(SourceData item, CancellationToken cancellationToken)
    {
        // Simulate processing delay
        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);

        // Simulate occasional monitoring-related failures (5% chance - reduced from 15%)
        var random = new Random(item.Id.GetHashCode());

        if (random.NextDouble() < 0.05)
            throw new InvalidOperationException($"Simulated monitoring failure for item {item.Id}");

        // Process the item
        return item with { Content = $"{item.Content} (monitoring-processed)" };
    }

    /// <summary>
    ///     Calculates the current error rate as a percentage.
    /// </summary>
    /// <returns>The error rate as a decimal between 0 and 1.</returns>
    private double CalculateErrorRate()
    {
        return _totalProcessed > 0
            ? (double)_totalErrors / _totalProcessed
            : 0;
    }

    /// <summary>
    ///     Checks error rate against thresholds and triggers alerts if needed.
    /// </summary>
    private void CheckErrorRateAndAlert()
    {
        var errorRate = CalculateErrorRate();
        var runtime = DateTime.UtcNow - _startTime;

        Console.WriteLine(
            $"[MONITORING] Current metrics: {_totalProcessed} processed, {_totalErrors} errors, {errorRate:P2} error rate, runtime: {runtime.TotalSeconds:F1}s");

        // Alert thresholds
        if (errorRate >= 0.5) // 50% error rate - critical
        {
            Console.WriteLine("🚨 [ALERT - CRITICAL] Error rate exceeded 50%! Immediate attention required!");
            LogAlert("CRITICAL", $"Error rate: {errorRate:P2}", _totalErrors, _totalProcessed);
        }
        else if (errorRate >= 0.3) // 30% error rate - warning
        {
            Console.WriteLine("⚠️ [ALERT - WARNING] Error rate exceeded 30%! Monitor closely.");
            LogAlert("WARNING", $"Error rate: {errorRate:P2}", _totalErrors, _totalProcessed);
        }
        else if (errorRate >= 0.1) // 10% error rate - info
        {
            Console.WriteLine("ℹ️ [ALERT - INFO] Error rate exceeded 10%. Normal operation monitoring.");
            LogAlert("INFO", $"Error rate: {errorRate:P2}", _totalErrors, _totalProcessed);
        }

        // Additional runtime-based alerts
        if (runtime.TotalMinutes > 1 && _totalProcessed < 10)
            Console.WriteLine("⏰ [ALERT - PERFORMANCE] Low throughput detected. Processing may be slow.");
    }

    /// <summary>
    ///     Logs alert information for later analysis.
    /// </summary>
    /// <param name="level">The alert level (CRITICAL, WARNING, INFO).</param>
    /// <param name="message">The alert message.</param>
    /// <param name="errorCount">The current error count.</param>
    /// <param name="processedCount">The current processed count.</param>
    private void LogAlert(string level, string message, int errorCount, int processedCount)
    {
        var alert = $"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}] {level}: {message} (Errors: {errorCount}, Processed: {processedCount})";
        Console.WriteLine($"[MONITORING] Alert logged: {alert}");

        // In a real implementation, this would send to a monitoring system
        // For demo purposes, we just log to console
    }

    /// <summary>
    ///     Outputs a summary of monitoring metrics.
    /// </summary>
    public void OutputMonitoringSummary()
    {
        var runtime = DateTime.UtcNow - _startTime;
        var errorRate = CalculateErrorRate();

        Console.WriteLine();
        Console.WriteLine("=== MONITORING SUMMARY ===");
        Console.WriteLine($"Total Processed: {_totalProcessed}");
        Console.WriteLine($"Total Errors: {_totalErrors}");
        Console.WriteLine($"Error Rate: {errorRate:P2}");
        Console.WriteLine($"Runtime: {runtime.TotalSeconds:F1} seconds");
        Console.WriteLine($"Throughput: {_totalProcessed / runtime.TotalSeconds:F2} items/second");

        if (_errorLog.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine("Recent Errors:");

            foreach (var error in _errorLog.TakeLast(5))
            {
                Console.WriteLine($"  - {error}");
            }
        }

        Console.WriteLine("===========================");
    }
}

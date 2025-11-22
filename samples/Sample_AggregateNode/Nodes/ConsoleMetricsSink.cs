using System.Globalization;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_AggregateNode.Models;

namespace Sample_AggregateNode.Nodes;

/// <summary>
///     Sink node that displays aggregated analytics metrics in a console dashboard format.
///     This node demonstrates how to consume and present aggregation results from multiple streams.
/// </summary>
public class ConsoleMetricsSink : SinkNode<object>
{
    private readonly TimeSpan _displayInterval = TimeSpan.FromSeconds(5);
    private DateTime _lastDisplayTime = DateTime.MinValue;
    private int _totalMetricsReceived;

    /// <summary>
    ///     Initializes a new instance of the ConsoleMetricsSink.
    /// </summary>
    public ConsoleMetricsSink()
    {
        Console.WriteLine("ConsoleMetricsSink: Initialized for displaying analytics dashboard");
        Console.WriteLine($"ConsoleMetricsSink: Will update dashboard every {_displayInterval.TotalSeconds} seconds");
    }

    /// <summary>
    ///     Processes and displays individual metrics as they arrive.
    ///     This method handles both EventCountMetrics and EventSumMetrics from different aggregation streams.
    /// </summary>
    /// <param name="input">The data pipe containing metric items (can be EventCountMetrics or EventSumMetrics).</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing display operation.</returns>
    public override async Task ExecuteAsync(
        IDataPipe<object> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine("ConsoleMetricsSink: Starting to display analytics dashboard...");
        Console.WriteLine();

        await foreach (var metric in input.WithCancellation(cancellationToken))
        {
            _totalMetricsReceived++;

            switch (metric)
            {
                case EventCountMetrics countMetrics:
                    await DisplayEventCountMetrics(countMetrics);
                    break;

                case EventSumMetrics sumMetrics:
                    await DisplayEventSumMetrics(sumMetrics);
                    break;

                default:
                    Console.WriteLine($"ConsoleMetricsSink: Received unknown metric type: {metric.GetType().Name}");
                    break;
            }

            // Display summary dashboard periodically
            if (DateTime.UtcNow - _lastDisplayTime >= _displayInterval)
            {
                await DisplayDashboardSummary();
                _lastDisplayTime = DateTime.UtcNow;
            }
        }

        // Final dashboard summary
        await DisplayDashboardSummary();
        Console.WriteLine("ConsoleMetricsSink: Dashboard display completed");
    }

    /// <summary>
    ///     Displays event count metrics in a formatted way.
    /// </summary>
    /// <param name="metrics">The event count metrics to display.</param>
    /// <returns>A task representing the display operation.</returns>
    private async Task DisplayEventCountMetrics(EventCountMetrics metrics)
    {
        await Task.CompletedTask;

        if (string.IsNullOrEmpty(metrics.EventType))
            return; // Skip empty metrics

        Console.WriteLine(
            $"[COUNT] {metrics.EventType.ToUpper(CultureInfo.InvariantCulture)}: {metrics.Count} events " +
            $"({metrics.EventsPerSecond.ToString("F2", CultureInfo.InvariantCulture)}/sec) " +
            $"Window: {metrics.WindowStart.ToString("HH:mm:ss", CultureInfo.InvariantCulture)} - {metrics.WindowEnd.ToString("HH:mm:ss", CultureInfo.InvariantCulture)}");
    }

    /// <summary>
    ///     Displays event sum metrics in a formatted way.
    /// </summary>
    /// <param name="metrics">The event sum metrics to display.</param>
    /// <returns>A task representing the display operation.</returns>
    private async Task DisplayEventSumMetrics(EventSumMetrics metrics)
    {
        await Task.CompletedTask;

        if (string.IsNullOrEmpty(metrics.Category))
            return; // Skip empty metrics

        Console.WriteLine(
            $"[SUM] {metrics.Category,-15} | Total: {metrics.TotalValue.ToString("C", CultureInfo.InvariantCulture),-10} | " +
            $"Count: {metrics.EventCount,-4} | Avg: {metrics.AverageValue.ToString("C", CultureInfo.InvariantCulture),-8} | " +
            $"Window: {metrics.WindowStart.ToString("HH:mm:ss", CultureInfo.InvariantCulture)} - {metrics.WindowEnd.ToString("HH:mm:ss", CultureInfo.InvariantCulture)}");
    }

    /// <summary>
    ///     Displays a comprehensive dashboard summary.
    /// </summary>
    /// <returns>A task representing the display operation.</returns>
    private async Task DisplayDashboardSummary()
    {
        await Task.CompletedTask;

        Console.WriteLine();
        Console.WriteLine("=== REAL-TIME ANALYTICS DASHBOARD ===");
        Console.WriteLine($"Last Updated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        Console.WriteLine($"Total Metrics Processed: {_totalMetricsReceived}");
        Console.WriteLine();

        // Display current time window information
        var now = DateTime.UtcNow;
        var currentMinute = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
        var nextMinute = currentMinute.AddMinutes(1);

        Console.WriteLine("Current Time Windows:");
        Console.WriteLine($"  Tumbling Window: {currentMinute:HH:mm:ss} - {nextMinute:HH:mm:ss}");
        Console.WriteLine("  Sliding Windows: Multiple 30s windows overlapping");
        Console.WriteLine();

        // Display legend
        Console.WriteLine("Legend:");
        Console.WriteLine("  [COUNT] - Event type counts (1-minute tumbling windows)");
        Console.WriteLine("  [SUM]   - Category value sums (30-second sliding windows)");
        Console.WriteLine();

        Console.WriteLine("==========================================");
        Console.WriteLine();
    }

    /// <summary>
    ///     Gets the current sink statistics.
    /// </summary>
    /// <returns>The total number of metrics received.</returns>
    public int GetStatistics()
    {
        Console.WriteLine($"ConsoleMetricsSink: Total metrics received: {_totalMetricsReceived}");
        return _totalMetricsReceived;
    }

    /// <summary>
    ///     Resets sink statistics.
    /// </summary>
    public void ResetStatistics()
    {
        _totalMetricsReceived = 0;
        _lastDisplayTime = DateTime.MinValue;
        Console.WriteLine("ConsoleMetricsSink: Statistics reset");
    }
}

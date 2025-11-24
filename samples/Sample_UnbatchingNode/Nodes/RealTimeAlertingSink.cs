using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_UnbatchingNode.Nodes;

/// <summary>
///     Sink node that processes individual alert events and handles real-time alerting.
///     This node demonstrates the output of unbatching - processing individual alert events.
/// </summary>
public class RealTimeAlertingSink : SinkNode<AlertEvent>
{
    private readonly int _maxAlertsPerSecond;
    private readonly Dictionary<string, int> _alertCounts;
    private readonly Dictionary<string, DateTime> _lastAlertTimes;
    private int _totalAlertsProcessed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="RealTimeAlertingSink" /> class.
    /// </summary>
    /// <param name="maxAlertsPerSecond">Maximum number of alerts to process per second to prevent alert fatigue.</param>
    public RealTimeAlertingSink(int maxAlertsPerSecond = 10)
    {
        _maxAlertsPerSecond = maxAlertsPerSecond;
        _alertCounts = new Dictionary<string, int>();
        _lastAlertTimes = new Dictionary<string, DateTime>();
        _totalAlertsProcessed = 0;
    }

    /// <summary>
    ///     Processes individual alert events and handles real-time alerting logic.
    /// </summary>
    /// <param name="input">A data pipe containing individual alert events.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A task representing the completion of the sink operation.</returns>
    public override async Task ExecuteAsync(
        IDataPipe<AlertEvent> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var alerts = new List<AlertEvent>();
        
        Console.WriteLine("Starting real-time alert processing...");

        // Process each alert event individually using the async enumerable
        await foreach (var alertObj in input.ToAsyncEnumerable(cancellationToken))
        {
            if (alertObj is AlertEvent alert)
            {
                alerts.Add(alert);
                await ProcessAlert(alert, stopwatch.Elapsed);
            }
        }

        stopwatch.Stop();
        
        // Print summary statistics
        Console.WriteLine();
        Console.WriteLine("=== REAL-TIME ALERTING SUMMARY ===");
        Console.WriteLine($"Total alerts processed: {_totalAlertsProcessed}");
        Console.WriteLine($"Processing time: {stopwatch.ElapsedMilliseconds}ms");
        Console.WriteLine($"Average time per alert: {(_totalAlertsProcessed > 0 ? (double)stopwatch.ElapsedMilliseconds / _totalAlertsProcessed : 0):F2}ms");
        
        // Alert distribution by type
        var alertTypeCounts = alerts.GroupBy(a => a.AlertType)
            .ToDictionary(g => g.Key, g => g.Count());
        
        Console.WriteLine("Alert distribution by type:");
        foreach (var kvp in alertTypeCounts.OrderBy(x => x.Key))
        {
            Console.WriteLine($"  {kvp.Key}: {kvp.Value}");
        }
        
        // Alert distribution by severity
        var severityCounts = alerts.GroupBy(a => a.Severity)
            .ToDictionary(g => g.Key, g => g.Count());
        
        Console.WriteLine("Alert distribution by severity:");
        foreach (var kvp in severityCounts.OrderByDescending(x => x.Key))
        {
            Console.WriteLine($"  {kvp.Key}: {kvp.Value}");
        }
        
        // Alert distribution by symbol
        var symbolCounts = alerts.GroupBy(a => a.Symbol)
            .ToDictionary(g => g.Key, g => g.Count());
        
        Console.WriteLine("Alert distribution by symbol:");
        foreach (var kvp in symbolCounts.OrderByDescending(x => x.Value))
        {
            Console.WriteLine($"  {kvp.Key}: {kvp.Value}");
        }
    }

    private async Task ProcessAlert(AlertEvent alert, TimeSpan processingTime)
    {
        // Rate limiting to prevent alert fatigue
        if (!ShouldProcessAlert(alert))
        {
            Console.WriteLine($"[RATE LIMITED] {alert.AlertType} alert for {alert.Symbol} - {alert.Message}");
            return;
        }

        _totalAlertsProcessed++;
        
        // Update tracking
        if (!_alertCounts.ContainsKey(alert.AlertType))
            _alertCounts[alert.AlertType] = 0;
        _alertCounts[alert.AlertType]++;
        
        _lastAlertTimes[alert.AlertType] = DateTime.UtcNow;

        // Simulate processing delay for high-priority alerts
        if (alert.RequiresAction)
        {
            await Task.Delay(10); // Simulate immediate action processing
        }

        // Format the alert output
        var severityIcon = alert.Severity switch
        {
            "Critical" => "🚨",
            "High" => "⚠️",
            "Medium" => "⚡",
            "Low" => "ℹ️",
            _ => "📊"
        };

        Console.WriteLine($"{severityIcon} [{alert.Severity}] {alert.AlertType} | {alert.Symbol} | " +
                         $"Value: {alert.TriggerValue:F2} (Threshold: {alert.Threshold:F2}) | " +
                         $"{alert.Message} | Event: {alert.OriginalEventId[..8]}...");

        // Simulate sending to external monitoring systems
        if (alert.RequiresAction)
        {
            Console.WriteLine($"   → Immediate action required for {alert.Symbol} - {alert.AlertType}");
        }
    }

    private bool ShouldProcessAlert(AlertEvent alert)
    {
        var now = DateTime.UtcNow;
        
        // Check if we've processed this type of alert recently
        if (_lastAlertTimes.TryGetValue(alert.AlertType, out var lastTime))
        {
            var timeSinceLastAlert = now - lastTime;
            if (timeSinceLastAlert.TotalSeconds < (1.0 / _maxAlertsPerSecond))
            {
                return false; // Rate limited
            }
        }

        // Always process critical alerts
        if (alert.Severity == "Critical")
            return true;

        return true;
    }
}
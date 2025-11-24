using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_WatermarkHandling.Models;

namespace Sample_WatermarkHandling.Nodes;

/// <summary>
///     Transform node that performs time-windowed aggregation with watermark-based window advancement.
///     This node demonstrates sophisticated event-time windowing and aggregation strategies.
/// </summary>
public class TimeWindowedAggregator : TransformNode<SensorReading, ProcessingStats>
{
    private readonly ConcurrentQueue<WindowResult> _completedWindows = new();
    private readonly ILogger<TimeWindowedAggregator> _logger;
    private readonly ProcessingStats _processingStats = new();
    private readonly object _statsLock = new();
    private readonly WindowConfiguration _windowConfig = new();
    private readonly ConcurrentDictionary<string, List<SensorReading>> _windowedData = new();
    private readonly object _windowLock = new();
    private DateTimeOffset _currentWatermark = DateTimeOffset.MinValue;

    /// <summary>
    ///     Initializes a new instance of the TimeWindowedAggregator class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public TimeWindowedAggregator(ILogger<TimeWindowedAggregator> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        InitializeWindowConfiguration();
    }

    /// <summary>
    ///     Processes sensor reading and performs time-windowed aggregation.
    /// </summary>
    /// <param name="reading">The input sensor reading.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The processing statistics.</returns>
    public override async Task<ProcessingStats> ExecuteAsync(
        SensorReading reading,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug("Processing reading from {DeviceId} for time windowed aggregation",
            reading.DeviceId);

        // Update current watermark from reading
        UpdateCurrentWatermark(reading);

        // Add reading to appropriate window
        AddReadingToWindow(reading);

        // Check for windows that should be completed based on watermark
        await CompleteWindowsBasedOnWatermark(cancellationToken);

        // Update processing statistics
        UpdateProcessingStatistics(reading);

        // Return current processing stats
        lock (_statsLock)
        {
            return _processingStats;
        }
    }

    /// <summary>
    ///     Initializes the window configuration.
    /// </summary>
    private void InitializeWindowConfiguration()
    {
        _windowConfig.WindowSize = TimeSpan.FromSeconds(5); // 5-second windows
        _windowConfig.AllowedLateness = TimeSpan.FromSeconds(1); // Allow 1 second lateness
        _windowConfig.MinimumEventsPerWindow = 1; // At least 1 event to trigger window
        _windowConfig.WindowSlide = TimeSpan.FromSeconds(1); // 1-second slide
        _windowConfig.AggregationStrategy = AggregationStrategy.Average;
    }

    /// <summary>
    ///     Processes a sensor reading with time windowing.
    /// </summary>
    /// <param name="reading">The input sensor reading.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task ProcessReadingWithWindowing(
        SensorReading reading,
        CancellationToken cancellationToken)
    {
        // Update current watermark from reading
        UpdateCurrentWatermark(reading);

        // Add reading to appropriate window
        AddReadingToWindow(reading);

        // Check for windows that should be completed based on watermark
        await CompleteWindowsBasedOnWatermark(cancellationToken);

        // Update processing statistics
        UpdateProcessingStatistics(reading);
    }

    /// <summary>
    ///     Updates current watermark.
    /// </summary>
    /// <param name="reading">The sensor reading.</param>
    private void UpdateCurrentWatermark(SensorReading reading)
    {
        if (reading.Timestamp > _currentWatermark)
            _currentWatermark = reading.Timestamp;
    }

    /// <summary>
    ///     Adds a reading to the appropriate time window.
    /// </summary>
    /// <param name="reading">The sensor reading to add.</param>
    private void AddReadingToWindow(SensorReading reading)
    {
        var windowKey = CalculateWindowKey(reading.Timestamp);

        _windowedData.AddOrUpdate(
            windowKey,
            new List<SensorReading> { reading },
            (_, existing) =>
            {
                existing.Add(reading);
                return existing;
            });
    }

    /// <summary>
    ///     Calculates the window key for a given timestamp.
    /// </summary>
    /// <param name="timestamp">The timestamp to calculate the window key for.</param>
    /// <returns>The window key.</returns>
    private string CalculateWindowKey(DateTimeOffset timestamp)
    {
        var windowStart = timestamp.AddTicks(
            -(timestamp.Ticks % _windowConfig.WindowSize.Ticks));

        return $"window_{windowStart:yyyyMMddHHmmss}";
    }

    /// <summary>
    ///     Completes windows based on the current watermark.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task CompleteWindowsBasedOnWatermark(CancellationToken cancellationToken)
    {
        var windowsToComplete = new List<string>();

        lock (_windowLock)
        {
            foreach (var kvp in _windowedData)
            {
                var windowKey = kvp.Key;
                var windowStartTime = ExtractWindowStartTime(windowKey);

                // Complete window if watermark has advanced beyond window end + allowed lateness
                var windowEndTime = windowStartTime.Add(_windowConfig.WindowSize).Add(_windowConfig.AllowedLateness);

                if (_currentWatermark >= windowEndTime && kvp.Value.Count >= _windowConfig.MinimumEventsPerWindow)
                    windowsToComplete.Add(windowKey);
            }
        }

        // Process completed windows
        foreach (var windowKey in windowsToComplete)
        {
            await ProcessWindow(windowKey, cancellationToken);
        }
    }

    /// <summary>
    ///     Extracts the window start time from a window key.
    /// </summary>
    /// <param name="windowKey">The window key.</param>
    /// <returns>The window start time.</returns>
    private static DateTimeOffset ExtractWindowStartTime(string windowKey)
    {
        var parts = windowKey.Split('_');

        if (parts.Length > 1 && DateTimeOffset.TryParse(parts[1], null, out var result))
            return result;

        return DateTimeOffset.MinValue;
    }

    /// <summary>
    ///     Processes a specific time window.
    /// </summary>
    /// <param name="windowKey">The window key to process.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task ProcessWindow(string windowKey, CancellationToken cancellationToken)
    {
        if (!_windowedData.TryRemove(windowKey, out var readings))
            return;

        if (readings.Count == 0)
            return;

        var windowStartTime = ExtractWindowStartTime(windowKey);
        var windowEndTime = windowStartTime.Add(_windowConfig.WindowSize);

        // Calculate window results based on aggregation strategy
        var windowResult = CalculateWindowResult(readings, windowStartTime, windowEndTime);

        // Store completed window
        _completedWindows.Enqueue(windowResult);

        // Log window completion
        _logger.LogInformation(
            "Completed window {WindowKey}: {EventCount} events, Duration: {Duration}s, Avg Value: {AvgValue:F2}",
            windowKey,
            readings.Count,
            _windowConfig.WindowSize.TotalSeconds,
            windowResult.AverageValue);

        await Task.CompletedTask;
    }

    /// <summary>
    ///     Calculates window results based on the aggregation strategy.
    /// </summary>
    /// <param name="readings">The readings in the window.</param>
    /// <param name="windowStart">The window start time.</param>
    /// <param name="windowEnd">The window end time.</param>
    /// <returns>The window result.</returns>
    private WindowResult CalculateWindowResult(
        List<SensorReading> readings,
        DateTimeOffset windowStart,
        DateTimeOffset windowEnd)
    {
        var values = readings.Select(r => r.Value).ToList();

        var averageValue = values.Count > 0
            ? values.Average()
            : 0.0;

        var minValue = values.Count > 0
            ? values.Min()
            : 0.0;

        var maxValue = values.Count > 0
            ? values.Max()
            : 0.0;

        var sumValue = values.Sum();

        return new WindowResult
        {
            WindowKey = CalculateWindowKey(windowStart),
            WindowStart = new DateTime(windowStart.Ticks, DateTimeKind.Utc),
            WindowEnd = new DateTime(windowEnd.Ticks, DateTimeKind.Utc),
            EventCount = readings.Count,
            AverageValue = averageValue,
            MinValue = minValue,
            MaxValue = maxValue,
            SumValue = sumValue,
            ReadingTypes = readings.Select(r => r.ReadingType.ToString()).Distinct().ToList(),
            DeviceIds = readings.Select(r => r.DeviceId).Distinct().ToList(),
        };
    }

    /// <summary>
    ///     Processes all remaining windows.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task ProcessAllWindows(CancellationToken cancellationToken)
    {
        var windowKeys = _windowedData.Keys.ToList();

        foreach (var windowKey in windowKeys)
        {
            await ProcessWindow(windowKey, cancellationToken);
        }
    }

    /// <summary>
    ///     Updates processing statistics.
    /// </summary>
    /// <param name="reading">The processed reading.</param>
    private void UpdateProcessingStatistics(SensorReading reading)
    {
        _processingStats.TotalEventsProcessed++;

        // Update device-specific statistics
        if (!_processingStats.DeviceStats.TryGetValue(reading.DeviceId, out var deviceStats))
        {
            deviceStats = new DeviceProcessingStats(reading.DeviceId);
            _processingStats.DeviceStats[reading.DeviceId] = deviceStats;
        }

        deviceStats.EventsProcessed++;
        deviceStats.LastEventTime = reading.Timestamp.UtcDateTime;

        // Update window statistics
        _processingStats.WindowStats.TotalWindows = _completedWindows.Count;
        _processingStats.WindowStats.CompletedWindows = _completedWindows.Count;

        if (_completedWindows.TryPeek(out var latestWindow))
            _processingStats.WindowStats.AverageWindowSize = latestWindow.WindowEnd - latestWindow.WindowStart;

        // Update resource utilization (simulated)
        _processingStats.ResourceUtilization.CpuUsage = Math.Min(0.8, _processingStats.TotalEventsProcessed / 1000.0);
        _processingStats.ResourceUtilization.MemoryUsage = Math.Min(0.7, _windowedData.Count / 100.0);
        _processingStats.ResourceUtilization.NetworkUsage = 0.6; // Simulated network usage
        _processingStats.ResourceUtilization.DiskUsage = 0.3; // Simulated disk usage

        // Update timing
        _processingStats.AverageProcessingLatency = TimeSpan.FromMilliseconds(50); // Simulated

        _processingStats.SystemThroughput = _processingStats.TotalEventsProcessed /
                                            Math.Max(1, (DateTimeOffset.UtcNow - _processingStats.ProcessingStartTime).TotalSeconds);

        _processingStats.LastUpdated = DateTimeOffset.UtcNow.UtcDateTime;
    }
}

/// <summary>
///     Represents window configuration for time-windowed aggregation.
/// </summary>
public class WindowConfiguration
{
    /// <summary>
    ///     Gets or sets the window size.
    /// </summary>
    public TimeSpan WindowSize { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     Gets or sets the allowed lateness for windows.
    /// </summary>
    public TimeSpan AllowedLateness { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets or sets the minimum events per window.
    /// </summary>
    public int MinimumEventsPerWindow { get; set; } = 1;

    /// <summary>
    ///     Gets or sets the window slide duration.
    /// </summary>
    public TimeSpan WindowSlide { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets or sets the aggregation strategy.
    /// </summary>
    public AggregationStrategy AggregationStrategy { get; set; } = AggregationStrategy.Average;
}

/// <summary>
///     Represents aggregation strategies for time windows.
/// </summary>
public enum AggregationStrategy
{
    /// <summary>
    ///     Calculate average values.
    /// </summary>
    Average,

    /// <summary>
    ///     Calculate sum values.
    /// </summary>
    Sum,

    /// <summary>
    ///     Find minimum and maximum values.
    /// </summary>
    MinMax,

    /// <summary>
    ///     Count events only.
    /// </summary>
    Count,

    /// <summary>
    ///     Calculate median values.
    /// </summary>
    Median,
}

/// <summary>
///     Represents the result of a time window processing.
/// </summary>
public class WindowResult
{
    /// <summary>
    ///     Gets or sets the window key.
    /// </summary>
    public string WindowKey { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the window start time.
    /// </summary>
    public DateTime WindowStart { get; set; }

    /// <summary>
    ///     Gets or sets the window end time.
    /// </summary>
    public DateTime WindowEnd { get; set; }

    /// <summary>
    ///     Gets or sets the number of events in the window.
    /// </summary>
    public int EventCount { get; set; }

    /// <summary>
    ///     Gets or sets the average value in the window.
    /// </summary>
    public double AverageValue { get; set; }

    /// <summary>
    ///     Gets or sets the minimum value in the window.
    /// </summary>
    public double MinValue { get; set; }

    /// <summary>
    ///     Gets or sets the maximum value in the window.
    /// </summary>
    public double MaxValue { get; set; }

    /// <summary>
    ///     Gets or sets the sum of values in the window.
    /// </summary>
    public double SumValue { get; set; }

    /// <summary>
    ///     Gets or sets the reading types in the window.
    /// </summary>
    public List<string> ReadingTypes { get; set; } = new();

    /// <summary>
    ///     Gets or sets the device IDs in the window.
    /// </summary>
    public List<string> DeviceIds { get; set; } = new();
}

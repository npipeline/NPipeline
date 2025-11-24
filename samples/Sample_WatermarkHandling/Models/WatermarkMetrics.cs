namespace Sample_WatermarkHandling.Models;

/// <summary>
///     Represents watermark progress tracking and performance metrics for the IoT manufacturing platform.
///     This class provides comprehensive monitoring of watermark behavior and system performance.
/// </summary>
public class WatermarkMetrics
{
    /// <summary>
    ///     Initializes a new instance of the WatermarkMetrics class.
    /// </summary>
    public WatermarkMetrics()
    {
        CurrentWatermark = DateTimeOffset.MinValue;
        ProcessingDelay = TimeSpan.Zero;
        LateDataCount = 0;
        WatermarkAdvanceRate = 0.0;
        SystemLoad = 0.0;
        NetworkCondition = NetworkCondition.Good;
        LastUpdated = DateTimeOffset.UtcNow;
    }

    /// <summary>
    ///     Gets or sets the current watermark timestamp.
    /// </summary>
    public DateTimeOffset CurrentWatermark { get; set; }

    /// <summary>
    ///     Gets or sets the delay between event time and processing time.
    /// </summary>
    public TimeSpan ProcessingDelay { get; set; }

    /// <summary>
    ///     Gets or sets the count of late data events.
    /// </summary>
    public long LateDataCount { get; set; }

    /// <summary>
    ///     Gets or sets the rate of watermark advancement (events per second).
    /// </summary>
    public double WatermarkAdvanceRate { get; set; }

    /// <summary>
    ///     Gets or sets the current system load (0.0 to 1.0).
    /// </summary>
    public double SystemLoad { get; set; }

    /// <summary>
    ///     Gets or sets the current network health status.
    /// </summary>
    public NetworkCondition NetworkCondition { get; set; }

    /// <summary>
    ///     Gets or sets the timestamp when metrics were last updated.
    /// </summary>
    public DateTimeOffset LastUpdated { get; set; }

    /// <summary>
    ///     Gets or sets the total number of events processed.
    /// </summary>
    public long TotalEventsProcessed { get; set; }

    /// <summary>
    ///     Gets or sets the average processing latency.
    /// </summary>
    public TimeSpan AverageProcessingLatency { get; set; }

    /// <summary>
    ///     Gets or sets the watermark accuracy metrics.
    /// </summary>
    public WatermarkAccuracy WatermarkAccuracy { get; set; } = new();

    /// <summary>
    ///     Gets a value indicating whether the watermark is healthy.
    /// </summary>
    public bool IsHealthy =>
        ProcessingDelay < TimeSpan.FromSeconds(10) &&
        SystemLoad < 0.9 &&
        NetworkCondition != NetworkCondition.Critical &&
        WatermarkAccuracy.OverallAccuracy > 0.8;

    /// <summary>
    ///     Returns a string representation of the watermark metrics.
    /// </summary>
    /// <returns>String representation of the watermark metrics.</returns>
    public override string ToString()
    {
        return $"Watermark: {CurrentWatermark:HH:mm:ss.fff} | " +
               $"Delay: {ProcessingDelay.TotalMilliseconds:F0}ms | " +
               $"Late Data: {LateDataCount} | " +
               $"Load: {SystemLoad:P0} | " +
               $"Network: {NetworkCondition} | " +
               $"Health: {(IsHealthy ? "✓" : "✗")}";
    }
}

/// <summary>
///     Represents network condition status.
/// </summary>
public enum NetworkCondition
{
    /// <summary>
    ///     Excellent network condition.
    /// </summary>
    Excellent,

    /// <summary>
    ///     Good network condition.
    /// </summary>
    Good,

    /// <summary>
    ///     Fair network condition.
    /// </summary>
    Fair,

    /// <summary>
    ///     Poor network condition.
    /// </summary>
    Poor,

    /// <summary>
    ///     Critical network condition.
    /// </summary>
    Critical,
}

/// <summary>
///     Represents watermark accuracy metrics.
/// </summary>
public class WatermarkAccuracy
{
    /// <summary>
    ///     Initializes a new instance of the WatermarkAccuracy class.
    /// </summary>
    public WatermarkAccuracy()
    {
        PrecisionScore = 1.0;
        TimelinessScore = 1.0;
        ConsistencyScore = 1.0;
        CompletenessScore = 1.0;
    }

    /// <summary>
    ///     Gets or sets the precision score (0.0 to 1.0).
    /// </summary>
    public double PrecisionScore { get; set; }

    /// <summary>
    ///     Gets or sets the timeliness score (0.0 to 1.0).
    /// </summary>
    public double TimelinessScore { get; set; }

    /// <summary>
    ///     Gets or sets the consistency score (0.0 to 1.0).
    /// </summary>
    public double ConsistencyScore { get; set; }

    /// <summary>
    ///     Gets or sets the completeness score (0.0 to 1.0).
    /// </summary>
    public double CompletenessScore { get; set; }

    /// <summary>
    ///     Gets the overall accuracy score (0.0 to 1.0).
    /// </summary>
    public double OverallAccuracy => (PrecisionScore + TimelinessScore + ConsistencyScore + CompletenessScore) / 4.0;

    /// <summary>
    ///     Returns a string representation of the watermark accuracy.
    /// </summary>
    /// <returns>String representation of the watermark accuracy.</returns>
    public override string ToString()
    {
        return $"Accuracy: {OverallAccuracy:P0} " +
               $"[P:{PrecisionScore:P0} T:{TimelinessScore:P0} C:{ConsistencyScore:P0} L:{CompletenessScore:P0}]";
    }
}

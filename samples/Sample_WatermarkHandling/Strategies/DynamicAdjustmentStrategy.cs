using Microsoft.Extensions.Logging;
using Sample_WatermarkHandling.Models;

namespace Sample_WatermarkHandling.Strategies;

/// <summary>
///     Strategy that dynamically adjusts watermarks based on system load and conditions.
///     This strategy demonstrates adaptive watermark management based on real-time system performance.
/// </summary>
public class DynamicAdjustmentStrategy
{
    private readonly DynamicAdjustmentConfiguration _config;
    private readonly ILogger<DynamicAdjustmentStrategy> _logger;
    private readonly SystemMetricsTracker _metricsTracker;

    /// <summary>
    ///     Initializes a new instance of the DynamicAdjustmentStrategy class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public DynamicAdjustmentStrategy(ILogger<DynamicAdjustmentStrategy> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = InitializeDynamicAdjustmentConfiguration();
        _metricsTracker = new SystemMetricsTracker();
    }

    /// <summary>
    ///     Adjusts watermark based on dynamic system conditions.
    /// </summary>
    /// <param name="baseWatermark">The base watermark to adjust.</param>
    /// <param name="globalMetrics">The current global metrics.</param>
    /// <returns>The dynamically adjusted watermark.</returns>
    public DateTimeOffset AdjustWatermark(DateTimeOffset baseWatermark, WatermarkMetrics globalMetrics)
    {
        // Update metrics tracker with current system state
        _metricsTracker.UpdateMetrics(globalMetrics);

        // Calculate dynamic adjustment based on system conditions
        var adjustment = CalculateDynamicAdjustment(globalMetrics);

        var adjustedWatermark = baseWatermark.Add(adjustment);

        _logger.LogDebug(
            "Dynamic watermark adjustment: Base={BaseWatermark:HH:mm:ss.fff}, Load={Load:P0}, Adjustment={AdjustmentMs:F0}ms, Adjusted={AdjustedWatermark:HH:mm:ss.fff}",
            baseWatermark,
            globalMetrics.SystemLoad,
            adjustment.TotalMilliseconds,
            adjustedWatermark);

        return adjustedWatermark;
    }

    /// <summary>
    ///     Calculates dynamic adjustment based on system metrics.
    /// </summary>
    /// <param name="globalMetrics">The current global metrics.</param>
    /// <returns>The adjustment to apply.</returns>
    private TimeSpan CalculateDynamicAdjustment(WatermarkMetrics globalMetrics)
    {
        var systemLoad = globalMetrics.SystemLoad;
        var networkCondition = globalMetrics.NetworkCondition;
        var processingDelay = globalMetrics.ProcessingDelay;

        // Calculate load-based adjustment
        var loadAdjustment = CalculateLoadBasedAdjustment(systemLoad);

        // Calculate network-based adjustment
        var networkAdjustment = CalculateNetworkBasedAdjustment(networkCondition);

        // Calculate latency-based adjustment
        var latencyAdjustment = CalculateLatencyBasedAdjustment(processingDelay);

        // Combine adjustments with weights
        var combinedAdjustment = TimeSpan.FromMilliseconds(
            loadAdjustment.TotalMilliseconds * _config.LoadWeight +
            networkAdjustment.TotalMilliseconds * _config.NetworkWeight +
            latencyAdjustment.TotalMilliseconds * _config.LatencyWeight);

        return combinedAdjustment;
    }

    /// <summary>
    ///     Calculates load-based adjustment.
    /// </summary>
    /// <param name="systemLoad">The current system load (0.0 to 1.0).</param>
    /// <returns>The load-based adjustment.</returns>
    private TimeSpan CalculateLoadBasedAdjustment(double systemLoad)
    {
        // High load: be more conservative (hold watermark back)
        if (systemLoad > _config.HighLoadThreshold)
            return TimeSpan.FromMilliseconds(_config.HighLoadAdjustmentMs);

        // Medium load: neutral adjustment
        if (systemLoad > _config.MediumLoadThreshold)
            return TimeSpan.FromMilliseconds(_config.MediumLoadAdjustmentMs);

        // Low load: be more aggressive (advance watermark)
        return TimeSpan.FromMilliseconds(_config.LowLoadAdjustmentMs);
    }

    /// <summary>
    ///     Calculates network-based adjustment.
    /// </summary>
    /// <param name="networkCondition">The current network condition.</param>
    /// <returns>The network-based adjustment.</returns>
    private TimeSpan CalculateNetworkBasedAdjustment(NetworkCondition networkCondition)
    {
        return networkCondition switch
        {
            NetworkCondition.Excellent => TimeSpan.FromMilliseconds(_config.ExcellentNetworkAdjustmentMs),
            NetworkCondition.Good => TimeSpan.FromMilliseconds(_config.GoodNetworkAdjustmentMs),
            NetworkCondition.Fair => TimeSpan.FromMilliseconds(_config.FairNetworkAdjustmentMs),
            NetworkCondition.Poor => TimeSpan.FromMilliseconds(_config.PoorNetworkAdjustmentMs),
            NetworkCondition.Critical => TimeSpan.FromMilliseconds(_config.CriticalNetworkAdjustmentMs),
            _ => TimeSpan.Zero,
        };
    }

    /// <summary>
    ///     Calculates latency-based adjustment.
    /// </summary>
    /// <param name="processingDelay">The current processing delay.</param>
    /// <returns>The latency-based adjustment.</returns>
    private TimeSpan CalculateLatencyBasedAdjustment(TimeSpan processingDelay)
    {
        // High latency: be more conservative
        if (processingDelay > _config.HighLatencyThreshold)
            return TimeSpan.FromMilliseconds(_config.HighLatencyAdjustmentMs);

        // Medium latency: moderate adjustment
        if (processingDelay > _config.MediumLatencyThreshold)
            return TimeSpan.FromMilliseconds(_config.MediumLatencyAdjustmentMs);

        // Low latency: be more aggressive
        return TimeSpan.FromMilliseconds(_config.LowLatencyAdjustmentMs);
    }

    /// <summary>
    ///     Initializes dynamic adjustment configuration.
    /// </summary>
    /// <returns>The dynamic adjustment configuration.</returns>
    private static DynamicAdjustmentConfiguration InitializeDynamicAdjustmentConfiguration()
    {
        return new DynamicAdjustmentConfiguration
        {
            // Load thresholds
            HighLoadThreshold = 0.8,
            MediumLoadThreshold = 0.5,

            // Load adjustments (in milliseconds)
            HighLoadAdjustmentMs = 200, // Hold watermark back
            MediumLoadAdjustmentMs = 50, // Slight hold back
            LowLoadAdjustmentMs = -25, // Advance watermark

            // Network adjustments (in milliseconds)
            ExcellentNetworkAdjustmentMs = -50, // Advance watermark
            GoodNetworkAdjustmentMs = -25, // Slight advance
            FairNetworkAdjustmentMs = 50, // Slight hold back
            PoorNetworkAdjustmentMs = 150, // Hold watermark back
            CriticalNetworkAdjustmentMs = 300, // Significant hold back

            // Latency thresholds
            HighLatencyThreshold = TimeSpan.FromMilliseconds(1000),
            MediumLatencyThreshold = TimeSpan.FromMilliseconds(500),

            // Latency adjustments (in milliseconds)
            HighLatencyAdjustmentMs = 100, // Hold watermark back
            MediumLatencyAdjustmentMs = 25, // Slight hold back
            LowLatencyAdjustmentMs = -25, // Advance watermark

            // Adjustment weights
            LoadWeight = 0.4, // 40% weight for system load
            NetworkWeight = 0.3, // 30% weight for network condition
            LatencyWeight = 0.3, // 30% weight for processing latency

            // Adaptation settings
            EnableAdaptiveMode = true,
            AdaptationInterval = TimeSpan.FromSeconds(30), // Adapt every 30 seconds
            MaxAdjustmentMs = 500, // Maximum adjustment in any direction
            MinAdjustmentMs = -200, // Maximum negative adjustment
        };
    }
}

/// <summary>
///     Represents dynamic adjustment configuration.
/// </summary>
public class DynamicAdjustmentConfiguration
{
    /// <summary>
    ///     Gets or sets the high load threshold.
    /// </summary>
    public double HighLoadThreshold { get; set; }

    /// <summary>
    ///     Gets or sets the medium load threshold.
    /// </summary>
    public double MediumLoadThreshold { get; set; }

    /// <summary>
    ///     Gets or sets the high load adjustment in milliseconds.
    /// </summary>
    public int HighLoadAdjustmentMs { get; set; }

    /// <summary>
    ///     Gets or sets the medium load adjustment in milliseconds.
    /// </summary>
    public int MediumLoadAdjustmentMs { get; set; }

    /// <summary>
    ///     Gets or sets the low load adjustment in milliseconds.
    /// </summary>
    public int LowLoadAdjustmentMs { get; set; }

    /// <summary>
    ///     Gets or sets the excellent network adjustment in milliseconds.
    /// </summary>
    public int ExcellentNetworkAdjustmentMs { get; set; }

    /// <summary>
    ///     Gets or sets the good network adjustment in milliseconds.
    /// </summary>
    public int GoodNetworkAdjustmentMs { get; set; }

    /// <summary>
    ///     Gets or sets the fair network adjustment in milliseconds.
    /// </summary>
    public int FairNetworkAdjustmentMs { get; set; }

    /// <summary>
    ///     Gets or sets the poor network adjustment in milliseconds.
    /// </summary>
    public int PoorNetworkAdjustmentMs { get; set; }

    /// <summary>
    ///     Gets or sets the critical network adjustment in milliseconds.
    /// </summary>
    public int CriticalNetworkAdjustmentMs { get; set; }

    /// <summary>
    ///     Gets or sets the high latency threshold.
    /// </summary>
    public TimeSpan HighLatencyThreshold { get; set; }

    /// <summary>
    ///     Gets or sets the medium latency threshold.
    /// </summary>
    public TimeSpan MediumLatencyThreshold { get; set; }

    /// <summary>
    ///     Gets or sets the high latency adjustment in milliseconds.
    /// </summary>
    public int HighLatencyAdjustmentMs { get; set; }

    /// <summary>
    ///     Gets or sets the medium latency adjustment in milliseconds.
    /// </summary>
    public int MediumLatencyAdjustmentMs { get; set; }

    /// <summary>
    ///     Gets or sets the low latency adjustment in milliseconds.
    /// </summary>
    public int LowLatencyAdjustmentMs { get; set; }

    /// <summary>
    ///     Gets or sets the load weight.
    /// </summary>
    public double LoadWeight { get; set; }

    /// <summary>
    ///     Gets or sets the network weight.
    /// </summary>
    public double NetworkWeight { get; set; }

    /// <summary>
    ///     Gets or sets the latency weight.
    /// </summary>
    public double LatencyWeight { get; set; }

    /// <summary>
    ///     Gets or sets whether adaptive mode is enabled.
    /// </summary>
    public bool EnableAdaptiveMode { get; set; }

    /// <summary>
    ///     Gets or sets the adaptation interval.
    /// </summary>
    public TimeSpan AdaptationInterval { get; set; }

    /// <summary>
    ///     Gets or sets the maximum adjustment in milliseconds.
    /// </summary>
    public int MaxAdjustmentMs { get; set; }

    /// <summary>
    ///     Gets or sets the minimum adjustment in milliseconds.
    /// </summary>
    public int MinAdjustmentMs { get; set; }
}

/// <summary>
///     Tracks system metrics for dynamic adjustment.
/// </summary>
internal sealed class SystemMetricsTracker
{
    private TimeSpan _averageLatency = TimeSpan.Zero;

    /// <summary>
    ///     Gets the current average system load.
    /// </summary>
    public double AverageLoad { get; private set; }

    /// <summary>
    ///     Gets the current average latency.
    /// </summary>
    public TimeSpan AverageLatency => _averageLatency;

    /// <summary>
    ///     Gets the last network condition.
    /// </summary>
    public NetworkCondition LastNetworkCondition { get; private set; } = NetworkCondition.Good;

    /// <summary>
    ///     Gets the last update time.
    /// </summary>
    public DateTimeOffset LastUpdateTime { get; private set; } = DateTimeOffset.MinValue;

    /// <summary>
    ///     Gets the sample count.
    /// </summary>
    public int SampleCount { get; private set; }

    /// <summary>
    ///     Updates the metrics tracker with new system metrics.
    /// </summary>
    /// <param name="globalMetrics">The current global metrics.</param>
    public void UpdateMetrics(WatermarkMetrics globalMetrics)
    {
        var now = DateTimeOffset.UtcNow;

        // Update running averages
        SampleCount++;
        var alpha = 0.1; // Smoothing factor
        AverageLoad = AverageLoad * (1 - alpha) + globalMetrics.SystemLoad * alpha;

        _averageLatency = TimeSpan.FromTicks(
            (long)(_averageLatency.Ticks * (1 - alpha) + globalMetrics.ProcessingDelay.Ticks * alpha));

        LastNetworkCondition = globalMetrics.NetworkCondition;
        LastUpdateTime = now;
    }
}

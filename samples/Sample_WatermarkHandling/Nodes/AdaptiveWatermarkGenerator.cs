using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_WatermarkHandling.Models;
using Sample_WatermarkHandling.Strategies;

namespace Sample_WatermarkHandling.Nodes;

/// <summary>
///     Transform node that generates adaptive watermarks based on network conditions and device characteristics.
///     This node demonstrates sophisticated watermark generation with multiple strategies and dynamic adjustment.
/// </summary>
public class AdaptiveWatermarkGenerator : TransformNode<SensorReading, SensorReading>
{
    private readonly ConcurrentDictionary<string, WatermarkMetrics> _deviceMetrics = new();
    private readonly DeviceSpecificLatenessStrategy _deviceStrategy;
    private readonly ConcurrentDictionary<string, DateTimeOffset> _deviceWatermarks = new();
    private readonly DynamicAdjustmentStrategy _dynamicStrategy;
    private readonly WatermarkMetrics _globalMetrics = new();
    private readonly ILogger<AdaptiveWatermarkGenerator> _logger;
    private readonly NetworkAwareWatermarkStrategy _networkStrategy;
    private DateTimeOffset _currentWatermark = DateTimeOffset.MinValue;

    /// <summary>
    ///     Initializes a new instance of the AdaptiveWatermarkGenerator class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    /// <param name="networkStrategy">The network-aware watermark strategy.</param>
    /// <param name="deviceStrategy">The device-specific lateness strategy.</param>
    /// <param name="dynamicStrategy">The dynamic adjustment strategy.</param>
    public AdaptiveWatermarkGenerator(
        ILogger<AdaptiveWatermarkGenerator> logger,
        NetworkAwareWatermarkStrategy networkStrategy,
        DeviceSpecificLatenessStrategy deviceStrategy,
        DynamicAdjustmentStrategy dynamicStrategy)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _networkStrategy = networkStrategy ?? throw new ArgumentNullException(nameof(networkStrategy));
        _deviceStrategy = deviceStrategy ?? throw new ArgumentNullException(nameof(deviceStrategy));
        _dynamicStrategy = dynamicStrategy ?? throw new ArgumentNullException(nameof(dynamicStrategy));
    }

    /// <summary>
    ///     Processes sensor reading and generates adaptive watermark.
    /// </summary>
    /// <param name="reading">The input sensor reading.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The processed sensor reading with watermark information.</returns>
    public override async Task<SensorReading> ExecuteAsync(
        SensorReading reading,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug("Processing sensor reading from {DeviceId} with timestamp {Timestamp}",
            reading.DeviceId, reading.Timestamp);

        // Get device metadata for watermark strategy selection
        var deviceMetadata = GetDeviceMetadata(reading.DeviceId);

        // Calculate device-specific watermark
        var deviceWatermark = await _deviceStrategy.CalculateWatermarkAsync(
            reading, deviceMetadata, cancellationToken);

        // Apply network-aware adjustments
        var networkAdjustedWatermark = _networkStrategy.AdjustWatermark(
            deviceWatermark, deviceMetadata.NetworkType);

        // Apply dynamic adjustments based on system conditions
        var finalWatermark = _dynamicStrategy.AdjustWatermark(
            networkAdjustedWatermark, _globalMetrics);

        // Update device and global watermark tracking
        UpdateWatermarkTracking(reading.DeviceId, finalWatermark, reading.Timestamp);

        // Update quality indicators with watermark information
        var updatedQualityIndicators = UpdateQualityIndicators(
            reading.QualityIndicators, finalWatermark, reading.Timestamp);

        var processedReading = reading with { QualityIndicators = updatedQualityIndicators };

        _logger.LogDebug("Processed reading from {DeviceId}, final watermark: {Watermark}",
            reading.DeviceId, finalWatermark);

        return processedReading;
    }

    /// <summary>
    ///     Gets device metadata for watermark strategy selection.
    /// </summary>
    /// <param name="deviceId">The device identifier.</param>
    /// <returns>Device metadata for watermark strategy selection.</returns>
    private DeviceMetadata GetDeviceMetadata(string deviceId)
    {
        // Simulate device metadata lookup based on device ID patterns
        var deviceType = deviceId.Split('-')[0];

        var networkType = deviceType switch
        {
            "PLA" => NetworkType.WiFi,
            "PLB" => NetworkType.LoRaWAN,
            "ENV" => NetworkType.Ethernet,
            _ => NetworkType.Unknown,
        };

        var clockAccuracy = networkType switch
        {
            NetworkType.WiFi => ClockAccuracy.GPSDisciplined,
            NetworkType.LoRaWAN => ClockAccuracy.NTPSynchronized,
            NetworkType.Ethernet => ClockAccuracy.InternalWithDrift,
            _ => ClockAccuracy.Unknown,
        };

        return new DeviceMetadata(
            deviceId,
            deviceType,
            networkType,
            clockAccuracy,
            $"Location-{deviceType}",
            DateTime.UtcNow.AddDays(-30));
    }

    /// <summary>
    ///     Updates watermark tracking for devices and global metrics.
    /// </summary>
    /// <param name="deviceId">The device identifier.</param>
    /// <param name="watermark">The calculated watermark.</param>
    /// <param name="eventTime">The event timestamp.</param>
    private void UpdateWatermarkTracking(string deviceId, DateTimeOffset watermark, DateTimeOffset eventTime)
    {
        // Update device-specific watermark
        _deviceWatermarks.AddOrUpdate(deviceId, watermark, (_, existing) =>
            existing > watermark
                ? existing
                : watermark);

        // Update device metrics
        var deviceMetrics = _deviceMetrics.GetOrAdd(deviceId, _ => new WatermarkMetrics());
        deviceMetrics.CurrentWatermark = watermark;
        deviceMetrics.ProcessingDelay = DateTimeOffset.UtcNow - eventTime;
        deviceMetrics.LastUpdated = DateTimeOffset.UtcNow;

        // Update global watermark (minimum of all device watermarks)
        var minDeviceWatermark = DateTimeOffset.MaxValue;

        foreach (var deviceWatermark in _deviceWatermarks.Values)
        {
            if (deviceWatermark < minDeviceWatermark)
                minDeviceWatermark = deviceWatermark;
        }

        _currentWatermark = minDeviceWatermark;
        _globalMetrics.CurrentWatermark = minDeviceWatermark;
        _globalMetrics.LastUpdated = DateTimeOffset.UtcNow;

        // Calculate global metrics
        UpdateGlobalMetrics();
    }

    /// <summary>
    ///     Updates global watermark metrics.
    /// </summary>
    private void UpdateGlobalMetrics()
    {
        var totalEvents = 0L;
        var totalDelay = TimeSpan.Zero;
        var lateDataCount = 0L;

        foreach (var deviceMetrics in _deviceMetrics.Values)
        {
            totalEvents++;
            totalDelay += deviceMetrics.ProcessingDelay;
            lateDataCount += deviceMetrics.LateDataCount;
        }

        _globalMetrics.TotalEventsProcessed = totalEvents;

        _globalMetrics.ProcessingDelay = totalEvents > 0
            ? TimeSpan.FromTicks(totalDelay.Ticks / totalEvents)
            : TimeSpan.Zero;

        _globalMetrics.LateDataCount = lateDataCount;
        _globalMetrics.SystemLoad = CalculateSystemLoad();
        _globalMetrics.NetworkCondition = AssessNetworkCondition();
    }

    /// <summary>
    ///     Calculates current system load.
    /// </summary>
    /// <returns>System load as a value between 0.0 and 1.0.</returns>
    private double CalculateSystemLoad()
    {
        // Simulate system load calculation based on active devices and processing rates
        var activeDeviceCount = _deviceWatermarks.Count;
        var baseLoad = activeDeviceCount / 10.0; // Normalize to 0-1 range
        return Math.Min(1.0, baseLoad);
    }

    /// <summary>
    ///     Assesses current network condition.
    /// </summary>
    /// <returns>Current network condition.</returns>
    private NetworkCondition AssessNetworkCondition()
    {
        var averageDelay = _globalMetrics.ProcessingDelay;

        return averageDelay switch
        {
            var d when d < TimeSpan.FromMilliseconds(100) => NetworkCondition.Excellent,
            var d when d < TimeSpan.FromMilliseconds(250) => NetworkCondition.Good,
            var d when d < TimeSpan.FromMilliseconds(500) => NetworkCondition.Fair,
            var d when d < TimeSpan.FromMilliseconds(1000) => NetworkCondition.Poor,
            _ => NetworkCondition.Critical,
        };
    }

    /// <summary>
    ///     Updates quality indicators with watermark information.
    /// </summary>
    /// <param name="originalIndicators">The original quality indicators.</param>
    /// <param name="watermark">The calculated watermark.</param>
    /// <param name="eventTime">The event timestamp.</param>
    /// <returns>Updated quality indicators with watermark information.</returns>
    private DataQualityIndicators UpdateQualityIndicators(
        DataQualityIndicators originalIndicators,
        DateTimeOffset watermark,
        DateTimeOffset eventTime)
    {
        var lateness = watermark - eventTime;

        return new DataQualityIndicators
        {
            CompletenessScore = originalIndicators.CompletenessScore,
            TimelinessScore = originalIndicators.TimelinessScore,
            AccuracyScore = originalIndicators.AccuracyScore,
            ConsistencyScore = originalIndicators.ConsistencyScore,
            IsStale = originalIndicators.IsStale,
            HasGaps = originalIndicators.HasGaps,
            IsSuspiciousValue = originalIndicators.IsSuspiciousValue,
            IsOutOfOrder = originalIndicators.IsOutOfOrder || lateness > TimeSpan.Zero,
            IsDuplicate = originalIndicators.IsDuplicate,
            IsIncomplete = originalIndicators.IsIncomplete,
            IsInconsistent = originalIndicators.IsInconsistent,
            IsDelayed = originalIndicators.IsDelayed || lateness > TimeSpan.FromMilliseconds(100),
            HasErrors = originalIndicators.HasErrors,
        };
    }
}

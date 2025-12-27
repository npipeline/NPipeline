using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Sample_WatermarkHandling.Models;

namespace Sample_WatermarkHandling.Strategies;

/// <summary>
///     Strategy that provides per-device lateness tolerance configuration.
///     This strategy demonstrates device-specific watermark handling based on clock accuracy and device characteristics.
/// </summary>
public class DeviceSpecificLatenessStrategy
{
    private readonly ConcurrentDictionary<string, DeviceLatenessConfig> _deviceConfigs = new();
    private readonly ILogger<DeviceSpecificLatenessStrategy> _logger;

    /// <summary>
    ///     Initializes a new instance of the DeviceSpecificLatenessStrategy class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public DeviceSpecificLatenessStrategy(ILogger<DeviceSpecificLatenessStrategy> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        InitializeDeviceConfigurations();
    }

    /// <summary>
    ///     Calculates watermark based on device-specific lateness configuration.
    /// </summary>
    /// <param name="reading">The sensor reading.</param>
    /// <param name="deviceMetadata">The device metadata.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The calculated watermark.</returns>
    public async Task<DateTimeOffset> CalculateWatermarkAsync(
        SensorReading reading,
        DeviceMetadata deviceMetadata,
        CancellationToken cancellationToken)
    {
        var deviceConfig = GetDeviceConfiguration(deviceMetadata.DeviceId);

        // Calculate device-specific watermark based on clock accuracy and lateness tolerance
        var deviceWatermark = CalculateDeviceSpecificWatermark(reading, deviceConfig, deviceMetadata);

        _logger.LogDebug(
            "Device-specific watermark calculation: {DeviceId} - Clock: {ClockAccuracy}, Tolerance: {ToleranceMs:F0}ms, Watermark: {Watermark:HH:mm:ss.fff}",
            deviceMetadata.DeviceId,
            deviceMetadata.ClockAccuracy,
            deviceConfig.LatenessTolerance.TotalMilliseconds,
            deviceWatermark);

        await Task.CompletedTask;
        return deviceWatermark;
    }

    /// <summary>
    ///     Gets device configuration for a specific device.
    /// </summary>
    /// <param name="deviceId">The device identifier.</param>
    /// <returns>The device lateness configuration.</returns>
    private DeviceLatenessConfig GetDeviceConfiguration(string deviceId)
    {
        var deviceType = ExtractDeviceType(deviceId);
        return _deviceConfigs.GetOrAdd(deviceType, GetDefaultDeviceConfig(deviceType));
    }

    /// <summary>
    ///     Extracts device type from device ID.
    /// </summary>
    /// <param name="deviceId">The device identifier.</param>
    /// <returns>The device type.</returns>
    private static string ExtractDeviceType(string deviceId)
    {
        var parts = deviceId.Split('-');

        return parts.Length > 0
            ? parts[0]
            : "UNKNOWN";
    }

    /// <summary>
    ///     Calculates device-specific watermark.
    /// </summary>
    /// <param name="reading">The sensor reading.</param>
    /// <param name="deviceConfig">The device configuration.</param>
    /// <param name="deviceMetadata">The device metadata.</param>
    /// <returns>The calculated watermark.</returns>
    private DateTimeOffset CalculateDeviceSpecificWatermark(
        SensorReading reading,
        DeviceLatenessConfig deviceConfig,
        DeviceMetadata deviceMetadata)
    {
        // Base watermark on event time
        var baseWatermark = reading.Timestamp;

        // Apply clock accuracy adjustments
        var clockAdjustedWatermark = ApplyClockAccuracyAdjustment(baseWatermark, deviceMetadata.ClockAccuracy);

        // Apply device-specific lateness tolerance
        var toleranceAdjustedWatermark = clockAdjustedWatermark.Add(deviceConfig.LatenessTolerance);

        // Apply device reliability adjustments
        var reliabilityAdjustedWatermark = ApplyReliabilityAdjustment(toleranceAdjustedWatermark, deviceConfig);

        return reliabilityAdjustedWatermark;
    }

    /// <summary>
    ///     Applies clock accuracy adjustment to watermark.
    /// </summary>
    /// <param name="watermark">The base watermark.</param>
    /// <param name="clockAccuracy">The clock accuracy.</param>
    /// <returns>The clock-adjusted watermark.</returns>
    private static DateTimeOffset ApplyClockAccuracyAdjustment(DateTimeOffset watermark, ClockAccuracy clockAccuracy)
    {
        return clockAccuracy switch
        {
            ClockAccuracy.GPSDisciplined => watermark.AddMilliseconds(-10), // Very precise, can be aggressive
            ClockAccuracy.NTPSynchronized => watermark.AddMilliseconds(0), // Standard precision, neutral
            ClockAccuracy.InternalWithDrift => watermark.AddMilliseconds(50), // Less precise, be conservative
            _ => watermark, // Unknown accuracy, no adjustment
        };
    }

    /// <summary>
    ///     Applies reliability adjustment to watermark.
    /// </summary>
    /// <param name="watermark">The input watermark.</param>
    /// <param name="deviceConfig">The device configuration.</param>
    /// <returns>The reliability-adjusted watermark.</returns>
    private static DateTimeOffset ApplyReliabilityAdjustment(DateTimeOffset watermark, DeviceLatenessConfig deviceConfig)
    {
        // Adjust based on device reliability score
        var reliabilityFactor = deviceConfig.ReliabilityScore; // 0.0 to 1.0
        var reliabilityAdjustment = TimeSpan.FromMilliseconds((1.0 - reliabilityFactor) * 100); // Up to 100ms adjustment

        return watermark.Add(reliabilityAdjustment);
    }

    /// <summary>
    ///     Initializes default device configurations.
    /// </summary>
    private void InitializeDeviceConfigurations()
    {
        // Production Line A (WiFi, GPS-disciplined): High precision, low tolerance
        _deviceConfigs["PLA"] = new DeviceLatenessConfig
        {
            DeviceType = "Production Line A",
            ClockAccuracy = ClockAccuracy.GPSDisciplined,
            LatenessTolerance = TimeSpan.FromMilliseconds(100),
            ReliabilityScore = 0.95,
            Priority = 1, // High priority
            ExpectedLatency = TimeSpan.FromMilliseconds(50),
        };

        // Production Line B (LoRaWAN, NTP): Medium precision, high tolerance
        _deviceConfigs["PLB"] = new DeviceLatenessConfig
        {
            DeviceType = "Production Line B",
            ClockAccuracy = ClockAccuracy.NTPSynchronized,
            LatenessTolerance = TimeSpan.FromMilliseconds(500),
            ReliabilityScore = 0.85,
            Priority = 2, // Medium priority
            ExpectedLatency = TimeSpan.FromMilliseconds(500),
        };

        // Environmental (Ethernet, Internal): Variable precision, medium tolerance
        _deviceConfigs["ENV"] = new DeviceLatenessConfig
        {
            DeviceType = "Environmental",
            ClockAccuracy = ClockAccuracy.InternalWithDrift,
            LatenessTolerance = TimeSpan.FromMilliseconds(250),
            ReliabilityScore = 0.90,
            Priority = 3, // Low priority
            ExpectedLatency = TimeSpan.FromMilliseconds(100),
        };
    }

    /// <summary>
    ///     Gets default device configuration for a device type.
    /// </summary>
    /// <param name="deviceType">The device type.</param>
    /// <returns>The default device configuration.</returns>
    private static DeviceLatenessConfig GetDefaultDeviceConfig(string deviceType)
    {
        return new DeviceLatenessConfig
        {
            DeviceType = deviceType,
            ClockAccuracy = ClockAccuracy.Unknown,
            LatenessTolerance = TimeSpan.FromMilliseconds(100),
            ReliabilityScore = 0.8,
            Priority = 3,
            ExpectedLatency = TimeSpan.FromMilliseconds(100),
        };
    }
}

/// <summary>
///     Represents device-specific lateness configuration.
/// </summary>
public class DeviceLatenessConfig
{
    /// <summary>
    ///     Gets or sets the device type description.
    /// </summary>
    public string DeviceType { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the clock accuracy.
    /// </summary>
    public ClockAccuracy ClockAccuracy { get; set; }

    /// <summary>
    ///     Gets or sets the lateness tolerance.
    /// </summary>
    public TimeSpan LatenessTolerance { get; set; }

    /// <summary>
    ///     Gets or sets the reliability score (0.0 to 1.0).
    /// </summary>
    public double ReliabilityScore { get; set; }

    /// <summary>
    ///     Gets or sets the device priority (lower is higher priority).
    /// </summary>
    public int Priority { get; set; }

    /// <summary>
    ///     Gets or sets the expected latency.
    /// </summary>
    public TimeSpan ExpectedLatency { get; set; }

    /// <summary>
    ///     Returns a string representation of the device configuration.
    /// </summary>
    /// <returns>String representation of the device configuration.</returns>
    public override string ToString()
    {
        return
            $"{DeviceType}: Clock={ClockAccuracy}, Tolerance={LatenessTolerance.TotalMilliseconds:F0}ms, Reliability={ReliabilityScore:P0}, Priority={Priority}";
    }
}

namespace Sample_WatermarkHandling.Models;

/// <summary>
///     Represents device metadata for IoT sensors in the manufacturing platform.
///     This class contains information about device capabilities, network characteristics, and timing properties.
/// </summary>
public class DeviceMetadata
{
    /// <summary>
    ///     Initializes a new instance of the DeviceMetadata class.
    /// </summary>
    /// <param name="deviceId">Unique identifier for the device.</param>
    /// <param name="deviceType">Type of sensor device.</param>
    /// <param name="networkType">Network connection type.</param>
    /// <param name="clockAccuracy">Clock synchronization precision.</param>
    /// <param name="location">Physical location in the facility.</param>
    /// <param name="lastCalibration">Last calibration timestamp.</param>
    public DeviceMetadata(
        string deviceId,
        string deviceType,
        NetworkType networkType,
        ClockAccuracy clockAccuracy,
        string location,
        DateTime lastCalibration)
    {
        DeviceId = deviceId;
        DeviceType = deviceType;
        NetworkType = networkType;
        ClockAccuracy = clockAccuracy;
        Location = location;
        LastCalibration = lastCalibration;
        LatencyCharacteristics = GetDefaultLatencyCharacteristics(networkType);
        AdditionalProperties = new Dictionary<string, object>();
    }

    /// <summary>
    ///     Gets the unique identifier for the device.
    /// </summary>
    public string DeviceId { get; }

    /// <summary>
    ///     Gets the type of sensor device.
    /// </summary>
    public string DeviceType { get; }

    /// <summary>
    ///     Gets the network connection type.
    /// </summary>
    public NetworkType NetworkType { get; }

    /// <summary>
    ///     Gets the clock synchronization precision.
    /// </summary>
    public ClockAccuracy ClockAccuracy { get; }

    /// <summary>
    ///     Gets the physical location in the manufacturing facility.
    /// </summary>
    public string Location { get; }

    /// <summary>
    ///     Gets the last calibration timestamp.
    /// </summary>
    public DateTime LastCalibration { get; }

    /// <summary>
    ///     Gets the latency characteristics for the device.
    /// </summary>
    public LatencyCharacteristics LatencyCharacteristics { get; set; }

    /// <summary>
    ///     Gets additional device properties.
    /// </summary>
    public Dictionary<string, object> AdditionalProperties { get; }

    /// <summary>
    ///     Returns a string representation of the device metadata.
    /// </summary>
    /// <returns>String representation of the device metadata.</returns>
    public override string ToString()
    {
        return $"{DeviceId} ({DeviceType}) - {NetworkType} [{ClockAccuracy}] at {Location}";
    }

    /// <summary>
    ///     Gets default latency characteristics based on network type.
    /// </summary>
    /// <param name="networkType">The network type.</param>
    /// <returns>Default latency characteristics.</returns>
    private static LatencyCharacteristics GetDefaultLatencyCharacteristics(NetworkType networkType)
    {
        return networkType switch
        {
            NetworkType.WiFi => new LatencyCharacteristics(TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(50), 0.95),
            NetworkType.LoRaWAN => new LatencyCharacteristics(TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(5000), 0.85),
            NetworkType.Ethernet => new LatencyCharacteristics(TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(100), 0.98),
            NetworkType.Cellular => new LatencyCharacteristics(TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(500), 0.90),
            _ => new LatencyCharacteristics(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(1000), 0.80),
        };
    }
}

/// <summary>
///     Represents the network connection type for IoT devices.
/// </summary>
public enum NetworkType
{
    /// <summary>
    ///     WiFi network connection.
    /// </summary>
    WiFi,

    /// <summary>
    ///     LoRaWAN network connection.
    /// </summary>
    LoRaWAN,

    /// <summary>
    ///     Ethernet network connection.
    /// </summary>
    Ethernet,

    /// <summary>
    ///     Cellular network connection.
    /// </summary>
    Cellular,

    /// <summary>
    ///     Unknown network type.
    /// </summary>
    Unknown,
}

/// <summary>
///     Represents clock synchronization accuracy for IoT devices.
/// </summary>
public enum ClockAccuracy
{
    /// <summary>
    ///     GPS-disciplined clock with ±1ms accuracy.
    /// </summary>
    GPSDisciplined,

    /// <summary>
    ///     NTP-synchronized clock with ±10ms accuracy.
    /// </summary>
    NTPSynchronized,

    /// <summary>
    ///     Internal clock with drift compensation.
    /// </summary>
    InternalWithDrift,

    /// <summary>
    ///     Unknown clock accuracy.
    /// </summary>
    Unknown,
}

/// <summary>
///     Represents latency characteristics for IoT devices.
/// </summary>
public class LatencyCharacteristics
{
    /// <summary>
    ///     Initializes a new instance of the LatencyCharacteristics class.
    /// </summary>
    /// <param name="minimumLatency">Minimum expected latency.</param>
    /// <param name="maximumLatency">Maximum expected latency.</param>
    /// <param name="reliability">Network reliability factor (0.0 to 1.0).</param>
    public LatencyCharacteristics(TimeSpan minimumLatency, TimeSpan maximumLatency, double reliability)
    {
        MinimumLatency = minimumLatency;
        MaximumLatency = maximumLatency;
        Reliability = reliability;
    }

    /// <summary>
    ///     Gets the minimum expected latency.
    /// </summary>
    public TimeSpan MinimumLatency { get; }

    /// <summary>
    ///     Gets the maximum expected latency.
    /// </summary>
    public TimeSpan MaximumLatency { get; }

    /// <summary>
    ///     Gets the network reliability factor (0.0 to 1.0).
    /// </summary>
    public double Reliability { get; }

    /// <summary>
    ///     Gets the average expected latency.
    /// </summary>
    public TimeSpan AverageLatency => TimeSpan.FromMilliseconds((MinimumLatency.TotalMilliseconds + MaximumLatency.TotalMilliseconds) / 2.0);

    /// <summary>
    ///     Returns a string representation of the latency characteristics.
    /// </summary>
    /// <returns>String representation of the latency characteristics.</returns>
    public override string ToString()
    {
        return $"Latency: {MinimumLatency.TotalMilliseconds:F0}-{MaximumLatency.TotalMilliseconds:F0}ms (Reliability: {Reliability:P0})";
    }
}

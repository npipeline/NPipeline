using Microsoft.Extensions.Logging;
using Sample_WatermarkHandling.Models;

namespace Sample_WatermarkHandling.Strategies;

/// <summary>
///     Strategy that adapts watermark generation based on network conditions and characteristics.
///     This strategy demonstrates network-aware watermark generation for different IoT connectivity types.
/// </summary>
public class NetworkAwareWatermarkStrategy
{
    private readonly NetworkConfiguration _config;
    private readonly ILogger<NetworkAwareWatermarkStrategy> _logger;

    /// <summary>
    ///     Initializes a new instance of the NetworkAwareWatermarkStrategy class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public NetworkAwareWatermarkStrategy(ILogger<NetworkAwareWatermarkStrategy> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = InitializeNetworkConfiguration();
    }

    /// <summary>
    ///     Adjusts watermark based on network type and conditions.
    /// </summary>
    /// <param name="baseWatermark">The base watermark to adjust.</param>
    /// <param name="networkType">The network type.</param>
    /// <returns>The adjusted watermark.</returns>
    public DateTimeOffset AdjustWatermark(DateTimeOffset baseWatermark, NetworkType networkType)
    {
        var adjustment = GetNetworkAdjustment(networkType);
        var adjustedWatermark = baseWatermark.Add(adjustment);

        _logger.LogDebug(
            "Network-aware watermark adjustment: {NetworkType} - Base: {BaseWatermark:HH:mm:ss.fff}, Adjustment: {AdjustmentMs:F0}ms, Adjusted: {AdjustedWatermark:HH:mm:ss.fff}",
            networkType,
            baseWatermark,
            adjustment.TotalMilliseconds,
            adjustedWatermark);

        return adjustedWatermark;
    }

    /// <summary>
    ///     Calculates watermark adjustment based on network type.
    /// </summary>
    /// <param name="networkType">The network type.</param>
    /// <returns>The adjustment to apply.</returns>
    private TimeSpan GetNetworkAdjustment(NetworkType networkType)
    {
        return networkType switch
        {
            NetworkType.WiFi => _config.WiFiAdjustment,
            NetworkType.LoRaWAN => _config.LoRaWANAdjustment,
            NetworkType.Ethernet => _config.EthernetAdjustment,
            NetworkType.Cellular => _config.CellularAdjustment,
            _ => TimeSpan.Zero,
        };
    }

    /// <summary>
    ///     Initializes network configuration with default values.
    /// </summary>
    /// <returns>The network configuration.</returns>
    private static NetworkConfiguration InitializeNetworkConfiguration()
    {
        return new NetworkConfiguration
        {
            // WiFi networks: Aggressive watermarks with low latency tolerance
            WiFiAdjustment = TimeSpan.FromMilliseconds(-50), // Advance watermark slightly

            // LoRaWAN networks: Conservative watermarks with high latency tolerance
            LoRaWANAdjustment = TimeSpan.FromMilliseconds(500), // Hold watermark back

            // Ethernet networks: Balanced watermarks with medium latency tolerance
            EthernetAdjustment = TimeSpan.FromMilliseconds(100), // Slight hold back

            // Cellular networks: Moderate watermarks with variable latency tolerance
            CellularAdjustment = TimeSpan.FromMilliseconds(200), // Moderate hold back
        };
    }
}

/// <summary>
///     Represents network configuration for watermark strategies.
/// </summary>
public class NetworkConfiguration
{
    /// <summary>
    ///     Gets or sets the WiFi adjustment.
    /// </summary>
    public TimeSpan WiFiAdjustment { get; set; }

    /// <summary>
    ///     Gets or sets the LoRaWAN adjustment.
    /// </summary>
    public TimeSpan LoRaWANAdjustment { get; set; }

    /// <summary>
    ///     Gets or sets the Ethernet adjustment.
    /// </summary>
    public TimeSpan EthernetAdjustment { get; set; }

    /// <summary>
    ///     Gets or sets the Cellular adjustment.
    /// </summary>
    public TimeSpan CellularAdjustment { get; set; }
}

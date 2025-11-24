using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_WatermarkHandling.Infrastructure;
using Sample_WatermarkHandling.Models;

namespace Sample_WatermarkHandling.Nodes;

/// <summary>
///     Source node for Production Line B sensors with LoRaWAN connectivity and NTP synchronization.
///     This node simulates Flow Meters, Quality Cameras, and Power Monitors with low bandwidth and high latency.
/// </summary>
public class ProductionLineBSource : SourceNode<SensorReading>
{
    private readonly Dictionary<string, double> _baselineValues = new();
    private readonly TimeSpan _delay = TimeSpan.FromMilliseconds(200);
    private readonly List<string> _deviceIds = new() { "PLB-FLOW-001", "PLB-CAM-001", "PLB-PWR-001", "PLB-FLOW-002", "PLB-CAM-002" };
    private readonly ILogger<ProductionLineBSource> _logger;
    private readonly Random _random = new();

    /// <summary>
    ///     Initializes a new instance of the ProductionLineBSource class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public ProductionLineBSource(ILogger<ProductionLineBSource> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Initialize baseline values for realistic sensor data
        foreach (var deviceId in _deviceIds)
        {
            var deviceType = deviceId.Split('-')[1];

            _baselineValues[deviceId] = deviceType switch
            {
                "FLOW" => 50.0 + (_random.NextDouble() - 0.5) * 20.0, // Flow: 40-60 L/min
                "CAM" => 85.0 + (_random.NextDouble() - 0.5) * 10.0, // Quality: 80-90%
                "PWR" => 220.0 + (_random.NextDouble() - 0.5) * 40.0, // Power: 200-240 V
                _ => 0.0,
            };
        }
    }

    /// <summary>
    ///     Executes the source node to generate Production Line B sensor data.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A data pipe containing the sensor readings.</returns>
    public override IDataPipe<SensorReading> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting Production Line B Source with LoRaWAN sensors and NTP synchronization");

        try
        {
            var channel = Channel.CreateUnbounded<SensorReading>();
            var dataPipe = new ChannelDataPipe<SensorReading>(channel, "ProductionLineB-Source");

            // Start generating data in the background
            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var reading in GenerateSensorDataAsync(cancellationToken))
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            _logger.LogInformation("Production Line B data generation cancelled");
                            break;
                        }

                        await dataPipe.WriteAsync(reading, cancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Production Line B data generation cancelled");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error generating Production Line B sensor data");
                }
                finally
                {
                    _ = channel.Writer.TryComplete();
                }
            }, cancellationToken);

            return dataPipe;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error starting Production Line B Source");
            throw;
        }
    }

    /// <summary>
    ///     Generates realistic Production Line B sensor data with LoRaWAN characteristics.
    ///     Simulates LoRaWAN-based sensors with NTP synchronization (±10ms accuracy) and high latency.
    /// </summary>
    private async IAsyncEnumerable<SensorReading> GenerateSensorDataAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var deviceIdsCache = _deviceIds.ToArray();
        var deviceCount = deviceIdsCache.Length;

        const int batchCount = 15;

        for (var batch = 0; batch < batchCount && !cancellationToken.IsCancellationRequested; batch++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            for (var i = 0; i < deviceCount; i++)
            {
                var deviceId = deviceIdsCache[i];
                var deviceType = deviceId.Split('-')[1];
                var baseline = _baselineValues[deviceId];

                // Add realistic variation to baseline values
                var variation = (_random.NextDouble() - 0.5) * 0.15; // ±7.5% variation (higher due to network constraints)
                var value = baseline * (1.0 + variation);

                // Add occasional transmission errors for LoRaWAN simulation
                if (_random.NextDouble() < 0.08) // 8% chance of transmission issues
                {
                    value *= _random.NextDouble() < 0.5
                        ? 1.8
                        : 0.2; // ±80% transmission error
                }

                // Higher latency simulation for LoRaWAN
                var timestamp = DateTimeOffset.UtcNow.AddMilliseconds(-_random.Next(200, 2000)); // 200-2000ms delay

                var (unit, readingType) = deviceType switch
                {
                    "FLOW" => ("L/min", ReadingType.FlowMeter),
                    "CAM" => ("%", ReadingType.QualityCamera),
                    "PWR" => ("V", ReadingType.PowerMonitor),
                    _ => ("unknown", ReadingType.Temperature), // Default to Temperature for unknown types
                };

                var qualityIndicators = GenerateLoRaWANQualityIndicators();

                var reading = new SensorReading(
                    deviceId,
                    timestamp,
                    value,
                    unit,
                    readingType,
                    qualityIndicators
                );

                yield return reading;

                await Task.Delay(_delay, cancellationToken);
            }

            // Longer delay between batches for LoRaWAN bandwidth constraints
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
        }

        _logger?.LogInformation("Production Line B sensor data generation completed after {BatchCount} batches", batchCount);
    }

    /// <summary>
    ///     Generates data quality indicators for LoRaWAN sensors with NTP synchronization.
    /// </summary>
    /// <returns>Data quality indicators with moderate scores reflecting LoRaWAN characteristics.</returns>
    private DataQualityIndicators GenerateLoRaWANQualityIndicators()
    {
        return new DataQualityIndicators
        {
            CompletenessScore = 0.85 + _random.NextDouble() * 0.10, // 85-95%
            TimelinessScore = 0.70 + _random.NextDouble() * 0.20, // 70-90%
            AccuracyScore = 0.80 + _random.NextDouble() * 0.15, // 80-95%
            ConsistencyScore = 0.75 + _random.NextDouble() * 0.20, // 75-95%
            IsStale = _random.NextDouble() < 0.10, // 10% chance
            HasGaps = _random.NextDouble() < 0.15, // 15% chance
            IsSuspiciousValue = _random.NextDouble() < 0.08, // 8% chance
            IsOutOfOrder = _random.NextDouble() < 0.12, // 12% chance
            IsDuplicate = _random.NextDouble() < 0.05, // 5% chance
            IsIncomplete = _random.NextDouble() < 0.08, // 8% chance
            IsInconsistent = _random.NextDouble() < 0.10, // 10% chance
            IsDelayed = _random.NextDouble() < 0.20, // 20% chance
            HasErrors = _random.NextDouble() < 0.05, // 5% chance
        };
    }
}

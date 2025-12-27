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
///     Source node for Production Line A sensors with WiFi connectivity and GPS-disciplined clocks.
///     This node simulates Temperature, Pressure, and Vibration sensors with high bandwidth and low latency.
/// </summary>
public class ProductionLineASource : SourceNode<SensorReading>
{
    private readonly Dictionary<string, double> _baselineValues = new();
    private readonly TimeSpan _delay = TimeSpan.FromMilliseconds(20);
    private readonly List<string> _deviceIds = new() { "PLA-TEMP-001", "PLA-PRESS-001", "PLA-VIB-001", "PLA-TEMP-002", "PLA-PRESS-002" };
    private readonly ILogger<ProductionLineASource> _logger;
    private readonly Random _random = new();

    /// <summary>
    ///     Initializes a new instance of the ProductionLineASource class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public ProductionLineASource(ILogger<ProductionLineASource> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Initialize baseline values for realistic sensor data
        foreach (var deviceId in _deviceIds)
        {
            var deviceType = deviceId.Split('-')[1];

            _baselineValues[deviceId] = deviceType switch
            {
                "TEMP" => 22.5 + (_random.NextDouble() - 0.5) * 5.0, // Temperature: 20-25°C
                "PRESS" => 101.3 + (_random.NextDouble() - 0.5) * 10.0, // Pressure: 96-106 kPa
                "VIB" => 0.5 + (_random.NextDouble() - 0.5) * 0.4, // Vibration: 0.3-0.7 g
                _ => 0.0,
            };
        }
    }

    /// <summary>
    ///     Executes the source node to generate Production Line A sensor data.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A data pipe containing the sensor readings.</returns>
    public override IDataPipe<SensorReading> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting Production Line A Source with WiFi sensors and GPS-disciplined clocks");

        try
        {
            var channel = Channel.CreateUnbounded<SensorReading>();
            var dataPipe = new ChannelDataPipe<SensorReading>(channel, "ProductionLineA-Source");

            // Start generating data in the background
            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var reading in GenerateSensorDataAsync(cancellationToken))
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            _logger.LogInformation("Production Line A data generation cancelled");
                            break;
                        }

                        await dataPipe.WriteAsync(reading, cancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Production Line A data generation cancelled");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error generating Production Line A sensor data");
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
            _logger.LogError(ex, "Error starting Production Line A Source");
            throw;
        }
    }

    /// <summary>
    ///     Generates realistic Production Line A sensor data with high performance characteristics.
    ///     Simulates WiFi-based sensors with GPS-disciplined clocks (±1ms accuracy).
    /// </summary>
    private async IAsyncEnumerable<SensorReading> GenerateSensorDataAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var deviceIdsCache = _deviceIds.ToArray();
        var deviceCount = deviceIdsCache.Length;

        const int batchCount = 20;

        for (var batch = 0; batch < batchCount && !cancellationToken.IsCancellationRequested; batch++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            for (var i = 0; i < deviceCount; i++)
            {
                var deviceId = deviceIdsCache[i];
                var deviceType = deviceId.Split('-')[1];
                var baseline = _baselineValues[deviceId];

                // Add realistic variation to baseline values
                var variation = (_random.NextDouble() - 0.5) * 0.1; // ±5% variation
                var value = baseline * (1.0 + variation);

                // Add occasional anomalies for testing
                if (_random.NextDouble() < 0.05) // 5% chance of anomaly
                {
                    value *= _random.NextDouble() < 0.5
                        ? 1.5
                        : 0.5; // ±50% anomaly
                }

                var timestamp = DateTimeOffset.UtcNow.AddMilliseconds(-_random.Next(0, 50)); // Small delay simulation

                var (unit, readingType) = deviceType switch
                {
                    "TEMP" => ("°C", ReadingType.Temperature),
                    "PRESS" => ("kPa", ReadingType.Pressure),
                    "VIB" => ("g", ReadingType.Vibration),
                    _ => ("unknown", ReadingType.Temperature), // Default to Temperature for unknown types
                };

                var qualityIndicators = GenerateHighQualityIndicators();

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

            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        }

        _logger?.LogInformation("Production Line A sensor data generation completed after {BatchCount} batches", batchCount);
    }

    /// <summary>
    ///     Generates high-quality data indicators for WiFi sensors with GPS-disciplined clocks.
    /// </summary>
    /// <returns>Data quality indicators with high scores.</returns>
    private DataQualityIndicators GenerateHighQualityIndicators()
    {
        return new DataQualityIndicators
        {
            CompletenessScore = 0.98 + _random.NextDouble() * 0.02, // 98-100%
            TimelinessScore = 0.95 + _random.NextDouble() * 0.05, // 95-100%
            AccuracyScore = 0.97 + _random.NextDouble() * 0.03, // 97-100%
            ConsistencyScore = 0.96 + _random.NextDouble() * 0.04, // 96-100%
            IsStale = false,
            HasGaps = false,
            IsSuspiciousValue = _random.NextDouble() < 0.02, // 2% chance
            IsOutOfOrder = _random.NextDouble() < 0.01, // 1% chance
            IsDuplicate = false,
            IsIncomplete = false,
            IsInconsistent = _random.NextDouble() < 0.01, // 1% chance
            IsDelayed = false,
            HasErrors = false,
        };
    }
}

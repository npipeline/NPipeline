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
///     Source node for Environmental sensors with Ethernet connectivity and internal clocks with drift compensation.
///     This node simulates Humidity and Air Quality sensors with reliable connectivity and medium latency.
/// </summary>
public class EnvironmentalSource : SourceNode<SensorReading>
{
    private readonly Dictionary<string, double> _baselineValues = new();
    private readonly Dictionary<string, TimeSpan> _clockDrift = new(); // Simulate clock drift
    private readonly TimeSpan _delay = TimeSpan.FromMilliseconds(50);
    private readonly List<string> _deviceIds = new() { "ENV-HUM-001", "ENV-AQ-001", "ENV-HUM-002", "ENV-AQ-002", "ENV-TEMP-001" };
    private readonly ILogger<EnvironmentalSource> _logger;
    private readonly Random _random = new();

    /// <summary>
    ///     Initializes a new instance of the EnvironmentalSource class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public EnvironmentalSource(ILogger<EnvironmentalSource> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Initialize baseline values for realistic sensor data
        foreach (var deviceId in _deviceIds)
        {
            var deviceType = deviceId.Split('-')[1];

            _baselineValues[deviceId] = deviceType switch
            {
                "HUM" => 45.0 + (_random.NextDouble() - 0.5) * 20.0, // Humidity: 35-55%
                "AQ" => 25.0 + (_random.NextDouble() - 0.5) * 10.0, // Air Quality: 20-30 AQI
                "TEMP" => 21.0 + (_random.NextDouble() - 0.5) * 4.0, // Temperature: 19-23°C
                _ => 0.0,
            };

            // Initialize clock drift for internal clocks (±50ms drift)
            var driftMs = (_random.NextDouble() - 0.5) * 100; // -50 to +50ms
            _clockDrift[deviceId] = TimeSpan.FromMilliseconds(driftMs);
        }
    }

    /// <summary>
    ///     Executes the source node to generate Environmental sensor data.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A data pipe containing the sensor readings.</returns>
    public override IDataPipe<SensorReading> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting Environmental Source with Ethernet sensors and internal clocks with drift compensation");

        try
        {
            var channel = Channel.CreateUnbounded<SensorReading>();
            var dataPipe = new ChannelDataPipe<SensorReading>(channel, "Environmental-Source");

            // Start generating data in the background
            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var reading in GenerateSensorDataAsync(cancellationToken))
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            _logger.LogInformation("Environmental data generation cancelled");
                            break;
                        }

                        await dataPipe.WriteAsync(reading, cancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Environmental data generation cancelled");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error generating Environmental sensor data");
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
            _logger.LogError(ex, "Error starting Environmental Source");
            throw;
        }
    }

    /// <summary>
    ///     Generates realistic Environmental sensor data with Ethernet characteristics.
    ///     Simulates Ethernet-based sensors with internal clocks and drift compensation.
    /// </summary>
    private async IAsyncEnumerable<SensorReading> GenerateSensorDataAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var deviceIdsCache = _deviceIds.ToArray();
        var deviceCount = deviceIdsCache.Length;

        const int batchCount = 18;

        for (var batch = 0; batch < batchCount && !cancellationToken.IsCancellationRequested; batch++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            for (var i = 0; i < deviceCount; i++)
            {
                var deviceId = deviceIdsCache[i];
                var deviceType = deviceId.Split('-')[1];
                var baseline = _baselineValues[deviceId];

                // Add realistic variation to baseline values
                var variation = (_random.NextDouble() - 0.5) * 0.08; // ±4% variation
                var value = baseline * (1.0 + variation);

                // Add occasional sensor drift for internal clock simulation
                if (_random.NextDouble() < 0.03) // 3% chance of drift event
                {
                    value *= _random.NextDouble() < 0.5
                        ? 1.2
                        : 0.8; // ±20% drift
                }

                // Medium latency simulation for Ethernet with clock drift
                var baseTimestamp = DateTimeOffset.UtcNow.AddMilliseconds(-_random.Next(10, 200)); // 10-200ms delay
                var timestamp = baseTimestamp.Add(_clockDrift[deviceId]); // Apply clock drift

                var (unit, readingType) = deviceType switch
                {
                    "HUM" => ("%", ReadingType.Humidity),
                    "AQ" => ("AQI", ReadingType.AirQuality),
                    "TEMP" => ("°C", ReadingType.Temperature),
                    _ => ("unknown", ReadingType.Temperature), // Default to Temperature for unknown types
                };

                var qualityIndicators = GenerateEthernetQualityIndicators();

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

            await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken);
        }

        _logger?.LogInformation("Environmental sensor data generation completed after {BatchCount} batches", batchCount);
    }

    /// <summary>
    ///     Generates data quality indicators for Ethernet sensors with internal clocks.
    /// </summary>
    /// <returns>Data quality indicators with good scores reflecting Ethernet reliability.</returns>
    private DataQualityIndicators GenerateEthernetQualityIndicators()
    {
        return new DataQualityIndicators
        {
            CompletenessScore = 0.92 + _random.NextDouble() * 0.06, // 92-98%
            TimelinessScore = 0.88 + _random.NextDouble() * 0.10, // 88-98%
            AccuracyScore = 0.90 + _random.NextDouble() * 0.08, // 90-98%
            ConsistencyScore = 0.89 + _random.NextDouble() * 0.09, // 89-98%
            IsStale = _random.NextDouble() < 0.05, // 5% chance
            HasGaps = _random.NextDouble() < 0.06, // 6% chance
            IsSuspiciousValue = _random.NextDouble() < 0.04, // 4% chance
            IsOutOfOrder = _random.NextDouble() < 0.07, // 7% chance (higher due to clock drift)
            IsDuplicate = _random.NextDouble() < 0.02, // 2% chance
            IsIncomplete = _random.NextDouble() < 0.03, // 3% chance
            IsInconsistent = _random.NextDouble() < 0.05, // 5% chance
            IsDelayed = _random.NextDouble() < 0.08, // 8% chance
            HasErrors = _random.NextDouble() < 0.03, // 3% chance
        };
    }
}

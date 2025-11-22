using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_TimeWindowedJoinNode.Nodes;

/// <summary>
///     Source node that generates IoT sensor readings with timestamps for demonstrating time-windowed joins.
///     This node creates a stream of sensor readings from multiple devices with varying frequencies.
/// </summary>
public class SensorReadingSource : SourceNode<SensorReading>
{
    private static readonly Action<ILogger, int, Exception?> _startingGeneration =
        LoggerMessage.Define<int>(LogLevel.Information, new EventId(1, "StartingGeneration"),
            "SensorReadingSource: Starting to generate readings for {DeviceCount} devices");

    private static readonly Action<ILogger, string, string, DateTime, Exception?> _generatedReading =
        LoggerMessage.Define<string, string, DateTime>(LogLevel.Debug, new EventId(2, "GeneratedReading"),
            "SensorReadingSource: Generated reading {SensorId} for device {DeviceId} at {Timestamp}");

    private static readonly Action<ILogger, int, Exception?> _finishedGeneration =
        LoggerMessage.Define<int>(LogLevel.Information, new EventId(3, "FinishedGeneration"),
            "SensorReadingSource: Finished generating {TotalReadings} readings");

    private readonly TimeSpan _delayBetweenReadings;
    private readonly string[] _deviceIds;
    private readonly ILogger<SensorReadingSource>? _logger;
    private readonly int _maxReadings;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SensorReadingSource" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    /// <param name="delayBetweenReadings">Delay between generating readings to simulate real-time data.</param>
    /// <param name="maxReadings">Maximum number of readings to generate per device.</param>
    /// <param name="deviceIds">List of device IDs to generate readings for.</param>
    public SensorReadingSource(
        ILogger<SensorReadingSource>? logger = null,
        TimeSpan? delayBetweenReadings = null,
        int maxReadings = 50,
        string[]? deviceIds = null)
    {
        _logger = logger;
        _delayBetweenReadings = delayBetweenReadings ?? TimeSpan.FromMilliseconds(200);
        _maxReadings = maxReadings;
        _deviceIds = deviceIds ?? new[] { "TEMP-001", "PRESS-001", "FLOW-001", "VIB-001" };
    }

    /// <inheritdoc />
    public override IDataPipe<SensorReading> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        if (_logger != null)
            _startingGeneration(_logger, _deviceIds.Length, null);

        return new StreamingDataPipe<SensorReading>(GenerateReadingsAsync(cancellationToken), "SensorReadingSource");
    }

    private async IAsyncEnumerable<SensorReading> GenerateReadingsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var random = new Random(42); // Fixed seed for reproducible results
        var readingId = 1;

        foreach (var deviceId in _deviceIds)
        {
            for (var i = 0; i < _maxReadings; i++)
            {
                var baseTime = DateTime.UtcNow.AddMinutes(-random.Next(60));

                var reading = new SensorReading(
                    $"SENSOR-{readingId:D4}",
                    deviceId,
                    baseTime.AddSeconds(i * 10), // Readings every 10 seconds
                    20.0 + random.NextDouble() * 30.0, // 20-50°C
                    40.0 + random.NextDouble() * 40.0, // 40-80%
                    $"Location-{i % 5 + 1}"
                );

                if (_logger != null)
                    _generatedReading(_logger, reading.SensorId, reading.DeviceId, reading.Timestamp, null);

                yield return reading;

                readingId++;

                if (i < _maxReadings - 1)
                    await Task.Delay(_delayBetweenReadings, cancellationToken);
            }
        }

        if (_logger != null)
            _finishedGeneration(_logger, readingId - 1, null);
    }
}

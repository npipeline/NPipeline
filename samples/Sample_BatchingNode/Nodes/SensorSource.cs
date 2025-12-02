using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_BatchingNode.Nodes;

/// <summary>
///     Source node that generates individual sensor readings.
///     This node simulates IoT devices sending sensor data at regular intervals.
/// </summary>
public class SensorSource : SourceNode<SensorReading>
{
    private readonly int _deviceCount;
    private readonly TimeSpan _interval;
    private readonly int _readingCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SensorSource" /> class.
    /// </summary>
    /// <param name="readingCount">The number of sensor readings to generate.</param>
    /// <param name="interval">The interval between readings.</param>
    /// <param name="deviceCount">The number of different devices to simulate.</param>
    public SensorSource(int readingCount = 50, TimeSpan? interval = null, int deviceCount = 3)
    {
        _readingCount = readingCount;
        _interval = interval ?? TimeSpan.FromMilliseconds(100);
        _deviceCount = deviceCount;
    }

    /// <summary>
    ///     Generates a stream of sensor readings from multiple devices.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the sensor readings.</returns>
    public override IDataPipe<SensorReading> Execute(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Generating {_readingCount} sensor readings from {_deviceCount} devices with {_interval.TotalMilliseconds}ms intervals");

        var readings = new List<SensorReading>();
        var random = new Random(42); // Fixed seed for reproducible results
        var baseTime = DateTime.UtcNow;

        for (var i = 0; i < _readingCount; i++)
        {
            var deviceId = $"Device-{i % _deviceCount + 1:D2}";
            var timestamp = baseTime.AddMilliseconds(i * _interval.TotalMilliseconds);

            // Simulate realistic sensor data with some variation
            var baseTemp = 20.0 + i % _deviceCount * 2.0; // Different base temp per device
            var temperature = baseTemp + (random.NextDouble() - 0.5) * 5.0;
            var humidity = 40.0 + random.NextDouble() * 40.0; // 40-80% humidity
            var pressure = 1013.25 + (random.NextDouble() - 0.5) * 20.0; // Around standard pressure
            var batteryLevel = 100.0 - i * 0.1 + (random.NextDouble() - 0.5) * 5.0; // Gradually decreasing

            var reading = new SensorReading(
                deviceId,
                timestamp,
                Math.Round(temperature, 2),
                Math.Round(humidity, 2),
                Math.Round(pressure, 2),
                Math.Round(Math.Max(0, batteryLevel), 2));

            readings.Add(reading);

            // Simulate the interval between readings
            if (i < _readingCount - 1) // Don't wait after the last reading
                Task.Delay(_interval, cancellationToken).Wait(cancellationToken);
        }

        Console.WriteLine($"Successfully generated {readings.Count} sensor readings");

        // Group by device to show the distribution
        var deviceGroups = readings.GroupBy(r => r.DeviceId).ToList();

        foreach (var group in deviceGroups)
        {
            Console.WriteLine($"  {group.Key}: {group.Count()} readings");
        }

        return new InMemoryDataPipe<SensorReading>(readings, "SensorSource");
    }
}

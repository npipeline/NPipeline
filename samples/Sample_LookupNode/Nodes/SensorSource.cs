using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_LookupNode.Helpers;
using Sample_LookupNode.Models;

namespace Sample_LookupNode.Nodes;

/// <summary>
///     Source node that generates IoT sensor readings for the pipeline.
///     This node demonstrates how to create a source that produces sensor data
///     by inheriting from SourceNode&lt;SensorReading&gt;.
/// </summary>
public class SensorSource : SourceNode<SensorReading>
{
    private readonly List<string> _deviceIds = new();
    private readonly Random _random = new();

    /// <summary>
    ///     Generates a collection of IoT sensor readings.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the generated sensor readings.</returns>
    public override IDataPipe<SensorReading> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        // Get all available device IDs from the registry
        _deviceIds.AddRange(DeviceRegistry.GetAllDevices().Select(d => d.DeviceId));

        var readings = new List<SensorReading>();
        var now = DateTime.UtcNow;

        // Generate 15-20 sensor readings with realistic patterns
        var readingCount = _random.Next(15, 21);

        Console.WriteLine($"Generating {readingCount} sensor readings...");

        for (var i = 0; i < readingCount; i++)
        {
            // Add random delay between readings to simulate real-time data
            var delay = TimeSpan.FromMilliseconds(_random.Next(100, 500));
            Task.Delay(delay, cancellationToken).Wait(cancellationToken);

            var reading = GenerateSensorReading(now, i);
            readings.Add(reading);

            Console.WriteLine($"Generated: {reading.DeviceId} - {reading.ReadingType}: {reading.Value}{reading.Unit}");
        }

        Console.WriteLine($"Total sensor readings generated: {readings.Count}");
        Console.WriteLine();

        return new InMemoryDataPipe<SensorReading>(readings);
    }

    /// <summary>
    ///     Generates a single realistic sensor reading.
    /// </summary>
    private SensorReading GenerateSensorReading(DateTime baseTime, int index)
    {
        // Select a random device
        var deviceId = _deviceIds[_random.Next(_deviceIds.Count)];
        var device = DeviceRegistry.GetDevice(deviceId);

        if (device == null)
        {
            // Fallback to a default device if not found
            deviceId = "UNKNOWN-DEVICE";
        }

        // Generate timestamp with some variation around the base time
        var timestamp = baseTime.AddMinutes(-_random.Next(0, 60)).AddSeconds(_random.Next(-30, 30));

        // Generate reading based on device type
        var deviceType = device?.DeviceType?.ToLowerInvariant() ?? string.Empty;

        var (readingType, value, unit) = deviceType switch
        {
            var type when type.Contains("temperature") => GenerateTemperatureReading(),
            var type when type.Contains("pressure") => GeneratePressureReading(),
            var type when type.Contains("humidity") => GenerateHumidityReading(),
            var type when type.Contains("vibration") => GenerateVibrationReading(),
            _ => GenerateDefaultReading(),
        };

        return new SensorReading(deviceId, timestamp, value, unit, readingType);
    }

    /// <summary>
    ///     Generates a realistic temperature reading.
    /// </summary>
    private (string Type, double Value, string Unit) GenerateTemperatureReading()
    {
        // Simulate different temperature scenarios
        var scenarios = new[]
        {
            ("Temperature", 22.5 + _random.NextDouble() * 10 - 5, "°C"), // Normal room temperature
            ("Temperature", 35.0 + _random.NextDouble() * 15, "°C"), // High temperature
            ("Temperature", 5.0 + _random.NextDouble() * 10, "°C"), // Low temperature
            ("Temperature", 85.0 + _random.NextDouble() * 10, "°C"), // Critical high temperature
        };

        return scenarios[_random.Next(scenarios.Length)];
    }

    /// <summary>
    ///     Generates a realistic pressure reading.
    /// </summary>
    private (string Type, double Value, string Unit) GeneratePressureReading()
    {
        // Simulate different pressure scenarios
        var scenarios = new[]
        {
            ("Pressure", 100.0 + _random.NextDouble() * 50, "PSI"), // Normal pressure
            ("Pressure", 250.0 + _random.NextDouble() * 100, "PSI"), // High pressure
            ("Pressure", 450.0 + _random.NextDouble() * 50, "PSI"), // Critical high pressure
            ("Pressure", 15.0 + _random.NextDouble() * 10, "PSI"), // Low pressure
        };

        return scenarios[_random.Next(scenarios.Length)];
    }

    /// <summary>
    ///     Generates a realistic humidity reading.
    /// </summary>
    private (string Type, double Value, string Unit) GenerateHumidityReading()
    {
        // Simulate different humidity scenarios
        var scenarios = new[]
        {
            ("Humidity", 45.0 + _random.NextDouble() * 20, "%"), // Normal humidity
            ("Humidity", 70.0 + _random.NextDouble() * 15, "%"), // High humidity
            ("Humidity", 20.0 + _random.NextDouble() * 10, "%"), // Low humidity
            ("Humidity", 90.0 + _random.NextDouble() * 8, "%"), // Critical high humidity
        };

        return scenarios[_random.Next(scenarios.Length)];
    }

    /// <summary>
    ///     Generates a realistic vibration reading.
    /// </summary>
    private (string Type, double Value, string Unit) GenerateVibrationReading()
    {
        // Simulate different vibration scenarios
        var scenarios = new[]
        {
            ("Vibration", 5.0 + _random.NextDouble() * 10, "mm/s"), // Normal vibration
            ("Vibration", 20.0 + _random.NextDouble() * 15, "mm/s"), // High vibration
            ("Vibration", 35.0 + _random.NextDouble() * 15, "mm/s"), // Critical vibration
            ("Vibration", 2.0 + _random.NextDouble() * 3, "mm/s"), // Low vibration
        };

        return scenarios[_random.Next(scenarios.Length)];
    }

    /// <summary>
    ///     Generates a default reading for unknown device types.
    /// </summary>
    private (string Type, double Value, string Unit) GenerateDefaultReading()
    {
        return ("Generic", _random.NextDouble() * 100, "units");
    }
}

using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_08_CustomNodeImplementation.Models;

namespace Sample_08_CustomNodeImplementation.Nodes;

/// <summary>
///     Custom source node that generates simulated sensor data.
///     This node demonstrates how to implement a custom source node by inheriting from SourceNode&lt;T&gt;.
///     It generates sensor readings with realistic patterns and custom formatting.
/// </summary>
/// <remarks>
///     This implementation demonstrates:
///     - Custom source node development
///     - Lifecycle management with initialization and disposal
///     - Performance optimization through efficient data generation
///     - Structured code for testability
/// </remarks>
public class SensorDataSource : SourceNode<SensorData>
{
    private readonly Random _random = new();
    private readonly List<string> _sensorIds;

    /// <summary>
    ///     Initializes a new instance of the SensorDataSource class.
    /// </summary>
    public SensorDataSource()
    {
        _sensorIds = new List<string>();

        // Initialize with a set of sensor IDs
        for (var i = 1; i <= 5; i++)
        {
            _sensorIds.Add($"SENSOR-{i:D3}");
        }
    }

    /// <summary>
    ///     Generates simulated sensor data with realistic patterns.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the generated sensor data.</returns>
    public override IDataPipe<SensorData> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Generating sensor data...");
        Console.WriteLine($"Initialized {_sensorIds.Count} sensors: {string.Join(", ", _sensorIds)}");

        var sensorDataList = new List<SensorData>();
        var baseTime = DateTime.UtcNow.AddMinutes(-10); // Start 10 minutes ago

        // Generate 50 sensor readings (10 per sensor)
        for (var sensorIndex = 0; sensorIndex < _sensorIds.Count; sensorIndex++)
        {
            var sensorId = _sensorIds[sensorIndex];
            var baseValue = 20.0 + sensorIndex * 5.0; // Different baseline for each sensor

            for (var readingIndex = 0; readingIndex < 10; readingIndex++)
            {
                // Generate realistic sensor data with some noise
                var timestamp = baseTime.AddMinutes(readingIndex);
                var noise = (_random.NextDouble() - 0.5) * 2.0; // ±1.0 noise
                var value = baseValue + noise + Math.Sin(readingIndex * 0.5) * 2.0; // Add some pattern

                var sensorData = new SensorData
                {
                    SensorId = sensorId,
                    Timestamp = timestamp,
                    Value = Math.Round(value, 2),
                    Unit = sensorIndex % 2 == 0
                        ? "°C"
                        : "kPa", // Alternate between temperature and pressure
                    Metadata = new Dictionary<string, object>
                    {
                        ["Quality"] = _random.NextDouble() > 0.1
                            ? "Good"
                            : "Questionable",
                        ["Location"] = $"Zone-{sensorIndex % 3 + 1}",
                        ["ReadingIndex"] = readingIndex,
                    },
                };

                sensorDataList.Add(sensorData);
            }
        }

        Console.WriteLine($"Generated {sensorDataList.Count} sensor readings from {_sensorIds.Count} sensors");

        return new ListDataPipe<SensorData>(sensorDataList, "SensorDataSource");
    }
}

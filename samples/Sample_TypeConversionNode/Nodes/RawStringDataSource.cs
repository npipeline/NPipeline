using System;
using System.Collections.Generic;
using System.Threading;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_TypeConversionNode.Nodes;

/// <summary>
///     Source node that generates raw string data simulating CSV or log file input.
///     This node demonstrates first stage of type conversion - receiving unstructured string data.
/// </summary>
/// <remarks>
///     This source generates realistic sensor data in string format that would typically come from:
///     - CSV files exported from legacy systems
///     - Log files with structured text data
///     - Text-based APIs returning delimited data
///     - File monitoring systems reading text files
///     The data includes various data quality scenarios to demonstrate robust type conversion.
/// </remarks>
public sealed class RawStringDataSource : SourceNode<RawStringData>
{
    private readonly int _count;
    private readonly TimeSpan _interval;

    /// <summary>
    ///     Initializes a new instance of <see cref="RawStringDataSource" /> class.
    /// </summary>
    /// <param name="count">The number of records to generate.</param>
    /// <param name="interval">The interval between record generation.</param>
    public RawStringDataSource(int count = 20, TimeSpan? interval = null)
    {
        _count = count;
        _interval = interval ?? TimeSpan.FromMilliseconds(200);
    }

    /// <summary>
    ///     Generates raw string data with various quality scenarios.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token to stop generation.</param>
    /// <returns>A data pipe containing raw string data.</returns>
    public override IDataPipe<RawStringData> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        var rawData = new List<RawStringData>();
        var random = new Random(42); // Fixed seed for reproducible results
        var sensorTypes = new[] { "Temperature", "Humidity", "Pressure", "Multi", "Environmental" };
        var statuses = new[] { "Active", "Inactive", "Maintenance", "Error", "Calibration" };

        for (var i = 0; i < _count; i++)
        {
            // Simulate various data quality scenarios
            var isValid = random.NextDouble() > 0.15; // 85% valid data

            var id = isValid
                ? Guid.NewGuid().ToString()
                : $"invalid-id-{i}";

            var timestamp = DateTime.UtcNow.AddMinutes(-random.Next(0, 60)).ToString("yyyy-MM-dd HH:mm:ss");

            // Generate realistic sensor values with potential issues
            var temperature = isValid
                ? (20.0 + random.NextDouble() * 15.0).ToString("F2")
                : random.Next(0, 2) == 0
                    ? "invalid-temp"
                    : "999.99";

            var humidity = isValid
                ? (30.0 + random.NextDouble() * 40.0).ToString("F1")
                : random.Next(0, 2) == 0
                    ? ""
                    : "150.0"; // Empty or out of range

            var pressure = isValid
                ? (980.0 + random.NextDouble() * 50.0).ToString("F1")
                : random.Next(0, 2) == 0
                    ? "N/A"
                    : "500.0";

            var sensorType = sensorTypes[random.Next(sensorTypes.Length)];
            var status = statuses[random.Next(statuses.Length)];

            rawData.Add(new RawStringData(
                id,
                timestamp,
                temperature,
                humidity,
                pressure,
                sensorType,
                status
            ));
        }

        return new ListDataPipe<RawStringData>(rawData, "RawStringDataSource");
    }
}

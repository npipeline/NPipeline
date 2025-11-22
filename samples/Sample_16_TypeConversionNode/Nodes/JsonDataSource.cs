using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_16_TypeConversionNode.Nodes;

/// <summary>
///     Source node that generates JSON string data simulating API or message queue input.
///     This node demonstrates handling structured data in JSON format that needs deserialization.
/// </summary>
/// <remarks>
///     This source generates realistic JSON data that would typically come from:
///     - REST APIs returning JSON responses
///     - Message queues with JSON payloads
///     - IoT devices sending JSON telemetry
///     - WebSocket streams with JSON data
///     - Database query results in JSON format
///     The JSON includes nested structures and various data types to demonstrate complex parsing.
/// </remarks>
public sealed class JsonDataSource : SourceNode<JsonStringData>
{
    // Cache JsonSerializerOptions to avoid creating new instances for each serialization
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = false,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly int _count;
    private readonly TimeSpan _interval;

    /// <summary>
    ///     Initializes a new instance of <see cref="JsonDataSource" /> class.
    /// </summary>
    /// <param name="count">The number of JSON records to generate.</param>
    /// <param name="interval">The interval between record generation.</param>
    public JsonDataSource(int count = 15, TimeSpan? interval = null)
    {
        _count = count;
        _interval = interval ?? TimeSpan.FromMilliseconds(300);
    }

    /// <summary>
    ///     Generates JSON string data with nested structures.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token to stop generation.</param>
    /// <returns>A data pipe containing JSON string data.</returns>
    public override IDataPipe<JsonStringData> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        var jsonData = new List<JsonStringData>();
        var random = new Random(123); // Fixed seed for reproducible results
        var sources = new[] { "API-Server-01", "MQTT-Broker-02", "WebSocket-Gateway", "Database-Export", "IoT-Gateway-03" };

        for (var i = 0; i < _count; i++)
        {
            // Create complex nested JSON structure
            var jsonObj = new
            {
                sensorId = Guid.NewGuid(),
                timestamp = DateTime.UtcNow.AddMinutes(-random.Next(0, 120)),
                readings = new
                {
                    temperature = 20.0 + random.NextDouble() * 15.0,
                    humidity = 30.0 + random.NextDouble() * 40.0,
                    pressure = 980.0 + random.NextDouble() * 50.0,
                },
                metadata = new
                {
                    location = new
                    {
                        latitude = 40.7128 + (random.NextDouble() - 0.5) * 0.1,
                        longitude = -74.0060 + (random.NextDouble() - 0.5) * 0.1,
                        altitude = random.NextDouble() > 0.5
                            ? (double?)random.Next(10, 100)
                            : null,
                    },
                    deviceInfo = new
                    {
                        manufacturer = "SensorCorp",
                        model = $"SC-{random.Next(1000, 9999)}",
                        firmwareVersion = $"{random.Next(1, 5)}.{random.Next(0, 9)}.{random.Next(0, 99)}",
                        installationDate = DateTime.UtcNow.AddDays(-random.Next(30, 365)),
                    },
                    tags = new[]
                    {
                        "environmental", "indoor", "monitored", random.Next(0, 2) == 0
                            ? "critical"
                            : "standard",
                    },
                },
                quality = new
                {
                    score = random.NextDouble(),
                    isValid = random.NextDouble() > 0.1,
                    lastCalibration = DateTime.UtcNow.AddDays(-random.Next(1, 30)),
                    calibrationDue = DateTime.UtcNow.AddDays(random.Next(1, 60)),
                },
                alerts = random.NextDouble() > 0.7
                    ? new[]
                    {
                        new
                        {
                            type = "threshold",
                            severity = random.NextDouble() > 0.5
                                ? "warning"
                                : "error",
                            message = "Temperature exceeds normal range",
                            triggeredAt = DateTime.UtcNow.AddMinutes(-random.Next(0, 60)),
                        },
                    }
                    : Array.Empty<object>(),
            };

            var jsonString = JsonSerializer.Serialize(jsonObj, JsonOptions);

            var source = sources[random.Next(sources.Length)];

            jsonData.Add(new JsonStringData(
                jsonString,
                source,
                DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss")
            ));
        }

        return new ListDataPipe<JsonStringData>(jsonData, "JsonDataSource");
    }
}

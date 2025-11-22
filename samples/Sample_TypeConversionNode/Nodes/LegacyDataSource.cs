using System;
using System.Collections.Generic;
using System.Threading;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_TypeConversionNode.Nodes;

/// <summary>
///     Source node that generates legacy format data simulating old system integration.
///     This node demonstrates handling data from legacy systems with different naming conventions.
/// </summary>
/// <remarks>
///     This source generates realistic legacy data that would typically come from:
///     - Mainframe systems with fixed-width formats
///     - Legacy databases with column naming conventions
///     - Old APIs with non-standard data formats
///     - File imports from legacy applications
///     - Third-party integrations with outdated schemas
///     The data uses uppercase naming and different field structures to demonstrate legacy integration.
/// </remarks>
public sealed class LegacyDataSource : SourceNode<LegacySensorFormat>
{
    private readonly int _count;
    private readonly TimeSpan _interval;

    /// <summary>
    ///     Initializes a new instance of <see cref="LegacyDataSource" /> class.
    /// </summary>
    /// <param name="count">The number of legacy records to generate.</param>
    /// <param name="interval">The interval between record generation.</param>
    public LegacyDataSource(int count = 12, TimeSpan? interval = null)
    {
        _count = count;
        _interval = interval ?? TimeSpan.FromMilliseconds(400);
    }

    /// <summary>
    ///     Generates legacy format data with uppercase naming and different conventions.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token to stop generation.</param>
    /// <returns>A data pipe containing legacy format data.</returns>
    public override IDataPipe<LegacySensorFormat> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        var legacyData = new List<LegacySensorFormat>();
        var random = new Random(456); // Fixed seed for reproducible results
        var sensorCategories = new[] { "CLIMATE", "INDUSTRIAL", "ENVIRONMENTAL", "QUALITY", "SAFETY" };
        var operationalStates = new[] { "ONLINE", "OFFLINE", "MAINTENANCE", "FAULT", "CALIBRATION" };

        for (var i = 0; i < _count; i++)
        {
            // Generate legacy-style data with different conventions
            var sensorId = $"LEGACY-{DateTime.UtcNow:yyyyMMdd}-{random.Next(1000, 9999):D4}";
            var readingTime = DateTime.UtcNow.AddHours(-random.Next(0, 24)).ToString("yyyyMMddHHmmss");

            // Legacy temperature format (sometimes in Fahrenheit, sometimes Celsius)
            var useFahrenheit = random.NextDouble() > 0.5;

            var tempValue = useFahrenheit
                ? (68.0 + random.NextDouble() * 20.0).ToString("F1") // Fahrenheit
                : (20.0 + random.NextDouble() * 10.0).ToString("F1"); // Celsius

            var humidityValue = (40.0 + random.NextDouble() * 30.0).ToString("F0");
            var pressureValue = (1000.0 + random.NextDouble() * 50.0).ToString("F1");

            var sensorCategory = sensorCategories[random.Next(sensorCategories.Length)];
            var operationalState = operationalStates[random.Next(operationalStates.Length)];

            legacyData.Add(new LegacySensorFormat(
                sensorId,
                readingTime,
                tempValue,
                humidityValue,
                pressureValue,
                sensorCategory,
                operationalState
            ));
        }

        return new ListDataPipe<LegacySensorFormat>(legacyData, "LegacyDataSource");
    }
}

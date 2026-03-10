using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_DuckDBConnector.Nodes;

/// <summary>Generates synthetic sensor readings for the DuckDB pipeline sample.</summary>
public sealed class SensorDataSourceNode : SourceNode<SensorReading>
{
    private readonly int _count;

    public SensorDataSourceNode(int count = 1_000)
    {
        _count = count;
    }

    public override IDataStream<SensorReading> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        return new InMemoryDataStream<SensorReading>(GenerateReadings().ToList(), "sensor-data");
    }

    private IEnumerable<SensorReading> GenerateReadings()
    {
        var sensors = new[] { "Sensor-A", "Sensor-B", "Sensor-C", "Sensor-D", "Sensor-E" };
        var regions = new[] { "North", "South", "East", "West" };
        var rng = new Random(42);
        var baseDate = new DateTime(2025, 1, 1);

        for (var i = 1; i <= _count; i++)
        {
            yield return new SensorReading
            {
                Id = i,
                SensorName = sensors[rng.Next(sensors.Length)],
                Temperature = Math.Round(rng.NextDouble() * 40 - 10, 2),
                Humidity = Math.Round(rng.NextDouble() * 100, 2),
                RecordedAt = baseDate.AddMinutes(rng.Next(60 * 24 * 90)),
                Region = regions[rng.Next(regions.Length)],
            };
        }
    }
}

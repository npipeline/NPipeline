using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Nodes;
using NPipeline.Pipeline;
using Sample_DuckDBConnector.Nodes;

namespace Sample_DuckDBConnector;

/// <summary>
///     Demonstrates writing to a local DuckDB database, reading back, and exporting to CSV.
///     Pipeline: SensorDataSourceNode → DuckDBSinkNode (write to table)
///     DuckDBSourceNode (query aggregate) → Console output
///     DuckDBSinkNode.ToFile (export CSV)
/// </summary>
public sealed class DuckDBConnectorPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Phase 1: Write sensor data to DuckDB
        var source = builder.AddSource(new SensorDataSourceNode(), "sensor-source");

        var sink = builder.AddSink(
            new DuckDBSinkNode<SensorReading>(GetDatabasePath(), "sensor_readings",
                new DuckDBConfiguration
                {
                    WriteStrategy = DuckDBWriteStrategy.Appender,
                    AutoCreateTable = true,
                    TruncateBeforeWrite = true,
                }),
            "duckdb-sink");

        builder.Connect(source, sink);
    }

    public static string GetDatabasePath()
    {
        var dir = Path.Combine(Directory.GetCurrentDirectory(), "output");
        Directory.CreateDirectory(dir);
        return Path.Combine(dir, "sensors.duckdb");
    }

    public static string GetCsvExportPath()
    {
        var dir = Path.Combine(Directory.GetCurrentDirectory(), "output");
        Directory.CreateDirectory(dir);
        return Path.Combine(dir, "sensor_summary.csv");
    }
}

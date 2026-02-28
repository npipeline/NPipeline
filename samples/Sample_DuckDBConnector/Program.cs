using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Nodes;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Pipeline;

namespace Sample_DuckDBConnector;

public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: DuckDB Connector ===");
        Console.WriteLine();

        // --- Phase 1: Write data via pipeline ---
        Console.WriteLine("Phase 1: Writing 1,000 sensor readings to DuckDB...");

        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((_, services) => services.AddNPipeline(Assembly.GetExecutingAssembly()))
            .Build();

        await host.Services.RunPipelineAsync<DuckDBConnectorPipeline>();

        var dbPath = DuckDBConnectorPipeline.GetDatabasePath();
        Console.WriteLine($"  Database: {dbPath}");
        Console.WriteLine($"  Size    : {new FileInfo(dbPath).Length:N0} bytes");
        Console.WriteLine();

        // --- Phase 2: Query analytical data ---
        Console.WriteLine("Phase 2: Querying aggregate statistics...");

        // We reuse a SensorStat shape for the aggregate query
        var statsSource = new DuckDBSourceNode<SensorStat>(
            dbPath,
            """
            SELECT region,
                   COUNT(*) AS reading_count,
                   ROUND(AVG(temperature), 2) AS avg_temp,
                   ROUND(AVG(humidity), 2) AS avg_humidity
            FROM sensor_readings
            GROUP BY region
            ORDER BY region
            """);

        Console.WriteLine($"  {"Region",-10} {"Count",8} {"Avg Temp",10} {"Avg Humidity",14}");
        Console.WriteLine($"  {new string('-', 44)}");

        await foreach (var stat in statsSource.Initialize(PipelineContext.Default, CancellationToken.None))
        {
            Console.WriteLine($"  {stat.Region,-10} {stat.ReadingCount,8} {stat.AvgTemp,10:F2} {stat.AvgHumidity,14:F2}");
        }

        Console.WriteLine();

        // --- Phase 3: Export to CSV ---
        Console.WriteLine("Phase 3: Exporting first 10 readings to CSV...");

        var csvPath = DuckDBConnectorPipeline.GetCsvExportPath();

        var exportSource = new DuckDBSourceNode<SensorReading>(
            dbPath,
            "SELECT * FROM sensor_readings ORDER BY id LIMIT 10");

        var exportSink = DuckDBSinkNode<SensorReading>.ToFile(csvPath, new DuckDBConfiguration
        {
            FileExportOptions = new DuckDBFileExportOptions { CsvHeader = true },
        });

        await exportSink.ExecuteAsync(
            exportSource.Initialize(PipelineContext.Default, CancellationToken.None),
            PipelineContext.Default,
            CancellationToken.None);

        Console.WriteLine($"  CSV: {csvPath}");
        Console.WriteLine($"  Size: {new FileInfo(csvPath).Length:N0} bytes");
        Console.WriteLine();
        Console.WriteLine("Sample completed successfully.");
    }
}

/// <summary>
///     Simple record for analytical query results.
/// </summary>
public sealed class SensorStat
{
    public string Region { get; set; } = string.Empty;
    public long ReadingCount { get; set; }
    public double AvgTemp { get; set; }
    public double AvgHumidity { get; set; }
}

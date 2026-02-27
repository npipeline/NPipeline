using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Connectors.DataLake;
using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;

namespace Sample_DataLakeConnector;

public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Data Lake Connector ===");
        Console.WriteLine();

        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((_, services) => services.AddNPipeline(Assembly.GetExecutingAssembly()))
            .Build();

        await host.Services.RunPipelineAsync<DataLakeConnectorPipeline>();

        Console.WriteLine();
        Console.WriteLine("Pipeline completed.");
        Console.WriteLine();

        // Show what was written
        var (tableUri, tablePath) = DataLakeConnectorPipeline.GetTableLocation();
        var resolver = StorageProviderFactory.CreateResolver();
        var provider = StorageProviderFactory.GetProviderOrThrow(resolver, tableUri);

        var manifestReader = new ManifestReader(provider, tableUri);
        var entries = await manifestReader.ReadAllAsync(CancellationToken.None);
        var snapshotIds = await manifestReader.GetSnapshotIdsAsync(CancellationToken.None);

        Console.WriteLine($"Snapshot      : {(snapshotIds.Count > 0 ? snapshotIds[0] : "(none)")}");
        Console.WriteLine($"Files written : {entries.Count}");
        Console.WriteLine($"Total rows    : {entries.Sum(e => e.RowCount)}");
        Console.WriteLine();

        // Read back first few records
        Console.WriteLine("First 5 records (read back via DataLakeTableSourceNode):");
        var sourceNode = new DataLakeTableSourceNode<SalesRecord>(provider, tableUri);
        var count = 0;
        await foreach (var record in sourceNode.Initialize(PipelineContext.Default, CancellationToken.None))
        {
            if (count++ >= 5) break;
            Console.WriteLine($"  [{record.Id,4}] {record.Product,-15} {record.Amount,8:C}  {record.EventDate:yyyy-MM-dd}  {record.Region}");
        }

        Console.WriteLine();
        Console.WriteLine("Partition structure:");
        foreach (var dir in Directory.GetDirectories(tablePath).OrderBy(d => d))
        {
            var name = Path.GetFileName(dir);
            if (name == "_manifest") continue;
            Console.WriteLine($"  {name}/");
            foreach (var sub in Directory.GetDirectories(dir).OrderBy(d => d))
                Console.WriteLine($"    {Path.GetFileName(sub)}/");
        }
    }
}

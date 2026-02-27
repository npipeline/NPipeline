using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Connectors.Parquet;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;

namespace Sample_ParquetConnector;

public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Parquet Connector ===");
        Console.WriteLine();

        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((_, services) => services.AddNPipeline(Assembly.GetExecutingAssembly()))
            .Build();

        await host.Services.RunPipelineAsync<ParquetConnectorPipeline>();

        Console.WriteLine();
        Console.WriteLine("Pipeline completed.");
        Console.WriteLine();

        // Show file info and read back first few records
        var outputPath = ParquetConnectorPipeline.GetOutputPath();
        var fileInfo = new FileInfo(outputPath);
        Console.WriteLine($"Output : {outputPath}");
        Console.WriteLine($"Size   : {fileInfo.Length:N0} bytes");
        Console.WriteLine();

        Console.WriteLine("First 5 records (read back from Parquet):");
        var sourceNode = new ParquetSourceNode<SalesRecord>(StorageUri.FromFilePath(outputPath));
        var count = 0;
        await foreach (var record in sourceNode.Initialize(PipelineContext.Default, CancellationToken.None))
        {
            if (count++ >= 5) break;
            Console.WriteLine($"  [{record.Id,4}] {record.Product,-15} {record.Amount,8:C}  {record.TransactionDate:yyyy-MM-dd}  {record.Region}");
        }
    }
}

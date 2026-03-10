using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_CosmosDbConnector;

/// <summary>
///     Entry point for the Cosmos DB Connector sample.
///     Demonstrates reading and writing data against the NoSQL (SQL) API of Azure Cosmos DB.
/// </summary>
public sealed class Program
{
    // Well-known default key for the Azure Cosmos DB Emulator
    private const string EmulatorConnectionString =
        "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Cosmos DB Connector ===");
        Console.WriteLine();

        var connectionString = GetConnectionString(args);

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) => { services.AddNPipeline(Assembly.GetExecutingAssembly()); })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();

            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(CosmosDbConnectorPipeline.GetDescription());

            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            var pipeline = new CosmosDbConnectorPipeline(connectionString);
            await pipeline.ConsumeAsync(host.Services, CancellationToken.None);

            Console.WriteLine("Pipeline execution completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing pipeline: {ex.Message}");
            Console.WriteLine();
            Console.WriteLine("Full error details:");
            Console.WriteLine(ex.ToString());
            Console.WriteLine();
            Console.WriteLine("If the emulator is not running, start it with:");
            Console.WriteLine("  docker-compose up -d");
            Console.WriteLine();
            Console.WriteLine("The emulator takes ~30 seconds to become healthy on first run.");
            Environment.ExitCode = 1;
        }

        Console.WriteLine();
        Console.WriteLine("Press any key to exit...");

        try
        {
            _ = Console.ReadKey();
        }
        catch (InvalidOperationException)
        {
            await Task.Delay(1000);
        }
    }

    /// <summary>
    ///     Uses the connection string from the first command-line argument, or falls back to
    ///     the well-known emulator default.
    /// </summary>
    private static string GetConnectionString(string[] args)
    {
        if (args.Length > 0)
        {
            Console.WriteLine("Using connection string from command-line argument.");
            Console.WriteLine();
            return args[0];
        }

        Console.WriteLine("No connection string provided — using Azure Cosmos DB Emulator defaults:");
        Console.WriteLine("  Endpoint : https://localhost:8081/");
        Console.WriteLine();
        Console.WriteLine("To use a real account, run:");
        Console.WriteLine("  dotnet run \"AccountEndpoint=https://<account>.documents.azure.com:443/;AccountKey=<key>;\"");
        Console.WriteLine();

        return EmulatorConnectionString;
    }
}

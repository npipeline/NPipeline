using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_MongoDbConnector;

/// <summary>
///     Entry point for the MongoDB Connector sample.
///     Demonstrates reading and writing data against MongoDB using NPipeline.
/// </summary>
public sealed class Program
{
    // Default connection string for local MongoDB via Docker Compose
    private const string DefaultConnectionString = "mongodb://localhost:27017/?replicaSet=rs0";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: MongoDB Connector ===");
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
            Console.WriteLine(MongoDbConnectorPipeline.GetDescription());

            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            var pipeline = new MongoDbConnectorPipeline(connectionString);
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
            Console.WriteLine("If MongoDB is not running, start it with:");
            Console.WriteLine("  docker-compose up -d");
            Console.WriteLine();
            Console.WriteLine("Wait for MongoDB to initialize (approximately 10-15 seconds) before running again.");
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
            // Running in non-interactive mode
            await Task.Delay(1000);
        }
    }

    /// <summary>
    ///     Uses the connection string from the first command-line argument, or falls back to
    ///     the default local MongoDB connection string.
    /// </summary>
    private static string GetConnectionString(string[] args)
    {
        if (args.Length > 0)
        {
            Console.WriteLine("Using connection string from command-line argument.");
            Console.WriteLine();
            return args[0];
        }

        Console.WriteLine("No connection string provided — using local MongoDB defaults:");
        Console.WriteLine("  Connection: mongodb://localhost:27017/?replicaSet=rs0");
        Console.WriteLine();
        Console.WriteLine("To use a remote MongoDB instance, run:");
        Console.WriteLine("  dotnet run \"mongodb://username:password@host:27017/?authSource=admin\"");
        Console.WriteLine();

        return DefaultConnectionString;
    }
}

using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_MySQLConnector;

/// <summary>
///     Entry point for MySQL Connector sample demonstrating various MySQL connector features.
///     This sample shows how to read from and write to MySQL using NPipeline's MySQL connector.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: MySQL Connector ===");
        Console.WriteLine();

        // Get connection string from command line or use default
        var connectionString = GetConnectionString(args);

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    // Add NPipeline services with assembly scanning
                    _ = services.AddNPipeline(Assembly.GetExecutingAssembly());
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();

            // Display pipeline description
            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(MySqlConnectorPipeline.GetDescription());
            Console.WriteLine();

            // Display connection information
            Console.WriteLine("Connection Information:");
            Console.WriteLine($"  - Server: {GetServerFromConnectionString(connectionString)}");
            Console.WriteLine($"  - Database: {GetDatabaseFromConnectionString(connectionString)}");
            Console.WriteLine();

            // Execute pipeline
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            var pipeline = new MySqlConnectorPipeline(connectionString);
            await pipeline.ExecuteAsync(null!, CancellationToken.None);

            Console.WriteLine();
            Console.WriteLine("Pipeline execution completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing pipeline: {ex.Message}");
            Console.WriteLine();
            Console.WriteLine("Full error details:");
            Console.WriteLine(ex.ToString());
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
            // Console input not available (e.g., when running in CI/CD)
            await Task.Delay(1000);
        }
    }

    /// <summary>
    ///     Gets the connection string from command line arguments or uses a default for local development.
    /// </summary>
    private static string GetConnectionString(string[] args)
    {
        // Check for connection string argument
        for (var i = 0; i < args.Length - 1; i++)
        {
            if (args[i] is "--connection-string" or "-c")
                return args[i + 1];
        }

        // Default connection string for local MySQL / Docker
        const string defaultConnectionString =
            "Server=localhost;Port=3306;Database=npipeline_sample;User=root;Password=root;AllowPublicKeyRetrieval=true;";

        Console.WriteLine("No connection string provided. Using default:");
        Console.WriteLine($"  {defaultConnectionString}");
        Console.WriteLine();
        Console.WriteLine("To provide a custom connection string, run:");
        Console.WriteLine("  dotnet run -- --connection-string \"Server=...;Database=...;\"");
        Console.WriteLine();

        return defaultConnectionString;
    }

    private static string GetServerFromConnectionString(string connectionString)
    {
        foreach (var part in connectionString.Split(';'))
        {
            var kv = part.Trim().Split('=', 2);
            if (kv.Length == 2 && kv[0].Trim().Equals("Server", StringComparison.OrdinalIgnoreCase))
                return kv[1].Trim();
        }

        return "unknown";
    }

    private static string GetDatabaseFromConnectionString(string connectionString)
    {
        foreach (var part in connectionString.Split(';'))
        {
            var kv = part.Trim().Split('=', 2);
            if (kv.Length == 2 && kv[0].Trim().Equals("Database", StringComparison.OrdinalIgnoreCase))
                return kv[1].Trim();
        }

        return "unknown";
    }
}


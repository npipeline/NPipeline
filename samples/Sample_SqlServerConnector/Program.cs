using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_SqlServerConnector;

/// <summary>
///     Entry point for SQL Server Connector sample demonstrating various SQL Server connector features.
///     This sample shows how to read from and write to SQL Server using NPipeline's SQL Server connector.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: SQL Server Connector ===");
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
            Console.WriteLine(SqlServerConnectorPipeline.GetDescription());
            Console.WriteLine();

            // Display connection information
            Console.WriteLine("Connection Information:");
            Console.WriteLine($"  - Server: {GetServerFromConnectionString(connectionString)}");
            Console.WriteLine($"  - Database: {GetDatabaseFromConnectionString(connectionString)}");
            Console.WriteLine();

            // Execute pipeline
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            var pipeline = new SqlServerConnectorPipeline(connectionString);
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
            // Console input not available (e.g., when running in CI/CD or redirected output)
            // Exit automatically after a short delay
            await Task.Delay(1000);
        }
    }

    /// <summary>
    ///     Gets the connection string from command line arguments or uses a default for LocalDB.
    /// </summary>
    private static string GetConnectionString(string[] args)
    {
        if (args.Length > 0)
        {
            Console.WriteLine("Using connection string from command line argument.");
            Console.WriteLine();
            return args[0];
        }

        // Default connection string for LocalDB
        var defaultConnectionString =
            @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=NPipelineSamples;Integrated Security=True;MultipleActiveResultSets=True;Connect Timeout=30;";

        Console.WriteLine("No connection string provided. Using default LocalDB connection:");
        Console.WriteLine($"  {defaultConnectionString}");
        Console.WriteLine();
        Console.WriteLine("To use a different connection, run:");
        Console.WriteLine("  dotnet run --project samples/Sample_SqlServerConnector \"<connection-string>\"");
        Console.WriteLine();
        Console.WriteLine("Example connection strings:");
        Console.WriteLine("  LocalDB: \"Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=MyDb;Integrated Security=True;\"");
        Console.WriteLine("  SQL Server: \"Server=localhost;Database=MyDb;User Id=sa;Password=yourpassword;\"");
        Console.WriteLine("  Azure SQL: \"Server=tcp:myserver.database.windows.net,1433;Database=mydb;User Id=myuser;Password=mypassword;Encrypt=True;\"");
        Console.WriteLine();

        return defaultConnectionString;
    }

    /// <summary>
    ///     Extracts the server name from a connection string.
    /// </summary>
    private static string GetServerFromConnectionString(string connectionString)
    {
        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);

        foreach (var part in parts)
        {
            var keyValue = part.Split('=', 2);

            if (keyValue.Length == 2 && keyValue[0].Trim().Equals("Data Source", StringComparison.OrdinalIgnoreCase))
                return keyValue[1].Trim();

            if (keyValue.Length == 2 && keyValue[0].Trim().Equals("Server", StringComparison.OrdinalIgnoreCase))
                return keyValue[1].Trim();
        }

        return "Unknown";
    }

    /// <summary>
    ///     Extracts the database name from a connection string.
    /// </summary>
    private static string GetDatabaseFromConnectionString(string connectionString)
    {
        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);

        foreach (var part in parts)
        {
            var keyValue = part.Split('=', 2);

            if (keyValue.Length == 2 && keyValue[0].Trim().Equals("Initial Catalog", StringComparison.OrdinalIgnoreCase))
                return keyValue[1].Trim();

            if (keyValue.Length == 2 && keyValue[0].Trim().Equals("Database", StringComparison.OrdinalIgnoreCase))
                return keyValue[1].Trim();
        }

        return "Unknown";
    }
}

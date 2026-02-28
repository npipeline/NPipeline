using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_SnowflakeConnector;

public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Snowflake Connector ===");
        Console.WriteLine();
        Console.WriteLine("This sample demonstrates reading from and writing to Snowflake using NPipeline.");
        Console.WriteLine();

        var connectionString = GetConnectionString(args);

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Console.WriteLine("Error: No connection string provided.");
            Console.WriteLine("Set NPIPELINE_SNOWFLAKE_CONNECTION_STRING or pass it as a command line argument.");
            Environment.ExitCode = 1;
            return;
        }

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) => { _ = services.AddNPipeline(Assembly.GetExecutingAssembly()); })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();

            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(SnowflakeConnectorPipeline.GetDescription());
            Console.WriteLine();

            Console.WriteLine("Connection Information:");
            Console.WriteLine($"  - Account: {GetAccountFromConnectionString(connectionString)}");
            Console.WriteLine($"  - Database: {GetDatabaseFromConnectionString(connectionString)}");
            Console.WriteLine();

            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            var pipeline = new SnowflakeConnectorPipeline(connectionString);
            await pipeline.ExecuteAsync(null!, CancellationToken.None);

            Console.WriteLine();
            Console.WriteLine("Sample completed successfully!");
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
            await Task.Delay(1000);
        }
    }

    private static string GetConnectionString(string[] args)
    {
        if (args.Length > 0)
        {
            Console.WriteLine("Using connection string from command line argument.");
            Console.WriteLine();
            return args[0];
        }

        var envConnectionString = Environment.GetEnvironmentVariable("NPIPELINE_SNOWFLAKE_CONNECTION_STRING");

        if (!string.IsNullOrWhiteSpace(envConnectionString))
        {
            Console.WriteLine("Using connection string from NPIPELINE_SNOWFLAKE_CONNECTION_STRING environment variable.");
            Console.WriteLine();
            return envConnectionString;
        }

        Console.WriteLine("No connection string provided.");
        Console.WriteLine();
        Console.WriteLine("To run this sample, provide a Snowflake connection string by either:");
        Console.WriteLine("  1. Setting the NPIPELINE_SNOWFLAKE_CONNECTION_STRING environment variable");
        Console.WriteLine("  2. Passing it as a command line argument");
        Console.WriteLine();
        Console.WriteLine("Example connection strings:");

        Console.WriteLine(
            "  Password auth: \"account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;password=mypassword;db=mydb;schema=PUBLIC;warehouse=COMPUTE_WH\"");

        Console.WriteLine(
            "  Key-pair auth: \"account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;authenticator=snowflake_jwt;private_key_file=/path/to/key.p8;db=mydb;schema=PUBLIC;warehouse=COMPUTE_WH\"");

        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run --project samples/Sample_SnowflakeConnector \"<connection-string>\"");
        Console.WriteLine();

        return string.Empty;
    }

    private static string GetAccountFromConnectionString(string connectionString)
    {
        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);

        foreach (var part in parts)
        {
            var keyValue = part.Split('=', 2);

            if (keyValue.Length == 2 && keyValue[0].Trim().Equals("account", StringComparison.OrdinalIgnoreCase))
                return keyValue[1].Trim();
        }

        return "Unknown";
    }

    private static string GetDatabaseFromConnectionString(string connectionString)
    {
        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);

        foreach (var part in parts)
        {
            var keyValue = part.Split('=', 2);

            if (keyValue.Length == 2 && keyValue[0].Trim().Equals("db", StringComparison.OrdinalIgnoreCase))
                return keyValue[1].Trim();
        }

        return "Unknown";
    }
}

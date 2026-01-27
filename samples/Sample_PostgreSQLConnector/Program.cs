using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_PostgreSQLConnector;

/// <summary>
/// Entry point for the PostgreSQL Connector sample demonstrating PostgreSQL data processing with NPipeline.
/// This sample shows how to read from PostgreSQL tables, transform data, and write to PostgreSQL tables.
/// </summary>
public sealed class Program
{
    // Default connection string - can be overridden via environment variable or command line
    private const string DefaultConnectionString = "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=NPipelineSamples";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: PostgreSQL Connector ===");
        Console.WriteLine();

        try
        {
            // Get connection string from environment variable or use default
            var connectionString = Environment.GetEnvironmentVariable("NPipeline_PostgreSQL_ConnectionString") ?? DefaultConnectionString;

            Console.WriteLine("Connection Configuration:");
            Console.WriteLine($"  Connection String: {MaskConnectionString(connectionString)}");
            Console.WriteLine();

            // Test database connection
            Console.WriteLine("Testing database connection...");
            if (!await TestConnectionAsync(connectionString))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Failed to connect to PostgreSQL database.");
                Console.WriteLine();
                Console.WriteLine("Please ensure:");
                Console.WriteLine("  1. PostgreSQL is running on localhost:5432");
                Console.WriteLine("  2. A database named 'NPipelineSamples' exists");
                Console.WriteLine("  3. The user 'postgres' with password 'postgres' has access");
                Console.WriteLine();
                Console.WriteLine("Or set the NPipeline_PostgreSQL_ConnectionString environment variable.");
                Console.ResetColor();
                Environment.ExitCode = 1;
                return;
            }

            Console.WriteLine("Database connection successful!");
            Console.WriteLine();

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
            Console.WriteLine(PostgreSQLConnectorPipeline.GetDescription());
            Console.WriteLine();

            // Set up pipeline parameters
            var pipelineParameters = new Dictionary<string, object>
            {
                ["ConnectionString"] = connectionString
            };

            Console.WriteLine("Pipeline Parameters:");
            Console.WriteLine($"  Connection String: {MaskConnectionString(connectionString)}");
            Console.WriteLine();

            // Execute the pipeline using the DI container
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            var pipeline = new PostgreSQLConnectorPipeline(connectionString);
            await pipeline.ExecuteAsync(new NPipeline.Pipeline.PipelineContext());

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
    }

    /// <summary>
    /// Tests the database connection.
    /// </summary>
    /// <param name="connectionString">The connection string to test.</param>
    /// <returns>True if connection succeeds, false otherwise.</returns>
    private static async Task<bool> TestConnectionAsync(string connectionString)
    {
        try
        {
            using var connection = new Npgsql.NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            await using var cmd = new Npgsql.NpgsqlCommand("SELECT 1", connection);
            var result = await cmd.ExecuteScalarAsync();
            return result != null && Convert.ToInt32(result) == 1;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Masks sensitive information in connection string for display.
    /// </summary>
    /// <param name="connectionString">The connection string to mask.</param>
    /// <returns>A masked connection string.</returns>
    private static string MaskConnectionString(string connectionString)
    {
        if (string.IsNullOrEmpty(connectionString))
            return "(empty)";

        var parts = connectionString.Split(';');
        var maskedParts = new List<string>();

        foreach (var part in parts)
        {
            if (string.IsNullOrWhiteSpace(part))
                continue;

            var keyValue = part.Split('=');
            if (keyValue.Length == 2)
            {
                var key = keyValue[0].Trim();
                var value = keyValue[1].Trim();

                // Mask password
                if (key.Equals("Password", StringComparison.OrdinalIgnoreCase))
                {
                    maskedParts.Add($"{key}=****");
                }
                else
                {
                    maskedParts.Add($"{key}={value}");
                }
            }
            else
            {
                maskedParts.Add(part);
            }
        }

        return string.Join("; ", maskedParts);
    }
}

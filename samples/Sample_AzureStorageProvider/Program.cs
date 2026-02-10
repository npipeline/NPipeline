// See https://aka.ms/new-console-template for more information

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Azure;

namespace Sample_AzureStorageProvider;

/// <summary>
///     Entry point for the Azure Blob Storage Provider sample application.
///     Demonstrates comprehensive usage of the Azure Blob Storage Provider with various scenarios.
/// </summary>
public sealed class Program
{
    /// <summary>
    ///     Entry point for the Azure Blob Storage Provider sample application.
    ///     Demonstrates comprehensive usage of the Azure Blob Storage Provider with various scenarios.
    /// </summary>
    /// <param name="_">Command line arguments (unused).</param>
    public static async Task Main(string[] _)
    {
        Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                                                                ║");
        Console.WriteLine("║   NPipeline Azure Blob Storage Provider Sample                ║");
        Console.WriteLine("║                                                                ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        try
        {
            // Build configuration from multiple sources
            var configuration = BuildConfiguration();

            // Build service provider with DI
            var serviceProvider = BuildServiceProvider(configuration);

            // Display configuration information
            DisplayConfigurationInfo(configuration);

            // Create and run the demo
            var demo = serviceProvider.GetRequiredService<AzureStorageProviderDemo>();
            await demo.RunAllDemosAsync();
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine();
            Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║   ERROR                                                        ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();
            Console.WriteLine($"An error occurred while running the sample:");
            Console.WriteLine($"  Message: {ex.Message}");
            Console.WriteLine();
            Console.WriteLine("Full error details:");
            Console.WriteLine(ex.ToString());
            Console.ResetColor();
            Environment.ExitCode = 1;
        }
        finally
        {
            Console.WriteLine();
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }
    }

    /// <summary>
    ///     Builds the configuration from multiple sources in priority order.
    /// </summary>
    /// <returns>The configured <see cref="IConfiguration" /> instance.</returns>
    private static IConfiguration BuildConfiguration()
    {
        return new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();
    }

    /// <summary>
    ///     Builds the service provider with dependency injection configured.
    /// </summary>
    /// <param name="configuration">The configuration instance.</param>
    /// <returns>The configured <see cref="IServiceProvider" /> instance.</returns>
    private static IServiceProvider BuildServiceProvider(IConfiguration configuration)
    {
        var services = new ServiceCollection();

        // Add configuration
        _ = services.AddSingleton(configuration);

        // Add Azure Blob Storage Provider with configuration
        _ = services.AddAzureBlobStorageProvider(options =>
        {
            // Try to get connection string from configuration
            var connectionString = configuration["AzureStorage:DefaultConnectionString"]
                ?? configuration["AZURE_STORAGE_CONNECTION_STRING"]
                ?? "UseDevelopmentStorage=true";

            options.DefaultConnectionString = connectionString;

            // Try to get service URL from configuration (for Azurite)
            var serviceUrl = configuration["AzureStorage:ServiceUrl"];
            if (!string.IsNullOrEmpty(serviceUrl))
            {
                options.ServiceUrl = new Uri(serviceUrl);
            }

            // Configure upload options
            options.BlockBlobUploadThresholdBytes = 64 * 1024 * 1024; // 64MB
            options.UploadMaximumConcurrency = 4;
            options.UploadMaximumTransferSizeBytes = 4 * 1024 * 1024; // 4MB

            // Enable default credential chain for production scenarios
            options.UseDefaultCredentialChain = true;
        });

        // Register the demo class
        _ = services.AddSingleton<AzureStorageProviderDemo>();

        return services.BuildServiceProvider();
    }

    /// <summary>
    ///     Displays information about the current configuration.
    /// </summary>
    /// <param name="configuration">The configuration instance.</param>
    private static void DisplayConfigurationInfo(IConfiguration configuration)
    {
        Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║   Configuration Information                                    ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        var connectionString = configuration["AzureStorage:DefaultConnectionString"]
            ?? configuration["AZURE_STORAGE_CONNECTION_STRING"]
            ?? "UseDevelopmentStorage=true";

        var serviceUrl = configuration["AzureStorage:ServiceUrl"];

        Console.WriteLine("  Azure Storage Configuration:");
        Console.WriteLine($"    Connection String: {MaskConnectionString(connectionString)}");

        if (!string.IsNullOrEmpty(serviceUrl))
        {
            Console.WriteLine($"    Service URL: {serviceUrl}");
        }
        else
        {
            Console.WriteLine($"    Service URL: Default (Azure Blob Storage endpoint)");
        }

        Console.WriteLine();
        Console.WriteLine("  Upload Configuration:");
        Console.WriteLine($"    Block Blob Threshold: 64 MB");
        Console.WriteLine($"    Max Concurrency: 4");
        Console.WriteLine($"    Max Transfer Size: 4 MB");
        Console.WriteLine();

        // Check if Azurite is being used
        if (connectionString.Contains("UseDevelopmentStorage", StringComparison.OrdinalIgnoreCase) ||
            connectionString.Contains("127.0.0.1", StringComparison.OrdinalIgnoreCase) ||
            connectionString.Contains("localhost", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine("  Note: Using Azurite emulator for local development.");
            Console.WriteLine("  Make sure Azurite is running:");
            Console.WriteLine("    - Install: npm install -g azurite");
            Console.WriteLine("    - Run: azurite");
            Console.WriteLine("    - Or use Docker: docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite");
            Console.WriteLine();
        }
        else
        {
            Console.WriteLine("  Note: Using Azure Storage account.");
            Console.WriteLine("  Ensure you have proper credentials configured.");
            Console.WriteLine();
        }
    }

    /// <summary>
    ///     Masks sensitive information in connection strings for display purposes.
    /// </summary>
    /// <param name="connectionString">The connection string to mask.</param>
    /// <returns>A masked version of the connection string.</returns>
    private static string MaskConnectionString(string connectionString)
    {
        if (string.IsNullOrEmpty(connectionString))
        {
            return "(empty)";
        }

        // For Azurite development storage, show as-is
        if (connectionString.Equals("UseDevelopmentStorage=true", StringComparison.OrdinalIgnoreCase))
        {
            return "UseDevelopmentStorage=true";
        }

        // Mask account keys in connection strings
        var masked = connectionString;
        var keyIndex = masked.IndexOf("AccountKey=", StringComparison.OrdinalIgnoreCase);
        if (keyIndex >= 0)
        {
            var keyStart = keyIndex + "AccountKey=".Length;
            var semicolonIndex = masked.IndexOf(';', keyStart);
            var keyEnd = semicolonIndex >= 0 ? semicolonIndex : masked.Length;

            masked = string.Concat(masked.AsSpan(0, keyStart), "*****", masked.AsSpan(keyEnd));
        }

        // Mask SAS tokens
        var sasIndex = masked.IndexOf("?sv=", StringComparison.OrdinalIgnoreCase);
        if (sasIndex >= 0)
        {
            masked = string.Concat(masked.AsSpan(0, sasIndex), "?sv=*****");
        }

        return masked;
    }
}

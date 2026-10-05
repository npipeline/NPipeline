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
    // The Azurite emulator publishes these development values, so they are not secrets.
    private const string AzuriteAccountName = "devstoreaccount1";

    private const string AzuriteAccountKey =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private const string AzuriteServiceUrl = "http://127.0.0.1:10000/devstoreaccount1";

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
            Console.WriteLine("An error occurred while running the sample:");
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
    private static IConfiguration BuildConfiguration() =>
        new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", true, true)
            .AddEnvironmentVariables()
            .Build();

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
            // Credentials always come from the options, never from the URI.
            var connectionString = configuration["AzureStorage:DefaultConnectionString"]
                                   ?? configuration["AZURE_STORAGE_CONNECTION_STRING"];

            if (!string.IsNullOrEmpty(connectionString))
            {
                // A connection string names its own endpoint, so do not also set ServiceUrl.
                options.DefaultConnectionString = connectionString;
            }
            else
            {
                // Default to the Azurite emulator's published development account.
                options.AccountName = configuration["AzureStorage:AccountName"] ?? AzuriteAccountName;
                options.DefaultAccountKey = configuration["AzureStorage:AccountKey"] ?? AzuriteAccountKey;
                options.ServiceUrl = new Uri(configuration["AzureStorage:ServiceUrl"] ?? AzuriteServiceUrl);
            }

            // Configure upload options: blocks upload while the demo writes, with no local temporary file.
            options.PartSizeBytes = 8 * 1024 * 1024; // 8 MiB blocks
            options.MaxConcurrency = 4;

            // The demo writes to containers that may not exist yet, so let the provider create them.
            options.CreateContainerIfMissing = true;

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
                               ?? configuration["AZURE_STORAGE_CONNECTION_STRING"];

        var serviceUrl = configuration["AzureStorage:ServiceUrl"] ?? (string.IsNullOrEmpty(connectionString) ? AzuriteServiceUrl : null);

        Console.WriteLine("  Azure Storage Configuration:");

        if (!string.IsNullOrEmpty(connectionString))
        {
            Console.WriteLine($"    Connection String: {MaskConnectionString(connectionString)}");
        }
        else
        {
            Console.WriteLine($"    Account Name: {configuration["AzureStorage:AccountName"] ?? AzuriteAccountName}");
            Console.WriteLine("    Account Key: ***");
        }

        if (!string.IsNullOrEmpty(serviceUrl) && string.IsNullOrEmpty(connectionString))
            Console.WriteLine($"    Service URL: {serviceUrl}");
        else
            Console.WriteLine("    Service URL: Default (Azure Blob Storage endpoint)");

        Console.WriteLine();
        Console.WriteLine("  Upload Configuration:");
        Console.WriteLine("    Block Size (PartSizeBytes): 8 MiB");
        Console.WriteLine("    Max Concurrency: 4");
        Console.WriteLine("    Create Container If Missing: true");
        Console.WriteLine();

        // Check if Azurite is being used
        if (string.IsNullOrEmpty(connectionString) ||
            connectionString.Contains("UseDevelopmentStorage", StringComparison.OrdinalIgnoreCase) ||
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
            return "(empty)";

        // For Azurite development storage, show as-is
        if (connectionString.Equals("UseDevelopmentStorage=true", StringComparison.OrdinalIgnoreCase))
            return "UseDevelopmentStorage=true";

        // Mask account keys in connection strings
        var masked = connectionString;
        var keyIndex = masked.IndexOf("AccountKey=", StringComparison.OrdinalIgnoreCase);

        if (keyIndex >= 0)
        {
            var keyStart = keyIndex + "AccountKey=".Length;
            var semicolonIndex = masked.IndexOf(';', keyStart);

            var keyEnd = semicolonIndex >= 0
                ? semicolonIndex
                : masked.Length;

            masked = string.Concat(masked.AsSpan(0, keyStart), "*****", masked.AsSpan(keyEnd));
        }

        // Mask SAS tokens
        var sasIndex = masked.IndexOf("?sv=", StringComparison.OrdinalIgnoreCase);

        if (sasIndex >= 0)
            masked = string.Concat(masked.AsSpan(0, sasIndex), "?sv=*****");

        return masked;
    }
}

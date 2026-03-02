using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.S3.Compatible;

namespace Sample_S3CompatibleStorageProvider;

/// <summary>
///     Entry point for S3-Compatible Storage Provider sample demonstrating usage with non-AWS
///     S3-compatible services such as MinIO, DigitalOcean Spaces, Cloudflare R2, and LocalStack.
/// </summary>
public sealed class Program
{
    /// <summary>
    ///     Your S3-compatible access key.
    ///     MinIO default: minioadmin
    /// </summary>
    private const string AccessKey = "minioadmin";

    /// <summary>
    ///     Your S3-compatible secret key.
    ///     MinIO default: minioadmin
    /// </summary>
    private const string SecretKey = "minioadmin";

    /// <summary>
    ///     The bucket name to use for all examples.
    ///     The bucket must exist on your S3-compatible service before running.
    /// </summary>
    private const string BucketName = "your-bucket-name-here";

    /// <summary>
    ///     Signing region used for request authentication.
    ///     Most providers accept "us-east-1". Cloudflare R2 requires "auto".
    /// </summary>
    private const string SigningRegion = "us-east-1";

    // ========================================================================
    // CONFIGURATION — Replace these values with your own endpoint and credentials
    // ========================================================================

    /// <summary>
    ///     The base URL of your S3-compatible service.
    ///     MinIO default: http://localhost:9000
    ///     LocalStack default: http://localhost:4566
    /// </summary>
    private static readonly Uri ServiceUrl = new("http://localhost:9000");

    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: S3-Compatible Storage Provider ===");
        Console.WriteLine();

        // Check if user has configured their credentials
        if (BucketName == "your-bucket-name-here")
        {
            Console.WriteLine("⚠️  WARNING: You need to configure your S3-compatible service settings!");
            Console.WriteLine();
            Console.WriteLine("Please update the following constants in Program.cs:");
            Console.WriteLine("  - ServiceUrl:    The base URL of your S3-compatible service");
            Console.WriteLine("  - AccessKey:     Your access key");
            Console.WriteLine("  - SecretKey:     Your secret key");
            Console.WriteLine("  - BucketName:    Your bucket name");
            Console.WriteLine("  - SigningRegion: Signing region (default: us-east-1)");
            Console.WriteLine();
            Console.WriteLine("Common configurations:");
            Console.WriteLine("  MinIO:        http://localhost:9000, minioadmin / minioadmin");
            Console.WriteLine("  LocalStack:   http://localhost:4566, test / test");
            Console.WriteLine("  DO Spaces:    https://<region>.digitaloceanspaces.com");
            Console.WriteLine("  Cloudflare R2: https://<account-id>.r2.cloudflarestorage.com, region=auto");
            Console.WriteLine();
            Console.WriteLine("Press any key to continue with demo mode (examples will be shown but not executed)...");
            Console.ReadKey();
            Console.WriteLine();
        }

        try
        {
            await Example1_BasicReadFromStorage();
            await Example2_BasicWriteToStorage();
            await Example3_ListObjects();
            await Example4_CheckFileExistence();
            await Example5_GetFileMetadata();
            await Example6_UsingDependencyInjection();
            Example7_ProviderConfigurations();

            Console.WriteLine();
            Console.WriteLine("=== All examples completed successfully! ===");
        }
        catch (Exception ex)
        {
            Console.WriteLine();
            Console.WriteLine($"Error executing examples: {ex.Message}");
            Console.WriteLine();
            Console.WriteLine("Full error details:");
            Console.WriteLine(ex.ToString());
            Environment.ExitCode = 1;
        }

        Console.WriteLine();
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }

    // ========================================================================
    // EXAMPLE 1: Basic Read
    // ========================================================================

    private static async Task Example1_BasicReadFromStorage()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 1: Basic Read");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = ServiceUrl,
            AccessKey = AccessKey,
            SecretKey = SecretKey,
            SigningRegion = SigningRegion,
        };

        var factory = new S3CompatibleClientFactory(options);
        var provider = new S3CompatibleStorageProvider(factory, options);

        var fileUri = StorageUri.Parse($"s3://{BucketName}/data/sample.csv");

        Console.WriteLine($"Reading file: {fileUri}");
        Console.WriteLine();

        try
        {
            await using var stream = await provider.OpenReadAsync(fileUri);
            using var reader = new StreamReader(stream);
            var content = await reader.ReadToEndAsync();

            Console.WriteLine("File contents:");
            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine(content);
            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine();
            Console.WriteLine("✓ Successfully read file!");
        }
        catch (UnauthorizedAccessException ex)
        {
            Console.WriteLine($"✗ Access denied: {ex.Message}");
        }
        catch (FileNotFoundException ex)
        {
            Console.WriteLine($"✗ File not found: {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Error reading file: {ex.Message}");
        }

        Console.WriteLine();
    }

    // ========================================================================
    // EXAMPLE 2: Basic Write
    // ========================================================================

    private static async Task Example2_BasicWriteToStorage()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 2: Basic Write");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = ServiceUrl,
            AccessKey = AccessKey,
            SecretKey = SecretKey,
            SigningRegion = SigningRegion,
        };

        var factory = new S3CompatibleClientFactory(options);
        var provider = new S3CompatibleStorageProvider(factory, options);

        var fileUri = StorageUri.Parse($"s3://{BucketName}/data/output/sample-{DateTime.UtcNow:yyyyMMdd-HHmmss}.txt");

        Console.WriteLine($"Writing file: {fileUri}");
        Console.WriteLine();

        try
        {
            var sampleData = $"Sample data written at {DateTime.UtcNow:O}\n" +
                             $"Written by the NPipeline S3-Compatible storage provider sample.\n";

            await using var stream = await provider.OpenWriteAsync(fileUri);
            await using var writer = new StreamWriter(stream);
            await writer.WriteAsync(sampleData);
            await writer.FlushAsync();

            Console.WriteLine("Data written:");
            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine(sampleData);
            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine();

            var exists = await provider.ExistsAsync(fileUri);

            Console.WriteLine(exists
                ? "✓ Successfully wrote file!"
                : "✗ File write verification failed — file not found.");
        }
        catch (UnauthorizedAccessException ex)
        {
            Console.WriteLine($"✗ Access denied: {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Error writing file: {ex.Message}");
        }

        Console.WriteLine();
    }

    // ========================================================================
    // EXAMPLE 3: List Objects
    // ========================================================================

    private static async Task Example3_ListObjects()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 3: List Objects");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = ServiceUrl,
            AccessKey = AccessKey,
            SecretKey = SecretKey,
            SigningRegion = SigningRegion,
        };

        var factory = new S3CompatibleClientFactory(options);
        var provider = new S3CompatibleStorageProvider(factory, options);

        var prefixUri = StorageUri.Parse($"s3://{BucketName}/data/");

        Console.WriteLine($"Listing objects in: {prefixUri}");
        Console.WriteLine();

        try
        {
            Console.WriteLine("Non-recursive listing (direct children only):");
            Console.WriteLine("─────────────────────────────────────────────────────────────");

            var count = 0;

            await foreach (var item in provider.ListAsync(prefixUri))
            {
                var type = item.IsDirectory
                    ? "[DIR]"
                    : "[FILE]";

                var size = item.IsDirectory
                    ? "-"
                    : FormatBytes(item.Size);

                var modified = item.LastModified.ToString("yyyy-MM-dd HH:mm:ss");

                Console.WriteLine($"{type} {item.Uri.Path.PadRight(40)} | Size: {size.PadRight(12)} | Modified: {modified}");
                count++;
            }

            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine($"Total items (non-recursive): {count}");
            Console.WriteLine();

            Console.WriteLine("Recursive listing (all descendants):");
            Console.WriteLine("─────────────────────────────────────────────────────────────");

            count = 0;

            await foreach (var item in provider.ListAsync(prefixUri, true))
            {
                var type = item.IsDirectory
                    ? "[DIR]"
                    : "[FILE]";

                var size = item.IsDirectory
                    ? "-"
                    : FormatBytes(item.Size);

                var modified = item.LastModified.ToString("yyyy-MM-dd HH:mm:ss");

                Console.WriteLine($"{type} {item.Uri.Path.PadRight(40)} | Size: {size.PadRight(12)} | Modified: {modified}");
                count++;
            }

            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine($"Total items (recursive): {count}");
            Console.WriteLine();
            Console.WriteLine("✓ Successfully listed objects!");
        }
        catch (UnauthorizedAccessException ex)
        {
            Console.WriteLine($"✗ Access denied: {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Error listing objects: {ex.Message}");
        }

        Console.WriteLine();
    }

    // ========================================================================
    // EXAMPLE 4: Check File Existence
    // ========================================================================

    private static async Task Example4_CheckFileExistence()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 4: Check File Existence");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = ServiceUrl,
            AccessKey = AccessKey,
            SecretKey = SecretKey,
            SigningRegion = SigningRegion,
        };

        var factory = new S3CompatibleClientFactory(options);
        var provider = new S3CompatibleStorageProvider(factory, options);

        var existingFileUri = StorageUri.Parse($"s3://{BucketName}/data/sample.csv");
        var nonExistentFileUri = StorageUri.Parse($"s3://{BucketName}/data/non-existent-file.txt");

        Console.WriteLine($"Checking existence of: {existingFileUri}");
        var exists = await provider.ExistsAsync(existingFileUri);
        Console.WriteLine($"  Result: {(exists ? "✓ EXISTS" : "✗ NOT FOUND")}");
        Console.WriteLine();

        Console.WriteLine($"Checking existence of: {nonExistentFileUri}");
        exists = await provider.ExistsAsync(nonExistentFileUri);
        Console.WriteLine($"  Result: {(exists ? "✓ EXISTS" : "✗ NOT FOUND")}");
        Console.WriteLine();

        Console.WriteLine("✓ Successfully checked file existence!");
        Console.WriteLine();
    }

    // ========================================================================
    // EXAMPLE 5: Get File Metadata
    // ========================================================================

    private static async Task Example5_GetFileMetadata()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 5: Get File Metadata");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = ServiceUrl,
            AccessKey = AccessKey,
            SecretKey = SecretKey,
            SigningRegion = SigningRegion,
        };

        var factory = new S3CompatibleClientFactory(options);
        var provider = new S3CompatibleStorageProvider(factory, options);

        var fileUri = StorageUri.Parse($"s3://{BucketName}/data/sample.csv");

        Console.WriteLine($"Retrieving metadata for: {fileUri}");
        Console.WriteLine();

        try
        {
            var metadata = await provider.GetMetadataAsync(fileUri);

            if (metadata is null)
            {
                Console.WriteLine("✗ File not found.");
                return;
            }

            Console.WriteLine("Metadata:");
            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine($"  Size:           {FormatBytes(metadata.Size)}");
            Console.WriteLine($"  Last Modified:  {metadata.LastModified:yyyy-MM-dd HH:mm:ss} UTC");
            Console.WriteLine($"  Content Type:   {metadata.ContentType ?? "N/A"}");
            Console.WriteLine($"  ETag:           {metadata.ETag ?? "N/A"}");
            Console.WriteLine($"  Is Directory:   {metadata.IsDirectory}");

            if (metadata.CustomMetadata.Count > 0)
            {
                Console.WriteLine();
                Console.WriteLine("  Custom Metadata:");

                foreach (var kvp in metadata.CustomMetadata)
                {
                    Console.WriteLine($"    {kvp.Key}: {kvp.Value}");
                }
            }

            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine();
            Console.WriteLine("✓ Successfully retrieved file metadata!");
        }
        catch (UnauthorizedAccessException ex)
        {
            Console.WriteLine($"✗ Access denied: {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Error retrieving metadata: {ex.Message}");
        }

        Console.WriteLine();
    }

    // ========================================================================
    // EXAMPLE 6: Using Dependency Injection
    // ========================================================================

    private static async Task Example6_UsingDependencyInjection()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 6: Using Dependency Injection");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        var services = new ServiceCollection();

        // Build options up-front (all required properties must be provided)
        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = ServiceUrl,
            AccessKey = AccessKey,
            SecretKey = SecretKey,
            SigningRegion = SigningRegion,
        };

        // Register via the extension method
        services.AddS3CompatibleStorageProvider(options);

        var serviceProvider = services.BuildServiceProvider();

        try
        {
            var provider = serviceProvider.GetRequiredService<S3CompatibleStorageProvider>();

            Console.WriteLine("✓ Successfully resolved S3CompatibleStorageProvider from DI container");
            Console.WriteLine();

            var providerMetadata = provider.GetMetadata();

            Console.WriteLine("Provider Metadata:");
            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine($"  Name:                {providerMetadata.Name}");
            Console.WriteLine($"  Supported Schemes:   {string.Join(", ", providerMetadata.SupportedSchemes)}");
            Console.WriteLine($"  Supports Read:       {providerMetadata.SupportsRead}");
            Console.WriteLine($"  Supports Write:      {providerMetadata.SupportsWrite}");
            Console.WriteLine($"  Supports Listing:    {providerMetadata.SupportsListing}");
            Console.WriteLine($"  Supports Metadata:   {providerMetadata.SupportsMetadata}");
            Console.WriteLine($"  Supports Hierarchy:  {providerMetadata.SupportsHierarchy}");

            if (providerMetadata.Capabilities.Count > 0)
            {
                Console.WriteLine();
                Console.WriteLine("  Capabilities:");

                foreach (var kvp in providerMetadata.Capabilities)
                {
                    Console.WriteLine($"    {kvp.Key}: {kvp.Value}");
                }
            }

            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine();
            Console.WriteLine("✓ Successfully demonstrated DI usage!");
        }
        finally
        {
            await serviceProvider.DisposeAsync();
        }

        Console.WriteLine();
    }

    // ========================================================================
    // EXAMPLE 7: Provider-Specific Configurations
    // ========================================================================

    private static void Example7_ProviderConfigurations()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 7: Provider-Specific Configurations");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        // ── MinIO ─────────────────────────────────────────────────────────
        Console.WriteLine("MinIO (local development):");
        Console.WriteLine("─────────────────────────────────────────────────────────────");

        _ = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
            AccessKey = "minioadmin",
            SecretKey = "minioadmin",
            SigningRegion = "us-east-1",
            ForcePathStyle = true,
        };

        Console.WriteLine("  ServiceUrl:    http://localhost:9000");
        Console.WriteLine("  AccessKey:     minioadmin");
        Console.WriteLine("  SecretKey:     minioadmin");
        Console.WriteLine("  SigningRegion: us-east-1");
        Console.WriteLine("  ForcePathStyle: true (required for MinIO)");
        Console.WriteLine();

        // ── LocalStack ────────────────────────────────────────────────────
        Console.WriteLine("LocalStack (local AWS simulation):");
        Console.WriteLine("─────────────────────────────────────────────────────────────");

        _ = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:4566"),
            AccessKey = "test",
            SecretKey = "test",
            SigningRegion = "us-east-1",
            ForcePathStyle = true,
        };

        Console.WriteLine("  ServiceUrl:    http://localhost:4566");
        Console.WriteLine("  AccessKey:     test  (any value works with LocalStack)");
        Console.WriteLine("  SecretKey:     test  (any value works with LocalStack)");
        Console.WriteLine("  SigningRegion: us-east-1");
        Console.WriteLine("  ForcePathStyle: true");
        Console.WriteLine();

        // ── DigitalOcean Spaces ───────────────────────────────────────────
        Console.WriteLine("DigitalOcean Spaces:");
        Console.WriteLine("─────────────────────────────────────────────────────────────");

        _ = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("https://nyc3.digitaloceanspaces.com"),
            AccessKey = "<your-spaces-access-key>",
            SecretKey = "<your-spaces-secret-key>",
            SigningRegion = "us-east-1",
            ForcePathStyle = false, // DigitalOcean Spaces uses virtual-hosted-style
        };

        Console.WriteLine("  ServiceUrl:    https://<region>.digitaloceanspaces.com");
        Console.WriteLine("  AccessKey:     <your Spaces access key>");
        Console.WriteLine("  SecretKey:     <your Spaces secret key>");
        Console.WriteLine("  SigningRegion: us-east-1");
        Console.WriteLine("  ForcePathStyle: false (Spaces uses virtual-hosted-style)");
        Console.WriteLine();

        // ── Cloudflare R2 ─────────────────────────────────────────────────
        Console.WriteLine("Cloudflare R2:");
        Console.WriteLine("─────────────────────────────────────────────────────────────");

        _ = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("https://<account-id>.r2.cloudflarestorage.com"),
            AccessKey = "<your-r2-access-key-id>",
            SecretKey = "<your-r2-secret-access-key>",
            SigningRegion = "auto", // Cloudflare R2 requires "auto" as the signing region
            ForcePathStyle = false,
        };

        Console.WriteLine("  ServiceUrl:    https://<account-id>.r2.cloudflarestorage.com");
        Console.WriteLine("  AccessKey:     <R2 access key ID>");
        Console.WriteLine("  SecretKey:     <R2 secret access key>");
        Console.WriteLine("  SigningRegion: auto  ← required for Cloudflare R2");
        Console.WriteLine("  ForcePathStyle: false");
        Console.WriteLine();

        Console.WriteLine("✓ Successfully demonstrated provider-specific configurations!");
        Console.WriteLine();
    }

    // ========================================================================
    // HELPER METHODS
    // ========================================================================

    private static string FormatBytes(long bytes)
    {
        string[] sizes = ["B", "KB", "MB", "GB", "TB"];
        double len = bytes;
        var order = 0;

        while (len >= 1024 && order < sizes.Length - 1)
        {
            order++;
            len /= 1024;
        }

        return $"{len:0.##} {sizes[order]}";
    }
}

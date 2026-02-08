using Amazon;
using Amazon.Runtime;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Aws;
using NPipeline.StorageProviders.Models;

namespace Sample_S3StorageProvider;

/// <summary>
///     Entry point for S3 Storage Provider sample demonstrating AWS S3 storage provider usage.
///     This sample shows how to read, write, list, and manage files in AWS S3 using the NPipeline storage provider.
/// </summary>
public sealed class Program
{
    // ========================================================================
    // CONFIGURATION - Replace these values with your own AWS credentials and bucket
    // ========================================================================

    /// <summary>
    ///     Your AWS S3 bucket name. Replace with your actual bucket name.
    /// </summary>
    private const string BucketName = "your-bucket-name-here";

    /// <summary>
    ///     Your AWS access key ID. Replace with your actual credentials or use the default credential chain.
    /// </summary>
    private const string AccessKeyId = "your-access-key-id";

    /// <summary>
    ///     Your AWS secret access key. Replace with your actual credentials or use the default credential chain.
    /// </summary>
    private const string SecretAccessKey = "your-secret-access-key";

    /// <summary>
    ///     Your AWS region. Replace with your actual region.
    /// </summary>
    private const string Region = "us-east-1";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: AWS S3 Storage Provider ===");
        Console.WriteLine();

        // Check if user has configured their credentials
        if (BucketName == "your-bucket-name-here")
        {
            Console.WriteLine("⚠️  WARNING: You need to configure your AWS credentials and bucket name!");
            Console.WriteLine();
            Console.WriteLine("Please update the following constants in Program.cs:");
            Console.WriteLine("  - BucketName: Your S3 bucket name");
            Console.WriteLine("  - AccessKeyId: Your AWS access key ID (or use default credential chain)");
            Console.WriteLine("  - SecretAccessKey: Your AWS secret access key (or use default credential chain)");
            Console.WriteLine("  - Region: Your AWS region");
            Console.WriteLine();
            Console.WriteLine("Alternatively, you can use the default AWS credential chain by:");
            Console.WriteLine("  1. Setting AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables");
            Console.WriteLine("  2. Configuring AWS credentials in ~/.aws/credentials");
            Console.WriteLine("  3. Using IAM roles when running on EC2/ECS");
            Console.WriteLine();
            Console.WriteLine("Press any key to continue with demo mode (examples will be shown but not executed)...");
            Console.ReadKey();
            Console.WriteLine();
        }

        try
        {
            // Run all examples
            await Example1_BasicReadFromS3();
            await Example2_BasicWriteToS3();
            await Example3_ListS3Objects();
            await Example4_CheckFileExistence();
            await Example5_GetFileMetadata();
            await Example6_UsingDependencyInjection();
            Example7_S3CompatibleEndpoints();

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
    // EXAMPLE 1: Basic Read from S3
    // ========================================================================

    /// <summary>
    ///     Example 1: Demonstrates reading a CSV file from S3.
    /// </summary>
    private static async Task Example1_BasicReadFromS3()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 1: Basic Read from S3");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        // Create S3 storage provider with credentials
        var options = new S3StorageProviderOptions
        {
            DefaultRegion = RegionEndpoint.GetBySystemName(Region),
            DefaultCredentials = new BasicAWSCredentials(AccessKeyId, SecretAccessKey),
            UseDefaultCredentialChain = false,
        };

        var clientFactory = new S3ClientFactory(options);
        var provider = new S3StorageProvider(clientFactory, options);

        // Define the S3 URI for the file to read
        var fileUri = StorageUri.Parse($"s3://{BucketName}/data/sample.csv");

        Console.WriteLine($"Reading file: {fileUri}");
        Console.WriteLine();

        try
        {
            // Open a readable stream from S3
            await using var stream = await provider.OpenReadAsync(fileUri);

            // Read the stream content
            using var reader = new StreamReader(stream);
            var content = await reader.ReadToEndAsync();

            Console.WriteLine("File contents:");
            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine(content);
            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine();
            Console.WriteLine("✓ Successfully read file from S3!");
        }
        catch (UnauthorizedAccessException ex)
        {
            Console.WriteLine($"✗ Access denied: {ex.Message}");
            Console.WriteLine("  Check your AWS credentials and bucket permissions.");
        }
        catch (FileNotFoundException ex)
        {
            Console.WriteLine($"✗ File not found: {ex.Message}");
            Console.WriteLine("  Ensure the file exists in the specified bucket.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Error reading file: {ex.Message}");
        }

        Console.WriteLine();
    }

    // ========================================================================
    // EXAMPLE 2: Basic Write to S3
    // ========================================================================

    /// <summary>
    ///     Example 2: Demonstrates writing data to S3.
    /// </summary>
    private static async Task Example2_BasicWriteToS3()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 2: Basic Write to S3");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        // Create S3 storage provider
        var options = new S3StorageProviderOptions
        {
            DefaultRegion = RegionEndpoint.GetBySystemName(Region),
            DefaultCredentials = new BasicAWSCredentials(AccessKeyId, SecretAccessKey),
            UseDefaultCredentialChain = false,
        };

        var clientFactory = new S3ClientFactory(options);
        var provider = new S3StorageProvider(clientFactory, options);

        // Define the S3 URI for the file to write
        var fileUri = StorageUri.Parse($"s3://{BucketName}/data/output/sample-{DateTime.UtcNow:yyyyMMdd-HHmmss}.txt");

        Console.WriteLine($"Writing file: {fileUri}");
        Console.WriteLine();

        try
        {
            // Create sample data
            var sampleData = $"Sample data written at {DateTime.UtcNow:O}\n" +
                             $"This is a test file created by the NPipeline S3 sample.\n" +
                             $"It demonstrates writing data to AWS S3.\n";

            // Open a writable stream to S3
            await using var stream = await provider.OpenWriteAsync(fileUri);

            // Write the data
            await using var writer = new StreamWriter(stream);
            await writer.WriteAsync(sampleData);
            await writer.FlushAsync();

            Console.WriteLine("Data written:");
            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine(sampleData);
            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine();

            // Verify the write succeeded by checking if the file exists
            var exists = await provider.ExistsAsync(fileUri);

            if (exists)
                Console.WriteLine("✓ Successfully wrote file to S3!");
            else
                Console.WriteLine("✗ File write verification failed - file not found.");
        }
        catch (UnauthorizedAccessException ex)
        {
            Console.WriteLine($"✗ Access denied: {ex.Message}");
            Console.WriteLine("  Check your AWS credentials and bucket write permissions.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Error writing file: {ex.Message}");
        }

        Console.WriteLine();
    }

    // ========================================================================
    // EXAMPLE 3: List S3 Objects
    // ========================================================================

    /// <summary>
    ///     Example 3: Demonstrates listing objects in an S3 bucket.
    /// </summary>
    private static async Task Example3_ListS3Objects()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 3: List S3 Objects");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        // Create S3 storage provider
        var options = new S3StorageProviderOptions
        {
            DefaultRegion = RegionEndpoint.GetBySystemName(Region),
            DefaultCredentials = new BasicAWSCredentials(AccessKeyId, SecretAccessKey),
            UseDefaultCredentialChain = false,
        };

        var clientFactory = new S3ClientFactory(options);
        var provider = new S3StorageProvider(clientFactory, options);

        // Define the prefix to list
        var prefixUri = StorageUri.Parse($"s3://{BucketName}/data/");

        Console.WriteLine($"Listing objects in: {prefixUri}");
        Console.WriteLine();

        try
        {
            // List objects non-recursively (only direct children)
            Console.WriteLine("Non-recursive listing (direct children only):");
            Console.WriteLine("─────────────────────────────────────────────────────────────");

            var count = 0;

            await foreach (var item in provider.ListAsync(prefixUri, false))
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

            // List objects recursively (all descendants)
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
            Console.WriteLine("✓ Successfully listed S3 objects!");
        }
        catch (UnauthorizedAccessException ex)
        {
            Console.WriteLine($"✗ Access denied: {ex.Message}");
            Console.WriteLine("  Check your AWS credentials and bucket list permissions.");
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

    /// <summary>
    ///     Example 4: Demonstrates checking if a file exists in S3.
    /// </summary>
    private static async Task Example4_CheckFileExistence()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 4: Check File Existence");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        // Create S3 storage provider
        var options = new S3StorageProviderOptions
        {
            DefaultRegion = RegionEndpoint.GetBySystemName(Region),
            DefaultCredentials = new BasicAWSCredentials(AccessKeyId, SecretAccessKey),
            UseDefaultCredentialChain = false,
        };

        var clientFactory = new S3ClientFactory(options);
        var provider = new S3StorageProvider(clientFactory, options);

        // Test file URIs
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

    /// <summary>
    ///     Example 5: Demonstrates retrieving metadata for a file in S3.
    /// </summary>
    private static async Task Example5_GetFileMetadata()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 5: Get File Metadata");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        // Create S3 storage provider
        var options = new S3StorageProviderOptions
        {
            DefaultRegion = RegionEndpoint.GetBySystemName(Region),
            DefaultCredentials = new BasicAWSCredentials(AccessKeyId, SecretAccessKey),
            UseDefaultCredentialChain = false,
        };

        var clientFactory = new S3ClientFactory(options);
        var provider = new S3StorageProvider(clientFactory, options);

        // Define the file URI
        var fileUri = StorageUri.Parse($"s3://{BucketName}/data/sample.csv");

        Console.WriteLine($"Retrieving metadata for: {fileUri}");
        Console.WriteLine();

        try
        {
            // Get metadata
            var metadata = await provider.GetMetadataAsync(fileUri);

            if (metadata == null)
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
            Console.WriteLine("  Check your AWS credentials and bucket permissions.");
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

    /// <summary>
    ///     Example 6: Demonstrates using S3 storage provider with dependency injection.
    /// </summary>
    private static async Task Example6_UsingDependencyInjection()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 6: Using Dependency Injection");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        // Create a service collection and configure S3 storage provider
        var services = new ServiceCollection();

        // Add S3 storage provider with configuration
        services.AddS3StorageProvider(options =>
        {
            options.DefaultRegion = RegionEndpoint.GetBySystemName(Region);
            options.DefaultCredentials = new BasicAWSCredentials(AccessKeyId, SecretAccessKey);
            options.UseDefaultCredentialChain = false;
            options.MultipartUploadThresholdBytes = 64 * 1024 * 1024; // 64 MB
        });

        // Build the service provider
        var serviceProvider = services.BuildServiceProvider();

        try
        {
            // Resolve the S3 storage provider from DI container
            var provider = serviceProvider.GetRequiredService<S3StorageProvider>();

            Console.WriteLine("✓ Successfully resolved S3StorageProvider from DI container");
            Console.WriteLine();

            // Use the provider to get provider metadata
            var providerMetadata = provider.GetMetadata();

            Console.WriteLine("Provider Metadata:");
            Console.WriteLine("─────────────────────────────────────────────────────────────");
            Console.WriteLine($"  Name:                {providerMetadata.Name}");
            Console.WriteLine($"  Supported Schemes:   {string.Join(", ", providerMetadata.SupportedSchemes)}");
            Console.WriteLine($"  Supports Read:       {providerMetadata.SupportsRead}");
            Console.WriteLine($"  Supports Write:      {providerMetadata.SupportsWrite}");
            Console.WriteLine($"  Supports Delete:     {providerMetadata.SupportsDelete}");
            Console.WriteLine($"  Supports Listing:    {providerMetadata.SupportsListing}");
            Console.WriteLine($"  Supports Metadata:   {providerMetadata.SupportsMetadata}");
            Console.WriteLine($"  Supports Hierarchy: {providerMetadata.SupportsHierarchy}");

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
            // Dispose the service provider
            await serviceProvider.DisposeAsync();
        }

        Console.WriteLine();
    }

    // ========================================================================
    // EXAMPLE 7: S3-Compatible Endpoints (MinIO, LocalStack)
    // ========================================================================

    /// <summary>
    ///     Example 7: Demonstrates configuration for S3-compatible endpoints like MinIO or LocalStack.
    /// </summary>
    private static void Example7_S3CompatibleEndpoints()
    {
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine("Example 7: S3-Compatible Endpoints (MinIO, LocalStack)");
        Console.WriteLine("─────────────────────────────────────────────────────────────");
        Console.WriteLine();

        Console.WriteLine("This example shows how to configure S3 storage provider for");
        Console.WriteLine("S3-compatible services like MinIO or LocalStack.");
        Console.WriteLine();

        // Example 1: MinIO configuration
        Console.WriteLine("MinIO Configuration:");
        Console.WriteLine("─────────────────────────────────────────────────────────────");

        var minioOptions = new S3StorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"), // MinIO default endpoint
            ForcePathStyle = true, // Required for MinIO
            DefaultRegion = RegionEndpoint.USEast1,
            DefaultCredentials = new BasicAWSCredentials("minioadmin", "minioadmin"),
            UseDefaultCredentialChain = false,
        };

        Console.WriteLine("  Service URL:      http://localhost:9000");
        Console.WriteLine("  Force Path Style: true");
        Console.WriteLine("  Region:           us-east-1");
        Console.WriteLine("  Access Key:       minioadmin");
        Console.WriteLine("  Secret Key:       minioadmin");
        Console.WriteLine();

        // Example 2: LocalStack configuration
        Console.WriteLine("LocalStack Configuration:");
        Console.WriteLine("─────────────────────────────────────────────────────────────");

        var localstackOptions = new S3StorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:4566"), // LocalStack default endpoint
            ForcePathStyle = true, // Required for LocalStack
            DefaultRegion = RegionEndpoint.USEast1,
            UseDefaultCredentialChain = true, // LocalStack accepts any credentials
        };

        Console.WriteLine("  Service URL:      http://localhost:4566");
        Console.WriteLine("  Force Path Style: true");
        Console.WriteLine("  Region:           us-east-1");
        Console.WriteLine("  Use Default Credential Chain: true");
        Console.WriteLine();

        // Example 3: Using with DI
        Console.WriteLine("DI Configuration for MinIO:");
        Console.WriteLine("─────────────────────────────────────────────────────────────");

        Console.WriteLine("  services.AddS3StorageProvider(options =>");
        Console.WriteLine("  {");
        Console.WriteLine("      options.ServiceUrl = new Uri(\"http://localhost:9000\");");
        Console.WriteLine("      options.ForcePathStyle = true;");
        Console.WriteLine("      options.DefaultRegion = RegionEndpoint.USEast1;");
        Console.WriteLine("      options.DefaultCredentials = new BasicAWSCredentials(\"minioadmin\", \"minioadmin\");");
        Console.WriteLine("      options.UseDefaultCredentialChain = false;");
        Console.WriteLine("  });");
        Console.WriteLine();

        Console.WriteLine("✓ Successfully demonstrated S3-compatible endpoint configuration!");
        Console.WriteLine();
    }

    // ========================================================================
    // HELPER METHODS
    // ========================================================================

    /// <summary>
    ///     Formats a byte count into a human-readable string.
    /// </summary>
    /// <param name="bytes">The number of bytes to format.</param>
    /// <returns>A human-readable string representation.</returns>
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

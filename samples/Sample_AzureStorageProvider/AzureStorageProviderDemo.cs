using System.Text;
using NPipeline.StorageProviders.Azure;
using NPipeline.StorageProviders.Models;
using Sample_AzureStorageProvider.Models;

namespace Sample_AzureStorageProvider;

/// <summary>
///     Demo scenarios class demonstrating all key features of the Azure Blob Storage Provider.
///     This class contains multiple demo methods showcasing different aspects of the provider.
/// </summary>
public class AzureStorageProviderDemo
{
    private readonly AzureBlobStorageProvider _provider;
    private readonly string _containerName;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AzureStorageProviderDemo" /> class.
    /// </summary>
    /// <param name="provider">The Azure Blob storage provider instance.</param>
    /// <param name="containerName">The container name to use for demos.</param>
    public AzureStorageProviderDemo(AzureBlobStorageProvider provider, string containerName = "demo-container")
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _containerName = containerName;
    }

    /// <summary>
    ///     Runs all demo scenarios in sequence.
    /// </summary>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    public async Task RunAllDemosAsync(CancellationToken cancellationToken = default)
    {
        Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║   Azure Blob Storage Provider - Comprehensive Demo Suite      ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        try
        {
            // Display provider metadata
            await DisplayProviderMetadataAsync(cancellationToken);

            // Demo 1: Basic Read/Write
            await DemoBasicReadWriteAsync(cancellationToken);

            // Demo 2: CSV Processing
            await DemoCsvProcessingAsync(cancellationToken);

            // Demo 3: Large File Handling
            await DemoLargeFileHandlingAsync(cancellationToken);

            // Demo 4: Listing and Filtering
            await DemoListingAndFilteringAsync(cancellationToken);

            // Demo 5: Metadata Operations
            await DemoMetadataOperationsAsync(cancellationToken);

            // Demo 6: Error Handling
            await DemoErrorHandlingAsync(cancellationToken);

            // Demo 7: Different Authentication Methods (informational)
            DemoAuthenticationMethods();

            Console.WriteLine();
            Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║   All demos completed successfully!                           ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Demo suite failed: {ex.Message}");
            Console.WriteLine(ex.ToString());
            Console.ResetColor();
            throw;
        }
    }

    /// <summary>
    ///     Displays metadata about the storage provider.
    /// </summary>
    private async Task DisplayProviderMetadataAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("┌──────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  Provider Metadata                                               │");
        Console.WriteLine("└──────────────────────────────────────────────────────────────────┘");

        var metadata = _provider.GetMetadata();
        Console.WriteLine($"  Name: {metadata.Name}");
        Console.WriteLine($"  Supported Schemes: {string.Join(", ", metadata.SupportedSchemes)}");
        Console.WriteLine($"  Supports Read: {metadata.SupportsRead}");
        Console.WriteLine($"  Supports Write: {metadata.SupportsWrite}");
        Console.WriteLine($"  Supports Delete: {metadata.SupportsDelete}");
        Console.WriteLine($"  Supports Listing: {metadata.SupportsListing}");
        Console.WriteLine($"  Supports Metadata: {metadata.SupportsMetadata}");
        Console.WriteLine($"  Supports Hierarchy: {metadata.SupportsHierarchy}");

        if (metadata.Capabilities != null && metadata.Capabilities.Count > 0)
        {
            Console.WriteLine("  Capabilities:");
            foreach (var capability in metadata.Capabilities)
            {
                Console.WriteLine($"    - {capability.Key}: {capability.Value}");
            }
        }

        Console.WriteLine();
        await Task.CompletedTask;
    }

    /// <summary>
    ///     Demo 1: Basic Read/Write - Demonstrates writing a simple text file and reading it back.
    /// </summary>
    private async Task DemoBasicReadWriteAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("┌──────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  Demo 1: Basic Read/Write                                        │");
        Console.WriteLine("└──────────────────────────────────────────────────────────────────┘");

        var blobPath = "demo/hello.txt";
        var uri = StorageUri.Parse($"azure://{_containerName}/{blobPath}");
        var content = "Hello, Azure Blob Storage! This is a test file created by NPipeline.";

        try
        {
            // Write content to blob
            Console.WriteLine($"  Writing to: {uri}");
            await using (var writeStream = await _provider.OpenWriteAsync(uri, cancellationToken))
            {
                await writeStream.WriteAsync(Encoding.UTF8.GetBytes(content), cancellationToken);
            }
            Console.WriteLine("  ✓ Write successful");

            // Read content back
            Console.WriteLine($"  Reading from: {uri}");
            await using (var readStream = await _provider.OpenReadAsync(uri, cancellationToken))
            using (var reader = new StreamReader(readStream))
            {
                var readContent = await reader.ReadToEndAsync(cancellationToken);
                Console.WriteLine($"  ✓ Read successful");
                Console.WriteLine($"  Content: {readContent}");
            }

            // Check existence
            var exists = await _provider.ExistsAsync(uri, cancellationToken);
            Console.WriteLine($"  ✓ Blob exists: {exists}");

            // Clean up
            await _provider.DeleteAsync(uri, cancellationToken);
            Console.WriteLine("  ✓ Blob deleted");
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"  ✗ Error: {ex.Message}");
            Console.ResetColor();
            throw;
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demo 2: CSV Processing - Demonstrates creating, uploading, reading, and processing CSV files.
    /// </summary>
    private async Task DemoCsvProcessingAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("┌──────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  Demo 2: CSV Processing                                           │");
        Console.WriteLine("└──────────────────────────────────────────────────────────────────┘");

        var csvPath = "demo/sensor-data.csv";
        var outputPath = "demo/processed-sensor-data.csv";
        var csvUri = StorageUri.Parse($"azure://{_containerName}/{csvPath}");
        var outputUri = StorageUri.Parse($"azure://{_containerName}/{outputPath}");

        try
        {
            // Create sample sensor data
            var sensorDataList = new List<SensorData>
            {
                new() { SensorId = "S001", Timestamp = DateTime.Now.AddHours(-3), Temperature = 23.5, Humidity = 65.2, Location = "Server Room" },
                new() { SensorId = "S002", Timestamp = DateTime.Now.AddHours(-2), Temperature = 24.1, Humidity = 62.8, Location = "Data Center" },
                new() { SensorId = "S003", Timestamp = DateTime.Now.AddHours(-1), Temperature = 22.8, Humidity = 68.5, Location = "Server Room" },
                new() { SensorId = "S004", Timestamp = DateTime.Now, Temperature = 25.3, Humidity = 60.1, Location = "Data Center" },
                new() { SensorId = "S005", Timestamp = DateTime.Now.AddMinutes(-30), Temperature = 23.9, Humidity = 64.7, Location = "Office" }
            };

            // Create CSV content
            var csvContent = new StringBuilder();
            csvContent.AppendLine("SensorId,Timestamp,Temperature,Humidity,Location");
            foreach (var data in sensorDataList)
            {
                csvContent.AppendLine(data.ToCsv());
            }

            // Upload CSV to blob storage
            Console.WriteLine($"  Uploading CSV to: {csvUri}");
            await using (var writeStream = await _provider.OpenWriteAsync(csvUri, cancellationToken))
            {
                await writeStream.WriteAsync(Encoding.UTF8.GetBytes(csvContent.ToString()), cancellationToken);
            }
            Console.WriteLine("  ✓ CSV uploaded successfully");

            // Read and process the CSV
            Console.WriteLine($"  Reading and processing CSV from: {csvUri}");
            await using (var readStream = await _provider.OpenReadAsync(csvUri, cancellationToken))
            using (var reader = new StreamReader(readStream))
            {
                var lineCount = 0;
                var processedData = new List<SensorData>();

                while (await reader.ReadLineAsync(cancellationToken) is { } line)
                {
                    lineCount++;
                    if (lineCount == 1) continue; // Skip header

                    var sensorData = SensorData.FromCsv(line);
                    Console.WriteLine($"    - {sensorData}");
                    processedData.Add(sensorData);
                }

                Console.WriteLine($"  ✓ Processed {processedData.Count} sensor readings");
            }

            // Transform data (filter by temperature > 23°C)
            Console.WriteLine("  Filtering data (temperature > 23°C)...");
            var filteredData = sensorDataList.Where(d => d.Temperature > 23.0).ToList();
            Console.WriteLine($"  ✓ Filtered to {filteredData.Count} readings");

            // Write processed data back to blob storage
            Console.WriteLine($"  Writing processed data to: {outputUri}");
            var processedCsvContent = new StringBuilder();
            processedCsvContent.AppendLine("SensorId,Timestamp,Temperature,Humidity,Location,Status");
            foreach (var data in filteredData)
            {
                var status = data.Temperature > 24.0 ? "HIGH" : "NORMAL";
                processedCsvContent.AppendLine($"{data.ToCsv()},{status}");
            }

            await using (var writeStream = await _provider.OpenWriteAsync(outputUri, cancellationToken))
            {
                await writeStream.WriteAsync(Encoding.UTF8.GetBytes(processedCsvContent.ToString()), cancellationToken);
            }
            Console.WriteLine("  ✓ Processed data written successfully");

            // Clean up
            await _provider.DeleteAsync(csvUri, cancellationToken);
            await _provider.DeleteAsync(outputUri, cancellationToken);
            Console.WriteLine("  ✓ Cleanup completed");
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"  ✗ Error: {ex.Message}");
            Console.ResetColor();
            throw;
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demo 3: Large File Handling - Demonstrates uploading large files using block blob upload.
    /// </summary>
    private async Task DemoLargeFileHandlingAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("┌──────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  Demo 3: Large File Handling (>64MB)                              │");
        Console.WriteLine("└──────────────────────────────────────────────────────────────────┘");

        var largeFilePath = "demo/large-file.bin";
        var uri = StorageUri.Parse($"azure://{_containerName}/{largeFilePath}");

        try
        {
            // Generate a large file (10MB for demo purposes - adjust as needed)
            var fileSize = 10 * 1024 * 1024; // 10MB
            Console.WriteLine($"  Generating large file ({fileSize / (1024 * 1024)}MB)...");
            var buffer = new byte[fileSize];
            new Random().NextBytes(buffer);
            Console.WriteLine("  ✓ File generated");

            // Upload large file
            Console.WriteLine($"  Uploading large file to: {uri}");
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            await using (var writeStream = await _provider.OpenWriteAsync(uri, cancellationToken))
            {
                await writeStream.WriteAsync(buffer, cancellationToken);
            }

            stopwatch.Stop();
            Console.WriteLine($"  ✓ Upload completed in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"    Upload speed: {fileSize / (1024.0 * 1024.0) / stopwatch.Elapsed.TotalSeconds:F2} MB/s");

            // Verify the blob exists and get metadata
            var metadata = await _provider.GetMetadataAsync(uri, cancellationToken);
            if (metadata != null)
            {
                Console.WriteLine($"  ✓ Blob verified");
                Console.WriteLine($"    Size: {metadata.Size / (1024.0 * 1024.0):F2} MB");
                Console.WriteLine($"    Last Modified: {metadata.LastModified:yyyy-MM-dd HH:mm:ss}");
            }

            // Read back a portion to verify
            Console.WriteLine($"  Reading first 1KB from: {uri}");
            await using (var readStream = await _provider.OpenReadAsync(uri, cancellationToken))
            {
                var readBuffer = new byte[1024];
                var bytesRead = await readStream.ReadAsync(readBuffer, cancellationToken);
                Console.WriteLine($"  ✓ Read {bytesRead} bytes successfully");
            }

            // Clean up
            await _provider.DeleteAsync(uri, cancellationToken);
            Console.WriteLine("  ✓ Large file deleted");
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"  ✗ Error: {ex.Message}");
            Console.ResetColor();
            throw;
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demo 4: Listing and Filtering - Demonstrates listing blobs with various filters.
    /// </summary>
    private async Task DemoListingAndFilteringAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("┌──────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  Demo 4: Listing and Filtering                                    │");
        Console.WriteLine("└──────────────────────────────────────────────────────────────────┘");

        try
        {
            // Upload multiple files with different paths
            var files = new[]
            {
                "demo/data/file1.txt",
                "demo/data/file2.txt",
                "demo/data/archive/file3.txt",
                "demo/logs/app.log",
                "demo/logs/error.log",
                "demo/config/settings.json"
            };

            Console.WriteLine("  Uploading test files...");
            foreach (var file in files)
            {
                var uri = StorageUri.Parse($"azure://{_containerName}/{file}");
                var content = $"Content of {file}";
                await using (var writeStream = await _provider.OpenWriteAsync(uri, cancellationToken))
                {
                    await writeStream.WriteAsync(Encoding.UTF8.GetBytes(content), cancellationToken);
                }
            }
            Console.WriteLine($"  ✓ Uploaded {files.Length} files");

            // List all blobs recursively
            Console.WriteLine();
            Console.WriteLine("  Listing all blobs (recursive):");
            var prefix = StorageUri.Parse($"azure://{_containerName}/demo/");
            var allBlobs = await _provider.ListAsync(prefix, recursive: true, cancellationToken).ToListAsync(cancellationToken);
            foreach (var blob in allBlobs)
            {
                Console.WriteLine($"    - {blob.Uri.Path} ({blob.Size} bytes)");
            }
            Console.WriteLine($"  ✓ Found {allBlobs.Count} blobs");

            // List blobs with prefix filter
            Console.WriteLine();
            Console.WriteLine("  Listing blobs with prefix 'demo/data/':");
            var dataPrefix = StorageUri.Parse($"azure://{_containerName}/demo/data/");
            var dataBlobs = await _provider.ListAsync(dataPrefix, recursive: true, cancellationToken).ToListAsync(cancellationToken);
            foreach (var blob in dataBlobs)
            {
                Console.WriteLine($"    - {blob.Uri.Path}");
            }
            Console.WriteLine($"  ✓ Found {dataBlobs.Count} blobs");

            // List blobs non-recursively
            Console.WriteLine();
            Console.WriteLine("  Listing blobs non-recursively (demo/):");
            var demoPrefix = StorageUri.Parse($"azure://{_containerName}/demo/");
            var nonRecursiveBlobs = await _provider.ListAsync(demoPrefix, recursive: false, cancellationToken).ToListAsync(cancellationToken);
            foreach (var blob in nonRecursiveBlobs)
            {
                Console.WriteLine($"    - {blob.Uri.Path}");
            }
            Console.WriteLine($"  ✓ Found {nonRecursiveBlobs.Count} blobs at top level");

            // List blobs with logs prefix
            Console.WriteLine();
            Console.WriteLine("  Listing blobs with prefix 'demo/logs/':");
            var logsPrefix = StorageUri.Parse($"azure://{_containerName}/demo/logs/");
            var logsBlobs = await _provider.ListAsync(logsPrefix, recursive: true, cancellationToken).ToListAsync(cancellationToken);
            foreach (var blob in logsBlobs)
            {
                Console.WriteLine($"    - {blob.Uri.Path}");
            }
            Console.WriteLine($"  ✓ Found {logsBlobs.Count} log files");

            // Clean up
            Console.WriteLine();
            Console.WriteLine("  Cleaning up test files...");
            foreach (var file in files)
            {
                var uri = StorageUri.Parse($"azure://{_containerName}/{file}");
                await _provider.DeleteAsync(uri, cancellationToken);
            }
            Console.WriteLine("  ✓ All test files deleted");
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"  ✗ Error: {ex.Message}");
            Console.ResetColor();
            throw;
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demo 5: Metadata Operations - Demonstrates uploading with metadata and retrieving metadata.
    /// </summary>
    private async Task DemoMetadataOperationsAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("┌──────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  Demo 5: Metadata Operations                                      │");
        Console.WriteLine("└──────────────────────────────────────────────────────────────────┘");

        var blobPath = "demo/metadata-test.txt";
        var uri = StorageUri.Parse($"azure://{_containerName}/{blobPath}");
        var content = "This file has custom metadata attached.";

        try
        {
            // Upload blob with content type
            Console.WriteLine($"  Uploading blob with content type: {uri}");
            var contentTypeUri = StorageUri.Parse($"azure://{_containerName}/{blobPath}?contentType=text/plain");
            await using (var writeStream = await _provider.OpenWriteAsync(contentTypeUri, cancellationToken))
            {
                await writeStream.WriteAsync(Encoding.UTF8.GetBytes(content), cancellationToken);
            }
            Console.WriteLine("  ✓ Blob uploaded with content type");

            // Retrieve and display metadata
            Console.WriteLine();
            Console.WriteLine("  Retrieving metadata:");
            var metadata = await _provider.GetMetadataAsync(uri, cancellationToken);
            if (metadata != null)
            {
                Console.WriteLine($"    Size: {metadata.Size} bytes");
                Console.WriteLine($"    Content Type: {metadata.ContentType}");
                Console.WriteLine($"    Last Modified: {metadata.LastModified:yyyy-MM-dd HH:mm:ss}");
                Console.WriteLine($"    ETag: {metadata.ETag}");
                Console.WriteLine($"    Is Directory: {metadata.IsDirectory}");

                if (metadata.CustomMetadata != null && metadata.CustomMetadata.Count > 0)
                {
                    Console.WriteLine("    Custom Metadata:");
                    foreach (var kvp in metadata.CustomMetadata)
                    {
                        Console.WriteLine($"      {kvp.Key}: {kvp.Value}");
                    }
                }
            }
            Console.WriteLine("  ✓ Metadata retrieved successfully");

            // Check blob existence
            Console.WriteLine();
            var exists = await _provider.ExistsAsync(uri, cancellationToken);
            Console.WriteLine($"  Blob exists: {exists}");

            // Check non-existent blob
            var nonExistentUri = StorageUri.Parse($"azure://{_containerName}/demo/non-existent-file.txt");
            var nonExistent = await _provider.ExistsAsync(nonExistentUri, cancellationToken);
            Console.WriteLine($"  Non-existent blob exists: {nonExistent}");

            // Clean up
            await _provider.DeleteAsync(uri, cancellationToken);
            Console.WriteLine("  ✓ Blob deleted");
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"  ✗ Error: {ex.Message}");
            Console.ResetColor();
            throw;
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demo 6: Error Handling - Demonstrates proper exception handling for various error scenarios.
    /// </summary>
    private async Task DemoErrorHandlingAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("┌──────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  Demo 6: Error Handling                                           │");
        Console.WriteLine("└──────────────────────────────────────────────────────────────────┘");

        // Scenario 1: Try to read non-existent blob
        Console.WriteLine("  Scenario 1: Reading non-existent blob");
        var nonExistentUri = StorageUri.Parse($"azure://{_containerName}/demo/non-existent-file.txt");
        try
        {
            await using var stream = await _provider.OpenReadAsync(nonExistentUri, cancellationToken);
            Console.WriteLine("    ✗ Should have thrown an exception");
        }
        catch (FileNotFoundException ex)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"    ✓ Caught expected FileNotFoundException: {ex.Message}");
            Console.ResetColor();
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"    ✓ Caught exception: {ex.GetType().Name}");
            Console.ResetColor();
        }

        // Scenario 2: Try to delete non-existent blob (should not throw)
        Console.WriteLine();
        Console.WriteLine("  Scenario 2: Deleting non-existent blob (DeleteIfExists)");
        try
        {
            await _provider.DeleteAsync(nonExistentUri, cancellationToken);
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("    ✓ Delete completed without exception (DeleteIfExists behavior)");
            Console.ResetColor();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"    Exception thrown: {ex.Message}");
        }

        // Scenario 3: Try to get metadata for non-existent blob
        Console.WriteLine();
        Console.WriteLine("  Scenario 3: Getting metadata for non-existent blob");
        try
        {
            var metadata = await _provider.GetMetadataAsync(nonExistentUri, cancellationToken);
            if (metadata == null)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("    ✓ Returned null as expected for non-existent blob");
                Console.ResetColor();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"    Exception thrown: {ex.Message}");
        }

        // Scenario 4: Try to access invalid container
        Console.WriteLine();
        Console.WriteLine("  Scenario 4: Accessing invalid container name");
        var invalidUri = StorageUri.Parse("azure://invalid-container-name-with-invalid-chars!/file.txt");
        try
        {
            await using var stream = await _provider.OpenReadAsync(invalidUri, cancellationToken);
            Console.WriteLine("    ✗ Should have thrown an exception");
        }
        catch (ArgumentException ex)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"    ✓ Caught expected ArgumentException: {ex.Message}");
            Console.ResetColor();
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"    ✓ Caught exception: {ex.GetType().Name}");
            Console.ResetColor();
        }

        // Scenario 5: Try to use null URI
        Console.WriteLine();
        Console.WriteLine("  Scenario 5: Using null URI");
        try
        {
            await _provider.ExistsAsync(null!, cancellationToken);
            Console.WriteLine("    ✗ Should have thrown an exception");
        }
        catch (ArgumentNullException ex)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"    ✓ Caught expected ArgumentNullException: {ex.ParamName}");
            Console.ResetColor();
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demo 7: Different Authentication Methods - Informational demo about authentication options.
    /// </summary>
    private void DemoAuthenticationMethods()
    {
        Console.WriteLine("┌──────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  Demo 7: Authentication Methods                                   │");
        Console.WriteLine("└──────────────────────────────────────────────────────────────────┘");

        Console.WriteLine("  The Azure Blob Storage Provider supports multiple authentication methods:");
        Console.WriteLine();
        Console.WriteLine("  1. Connection String (Recommended for development):");
        Console.WriteLine("     - UseDevelopmentStorage=true (for Azurite emulator)");
        Console.WriteLine("     - DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net");
        Console.WriteLine();
        Console.WriteLine("  2. Account Key:");
        Console.WriteLine("     - Set AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY environment variables");
        Console.WriteLine("     - Or configure via AzureBlobStorageProviderOptions.DefaultConnectionString");
        Console.WriteLine();
        Console.WriteLine("  3. SAS Token (Shared Access Signature):");
        Console.WriteLine("     - Include SAS token in the connection string or blob URI");
        Console.WriteLine("     - Example: azure://container/blob?sas_token=...");
        Console.WriteLine();
        Console.WriteLine("  4. Default Azure Credential Chain (Production):");
        Console.WriteLine("     - Uses DefaultAzureCredential from Azure.Identity");
        Console.WriteLine("     - Supports: Managed Identity, Service Principal, Visual Studio, Azure CLI, etc.");
        Console.WriteLine("     - Enabled by default via AzureBlobStorageProviderOptions.UseDefaultCredentialChain");
        Console.WriteLine();
        Console.WriteLine("  5. Custom Token Credential:");
        Console.WriteLine("     - Provide a custom TokenCredential via AzureBlobStorageProviderOptions.DefaultCredential");
        Console.WriteLine();
        Console.WriteLine("  Configuration Priority:");
        Console.WriteLine("    1. Connection string in URI parameters");
        Console.WriteLine("    2. AzureBlobStorageProviderOptions.DefaultConnectionString");
        Console.WriteLine("    3. AzureBlobStorageProviderOptions.DefaultCredential");
        Console.WriteLine("    4. Default credential chain (if UseDefaultCredentialChain is true)");
        Console.WriteLine();
        Console.WriteLine("  For this demo, we're using the Azurite emulator with:");
        Console.WriteLine("    ConnectionString: UseDevelopmentStorage=true");
        Console.WriteLine("    ServiceUrl: http://127.0.0.1:10000/devstoreaccount1");
        Console.WriteLine();

        Console.WriteLine("  To configure authentication in your application:");
        Console.WriteLine();
        Console.WriteLine("  Option A - Environment Variables:");
        Console.WriteLine("    export AZURE_STORAGE_CONNECTION_STRING=\"UseDevelopmentStorage=true\"");
        Console.WriteLine();
        Console.WriteLine("  Option B - appsettings.json:");
        Console.WriteLine("    {");
        Console.WriteLine("      \"AzureStorage\": {");
        Console.WriteLine("        \"DefaultConnectionString\": \"UseDevelopmentStorage=true\",");
        Console.WriteLine("        \"ServiceUrl\": \"http://127.0.0.1:10000/devstoreaccount1\"");
        Console.WriteLine("      }");
        Console.WriteLine("    }");
        Console.WriteLine();
        Console.WriteLine("  Option C - Code Configuration:");
        Console.WriteLine("    services.AddAzureBlobStorageProvider(options =>");
        Console.WriteLine("    {");
        Console.WriteLine("        options.DefaultConnectionString = \"UseDevelopmentStorage=true\";");
        Console.WriteLine("        options.ServiceUrl = new Uri(\"http://127.0.0.1:10000/devstoreaccount1\");");
        Console.WriteLine("    });");
        Console.WriteLine();
    }
}

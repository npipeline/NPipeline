using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.Sftp;

// =============================================================================
// SFTP Storage Provider Sample Application
// =============================================================================
// 
// This sample demonstrates how to use the SFTP storage provider to:
// 1. Configure the provider with password or key-based authentication
// 2. Read files from an SFTP server
// 3. Write files to an SFTP server
// 4. List files in a directory
// 5. Check if files exist
// 6. Get file metadata
//
// IMPORTANT: To run this sample, you need access to an SFTP server.
// You can use Docker to run a local SFTP server:
//   docker run -p 2222:22 -d atmoz/sftp user:password:1001:1001:upload
// =============================================================================

Console.WriteLine("SFTP Storage Provider Sample");
Console.WriteLine("=============================");
Console.WriteLine();

// Configure the service collection
var services = new ServiceCollection();

// Configure the SFTP storage provider
// Option 1: Configure with password authentication
services.AddSftpStorageProvider(options =>
{
    options.DefaultHost = "localhost"; // Change to your SFTP server
    options.DefaultPort = 2222; // Change to your SFTP port (default: 22)
    options.DefaultUsername = "user";
    options.DefaultPassword = "password";
    options.MaxPoolSize = 10;
    options.ConnectionTimeout = TimeSpan.FromSeconds(30);
    options.KeepAliveInterval = TimeSpan.FromSeconds(30);
});

// Build the service provider
using var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<SftpStorageProvider>();

// Display provider metadata
Console.WriteLine("Provider Metadata:");
var metadata = provider.GetMetadata();
Console.WriteLine($"  Name: {metadata.Name}");
Console.WriteLine($"  Supported Schemes: {string.Join(", ", metadata.SupportedSchemes)}");
Console.WriteLine($"  Supports Read: {metadata.SupportsRead}");
Console.WriteLine($"  Supports Write: {metadata.SupportsWrite}");
Console.WriteLine($"  Supports Listing: {metadata.SupportsListing}");
Console.WriteLine($"  Supports Hierarchy: {metadata.SupportsHierarchy}");
Console.WriteLine();

// Example 1: Write a file to SFTP
Console.WriteLine("Example 1: Writing a file to SFTP");

try
{
    var writeUri = StorageUri.Parse("sftp://localhost/upload/sample.txt");

    await using var writeStream = await provider.OpenWriteAsync(writeUri);
    using var writer = new StreamWriter(writeStream);

    await writer.WriteLineAsync("Hello from SFTP Storage Provider!");
    await writer.WriteLineAsync($"Written at: {DateTime.UtcNow:O}");
    await writer.WriteLineAsync("This is a sample file created by NPipeline.");

    Console.WriteLine("  ✓ File written successfully to /upload/sample.txt");
}
catch (Exception ex)
{
    Console.WriteLine($"  ✗ Failed to write file: {ex.Message}");
}

Console.WriteLine();

// Example 2: Read a file from SFTP
Console.WriteLine("Example 2: Reading a file from SFTP");

try
{
    var readUri = StorageUri.Parse("sftp://localhost/upload/sample.txt");

    await using var readStream = await provider.OpenReadAsync(readUri);
    using var reader = new StreamReader(readStream);

    var content = await reader.ReadToEndAsync();
    Console.WriteLine("  File content:");
    Console.WriteLine("  " + content.ReplaceLineEndings("\n  ").TrimEnd());
}
catch (Exception ex)
{
    Console.WriteLine($"  ✗ Failed to read file: {ex.Message}");
}

Console.WriteLine();

// Example 3: Check if a file exists
Console.WriteLine("Example 3: Checking if files exist");

try
{
    var existingFile = StorageUri.Parse("sftp://localhost/upload/sample.txt");
    var nonExistingFile = StorageUri.Parse("sftp://localhost/upload/nonexistent.txt");

    var existsResult = await provider.ExistsAsync(existingFile);
    var notExistsResult = await provider.ExistsAsync(nonExistingFile);

    Console.WriteLine($"  sample.txt exists: {existsResult}");
    Console.WriteLine($"  nonexistent.txt exists: {notExistsResult}");
}
catch (Exception ex)
{
    Console.WriteLine($"  ✗ Failed to check file existence: {ex.Message}");
}

Console.WriteLine();

// Example 4: Get file metadata
Console.WriteLine("Example 4: Getting file metadata");

try
{
    var uri = StorageUri.Parse("sftp://localhost/upload/sample.txt");
    var fileMetadata = await provider.GetMetadataAsync(uri);

    if (fileMetadata != null)
    {
        Console.WriteLine($"  Size: {fileMetadata.Size} bytes");
        Console.WriteLine($"  Last Modified: {fileMetadata.LastModified}");
        Console.WriteLine($"  Is Directory: {fileMetadata.IsDirectory}");
        Console.WriteLine($"  ETag: {fileMetadata.ETag}");
    }
    else
        Console.WriteLine("  File not found");
}
catch (Exception ex)
{
    Console.WriteLine($"  ✗ Failed to get metadata: {ex.Message}");
}

Console.WriteLine();

// Example 5: List files in a directory
Console.WriteLine("Example 5: Listing files in a directory");

try
{
    var listUri = StorageUri.Parse("sftp://localhost/upload/");
    Console.WriteLine("  Files in /upload/:");

    await foreach (var item in provider.ListAsync(listUri, false))
    {
        var type = item.IsDirectory
            ? "DIR "
            : "FILE";

        var size = item.IsDirectory
            ? ""
            : $" ({item.Size} bytes)";

        Console.WriteLine($"    [{type}] {item.Uri.Path}{size}");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"  ✗ Failed to list files: {ex.Message}");
}

Console.WriteLine();

// Example 6: Using URI-based credentials (override defaults)
Console.WriteLine("Example 6: Using URI-based credentials");
Console.WriteLine("  You can specify credentials in the URI:");
Console.WriteLine("    sftp://user:password@host/path/file.txt");
Console.WriteLine("    sftp://host/path/file.txt?username=user&password=secret");
Console.WriteLine("    sftp://host/path/file.txt?username=user&keyPath=/path/to/key");
Console.WriteLine();

// Example 7: High-performance configuration
Console.WriteLine("Example 7: High-performance configuration");
Console.WriteLine("  For high-throughput scenarios, configure the pool:");

Console.WriteLine(@"  
  services.AddSftpStorageProvider(options =>
  {
      options.MaxPoolSize = 20;
      options.ConnectionIdleTimeout = TimeSpan.FromMinutes(10);
      options.KeepAliveInterval = TimeSpan.FromSeconds(15);
      options.ConnectionTimeout = TimeSpan.FromSeconds(15);
      options.ValidateOnAcquire = true;
  });
");

Console.WriteLine();

Console.WriteLine("Sample completed!");
Console.WriteLine();
Console.WriteLine("TIP: To test with a local SFTP server using Docker:");
Console.WriteLine("  docker run -p 2222:22 -d atmoz/sftp user:password:1001:1001:upload");

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Adls;
using NPipeline.StorageProviders.Models;

namespace Sample_AdlsStorageProvider;

/// <summary>
///     Entry point for the ADLS Gen2 Storage Provider sample application.
///     Demonstrates usage of the Azure Data Lake Storage Gen2 provider with the
///     NPipeline storage provider framework.
/// </summary>
public sealed class Program
{
    /// <summary>
    ///     Entry point for the ADLS Gen2 Storage Provider sample application.
    /// </summary>
    public static async Task<int> Main(string[] args)
    {
        Console.WriteLine("\u2554" + new string('\u2550', 66) + "\u2557");
        Console.WriteLine("\u2551  NPipeline ADLS Gen2 Storage Provider Sample                    \u2551");
        Console.WriteLine("\u255a" + new string('\u2550', 66) + "\u255d");
        Console.WriteLine();

        // Build host with DI
        using var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((_, services) =>
            {
                services.AddAdlsGen2StorageProvider(options =>
                {
                    // For local Azurite development:
                    // options.DefaultConnectionString = "UseDevelopmentStorage=true";
                    // options.ServiceUrl = new Uri("http://127.0.0.1:10000/devstoreaccount1/");

                    // For Azure with DefaultAzureCredential:
                    // options.ServiceUrl = new Uri("https://<account>.dfs.core.windows.net/");
                    // options.UseDefaultCredentialChain = true;

                    options.UploadThresholdBytes = 64 * 1024 * 1024; // 64 MB
                });
            })
            .Build();

        try
        {
            var provider = host.Services.GetRequiredService<IStorageProvider>();
            var metadataProvider = host.Services.GetRequiredService<IStorageProviderMetadataProvider>();
            var deletableProvider = host.Services.GetRequiredService<IDeletableStorageProvider>();
            var moveableProvider = host.Services.GetRequiredService<IMoveableStorageProvider>();

            // --- Provider metadata ---
            var providerMetadata = metadataProvider.GetMetadata();
            Console.WriteLine("Provider information:");
            Console.WriteLine($"  Name             : {providerMetadata.Name}");
            Console.WriteLine($"  Schemes          : {string.Join(", ", providerMetadata.SupportedSchemes)}");
            Console.WriteLine($"  SupportsHierarchy: {providerMetadata.SupportsHierarchy}");
            Console.WriteLine($"  SupportsRead     : {providerMetadata.SupportsRead}");
            Console.WriteLine($"  SupportsWrite    : {providerMetadata.SupportsWrite}");
            Console.WriteLine($"  SupportsListing  : {providerMetadata.SupportsListing}");
            Console.WriteLine($"  SupportsMetadata : {providerMetadata.SupportsMetadata}");
            Console.WriteLine();

            // --- Scheme check ---
            var sampleUri = StorageUri.Parse("adls://my-container/samples/test-file.txt");
            Console.WriteLine($"CanHandle adls:// URI: {provider.CanHandle(sampleUri)}");
            Console.WriteLine();

            // The following operations require an active ADLS Gen2 / Azurite endpoint.
            // Uncomment and configure credentials to run them.
            Console.WriteLine("Operational examples (requires Azure credentials):");
            Console.WriteLine();

            /*
            const string filesystem = "my-container";
            const string path       = "samples/hello.txt";
            var uri = StorageUri.Parse($"adls://{filesystem}/{path}");

            // --- Write ---
            Console.WriteLine($"Writing to: {uri}");
            await using (var writeStream = await provider.OpenWriteAsync(uri))
            {
                var bytes = System.Text.Encoding.UTF8.GetBytes("Hello, ADLS Gen2!");
                await writeStream.WriteAsync(bytes);
            }
            Console.WriteLine("  Write completed.");

            // --- Read ---
            Console.WriteLine($"Reading from: {uri}");
            await using var readStream = await provider.OpenReadAsync(uri);
            using var reader = new System.IO.StreamReader(readStream);
            var content = await reader.ReadToEndAsync();
            Console.WriteLine($"  Content: {content}");

            // --- Exists ---
            var exists = await provider.ExistsAsync(uri);
            Console.WriteLine($"  Exists: {exists}");

            // --- Metadata ---
            var fileMetadata = await provider.GetMetadataAsync(uri);
            if (fileMetadata is not null)
            {
                Console.WriteLine($"  Size         : {fileMetadata.Size} bytes");
                Console.WriteLine($"  LastModified : {fileMetadata.LastModified}");
                Console.WriteLine($"  ContentType  : {fileMetadata.ContentType}");
                Console.WriteLine($"  IsDirectory  : {fileMetadata.IsDirectory}");
            }

            // --- List (non-recursive) ---
            var listUri = StorageUri.Parse($"adls://{filesystem}/samples/");
            Console.WriteLine($"Listing (non-recursive): {listUri}");
            await foreach (var item in provider.ListAsync(listUri, recursive: false))
            {
                var type = item.IsDirectory ? "[dir]" : $"{item.Size,10} bytes";
                Console.WriteLine($"  {type}  {item.Uri}");
            }

            // --- Move (atomic rename) ---
            var destUri = StorageUri.Parse($"adls://{filesystem}/samples/renamed.txt");
            Console.WriteLine($"Moving {uri}  ->  {destUri}");
            await moveableProvider.MoveAsync(uri, destUri);
            Console.WriteLine("  Move completed.");

            // --- Delete ---
            Console.WriteLine($"Deleting: {destUri}");
            await deletableProvider.DeleteAsync(destUri);
            Console.WriteLine("  Delete completed.");
            */

            Console.WriteLine("Sample completed. Configure credentials and uncomment the operational examples to run live operations.");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
            Console.Error.WriteLine(ex.StackTrace);
            return 1;
        }
    }
}

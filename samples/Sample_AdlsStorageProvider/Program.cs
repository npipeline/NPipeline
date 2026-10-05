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
                    // Credentials always come from the options, never from the URI.

                    // For local Azurite development, use the full connection string. It names its own
                    // endpoint, so do not also set ServiceUrl:
                    // options.DefaultConnectionString =
                    //     "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=<azurite-key>;" +
                    //     "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

                    // For Azure with DefaultAzureCredential:
                    // options.AccountName = "<account>";
                    // options.UseDefaultCredentialChain = true;

                    // Or with an account key or a SAS token:
                    // options.AccountName = "<account>";
                    // options.DefaultAccountKey = "<account-key>";   // or options.DefaultSasToken = "<sas-token>";

                    // Writes stream to the service in blocks, with no local temporary file.
                    options.PartSizeBytes = 8 * 1024 * 1024; // 8 MiB blocks
                    options.MaxConcurrency = 4;

                    // The provider does not create filesystems unless you ask it to.
                    options.CreateContainerIfMissing = true;
                });
            })
            .Build();

        try
        {
            var provider = host.Services.GetRequiredService<IStorageProvider>();

            // --- Provider metadata ---
            var providerMetadata = provider;
            Console.WriteLine("Provider information:");
            Console.WriteLine($"  Name             : {providerMetadata.Name}");
            Console.WriteLine($"  Schemes          : {string.Join(", ", providerMetadata.Schemes)}");
            Console.WriteLine($"  SupportsHierarchy: {providerMetadata.Capabilities.HasFlag(StorageCapabilities.Hierarchy)}");
            Console.WriteLine($"  SupportsRead     : {providerMetadata.Capabilities.HasFlag(StorageCapabilities.Read)}");
            Console.WriteLine($"  SupportsWrite    : {providerMetadata.Capabilities.HasFlag(StorageCapabilities.Write)}");
            Console.WriteLine($"  SupportsListing  : {providerMetadata.Capabilities.HasFlag(StorageCapabilities.List)}");
            Console.WriteLine($"  SupportsMetadata : {providerMetadata.Capabilities.HasFlag(StorageCapabilities.Read)}");
            Console.WriteLine();

            // --- Scheme check ---
            var sampleUri = StorageUri.Parse("adls://my-container/samples/test-file.txt");
            Console.WriteLine($"Provider serves adls:// URIs: {provider.Schemes.Contains(sampleUri.Scheme)}");
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
                await writeStream.CommitAsync();
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

            // --- Move (an atomic rename with a hierarchical namespace; copy and delete without one) ---
            var destUri = StorageUri.Parse($"adls://{filesystem}/samples/renamed.txt");
            Console.WriteLine($"Moving {uri}  ->  {destUri}");
            await provider.MoveAsync(uri, destUri);
            Console.WriteLine("  Move completed.");

            // --- Delete ---
            Console.WriteLine($"Deleting: {destUri}");
            await provider.DeleteAsync(destUri);
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

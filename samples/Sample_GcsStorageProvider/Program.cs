using System.Reflection;
using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Gcs;
using NPipeline.StorageProviders.Models;

namespace Sample_GcsStorageProvider;

/// <summary>
///     Entry point for the GCS Storage Provider sample demonstrating NPipeline with Google Cloud Storage.
///     This sample shows how to read from and write to GCS using a pipeline.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: GCS Storage Provider Pipeline ===");
        Console.WriteLine();

        // Configuration from environment variables
        var bucket = Environment.GetEnvironmentVariable("NP_GCS_BUCKET") ?? "sample-bucket";
        var projectId = Environment.GetEnvironmentVariable("NP_GCS_PROJECT_ID") ?? "test-project";
        var serviceUrl = Environment.GetEnvironmentVariable("NP_GCS_SERVICE_URL");

        Console.WriteLine($"Configuration:");
        Console.WriteLine($"  Bucket: {bucket}");
        Console.WriteLine($"  Project ID: {projectId}");
        Console.WriteLine($"  Service URL: {serviceUrl ?? "(default GCS endpoint)"}");
        Console.WriteLine();

        try
        {
            // Build the host with NPipeline and GCS storage provider
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    // Add NPipeline services with assembly scanning
                    _ = services.AddNPipeline(Assembly.GetExecutingAssembly());

                    // Add GCS storage provider
                    services.AddGcsStorageProvider(options =>
                    {
                        options.DefaultProjectId = projectId;
                        options.UseDefaultCredentials = true;

                        if (!string.IsNullOrWhiteSpace(serviceUrl) && Uri.TryCreate(serviceUrl, UriKind.Absolute, out var endpoint))
                            options.ServiceUrl = endpoint;
                    });
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and GCS storage provider.");
            Console.WriteLine();

            // Get the storage provider for setup
            var storageProvider = host.Services.GetRequiredService<GcsStorageProvider>();

            // Seed initial data if using emulator (detected by custom service URL)
            if (!string.IsNullOrWhiteSpace(serviceUrl))
            {
                Console.WriteLine("Detected emulator mode - seeding initial data...");
                await SeedInitialDataAsync(storageProvider, bucket);
                Console.WriteLine();
            }

            // Display pipeline description
            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(GcsPipeline.GetDescription());
            Console.WriteLine();

            // Prepare pipeline parameters
            var parameters = new Dictionary<string, object>
            {
                ["Bucket"] = bucket,
                ["InputPrefix"] = "input/",
                ["OutputPrefix"] = "output/"
            };

            // Execute the pipeline using the standard pattern
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<GcsPipeline>(parameters);

            Console.WriteLine();
            Console.WriteLine("Pipeline execution completed successfully!");

            // Show the output
            Console.WriteLine();
            Console.WriteLine("Listing output files:");
            await ListOutputFilesAsync(storageProvider, bucket);
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
    ///     Seeds initial data for the emulator.
    /// </summary>
    private static async Task SeedInitialDataAsync(IStorageProvider storageProvider, string bucket)
    {
        var inputPrefix = "input/";
        var sampleDocuments = new[]
        {
            ("document-1.txt", "Hello from NPipeline! This is the first sample document for GCS processing."),
            ("document-2.txt", "Google Cloud Storage integration allows scalable data processing pipelines."),
            ("document-3.txt", "This document will be transformed by the pipeline and written to the output folder."),
        };

        foreach (var (name, content) in sampleDocuments)
        {
            var uri = StorageUri.Parse($"gs://{bucket}/{inputPrefix}{name}?contentType=text/plain");
            Console.WriteLine($"  Creating: {uri}");

            try
            {
                await using var stream = await storageProvider.OpenWriteAsync(uri);
                var bytes = Encoding.UTF8.GetBytes(content);
                await stream.WriteAsync(bytes);
                Console.WriteLine($"    Wrote {bytes.Length} bytes");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"    Error: {ex.Message}");
            }
        }

        Console.WriteLine($"  Seeded {sampleDocuments.Length} input documents");
    }

    /// <summary>
    ///     Lists the output files after pipeline execution.
    /// </summary>
    private static async Task ListOutputFilesAsync(IStorageProvider storageProvider, string bucket)
    {
        var outputPrefix = StorageUri.Parse($"gs://{bucket}/output/");

        try
        {
            var count = 0;
            await foreach (var item in storageProvider.ListAsync(outputPrefix, recursive: true))
            {
                count++;
                var kind = item.IsDirectory ? "DIR " : "FILE";
                Console.WriteLine($"  [{kind}] {item.Uri.Path} ({item.Size} bytes)");
            }

            if (count == 0)
            {
                Console.WriteLine("  No output files found.");
            }
            else
            {
                Console.WriteLine($"  Total: {count} items");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  Error listing files: {ex.Message}");
        }
    }
}

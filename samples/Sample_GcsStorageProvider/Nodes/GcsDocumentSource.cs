using System.Text;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace Sample_GcsStorageProvider.Nodes;

/// <summary>
///     Source node that reads text documents from Google Cloud Storage.
///     This node demonstrates how to create a source that reads from GCS
///     using the GcsStorageProvider.
/// </summary>
/// <remarks>
///     Configuration is read from PipelineContext.Parameters:
///     - "Bucket": GCS bucket name (required)
///     - "InputPrefix": Prefix for input objects (default: "input/")
/// </remarks>
public class GcsDocumentSource : SourceNode<string>
{
    private readonly IStorageProvider _storageProvider;

    /// <summary>
    ///     Initializes a new instance of the <see cref="GcsDocumentSource" /> class.
    /// </summary>
    /// <param name="storageProvider">The GCS storage provider (injected via DI).</param>
    public GcsDocumentSource(IStorageProvider storageProvider)
    {
        _storageProvider = storageProvider ?? throw new ArgumentNullException(nameof(storageProvider));
    }

    /// <summary>
    ///     Reads documents from GCS and returns them as a data pipe.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the document contents.</returns>
    public override IDataPipe<string> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        // Get configuration from context parameters
        var bucket = context.Parameters.TryGetValue("Bucket", out var bucketObj)
            ? bucketObj?.ToString() ?? "sample-bucket"
            : "sample-bucket";

        var prefix = context.Parameters.TryGetValue("InputPrefix", out var prefixObj)
            ? prefixObj?.ToString() ?? "input/"
            : "input/";

        Console.WriteLine($"Reading documents from GCS: gs://{bucket}/{prefix}");

        var documents = ReadDocumentsAsync(bucket, prefix, cancellationToken).GetAwaiter().GetResult();
        Console.WriteLine($"Read {documents.Count} documents from GCS");

        return new InMemoryDataPipe<string>(documents, "GcsDocumentSource");
    }

    private async Task<List<string>> ReadDocumentsAsync(string bucket, string prefix, CancellationToken cancellationToken)
    {
        var documents = new List<string>();
        var prefixUri = StorageUri.Parse($"gs://{bucket}/{prefix}");

        // List all objects with the specified prefix
        await foreach (var item in _storageProvider.ListAsync(prefixUri, true, cancellationToken))
        {
            if (item.IsDirectory)
                continue;

            cancellationToken.ThrowIfCancellationRequested();

            Console.WriteLine($"Reading: {item.Uri}");

            try
            {
                // Read the object content
                await using var stream = await _storageProvider.OpenReadAsync(item.Uri, cancellationToken);
                using var reader = new StreamReader(stream, Encoding.UTF8);
                var content = await reader.ReadToEndAsync(cancellationToken);

                documents.Add(content);
                Console.WriteLine($"  Read {content.Length} characters");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  Error reading {item.Uri}: {ex.Message}");
            }
        }

        return documents;
    }
}

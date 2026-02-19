using System.Text;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace Sample_GcsStorageProvider.Nodes;

/// <summary>
///     Sink node that writes processed documents to Google Cloud Storage.
///     This node demonstrates how to create a sink that writes to GCS
///     using the GcsStorageProvider.
/// </summary>
/// <remarks>
///     Configuration is read from PipelineContext.Parameters:
///     - "Bucket": GCS bucket name (required)
///     - "OutputPrefix": Prefix for output objects (default: "output/")
/// </remarks>
public class GcsDocumentSink : SinkNode<string>
{
    private readonly IStorageProvider _storageProvider;

    /// <summary>
    ///     Initializes a new instance of the <see cref="GcsDocumentSink" /> class.
    /// </summary>
    /// <param name="storageProvider">The GCS storage provider (injected via DI).</param>
    public GcsDocumentSink(IStorageProvider storageProvider)
    {
        _storageProvider = storageProvider ?? throw new ArgumentNullException(nameof(storageProvider));
    }

    /// <summary>
    ///     Writes processed documents to GCS.
    /// </summary>
    /// <param name="input">The data pipe containing documents to write.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the sink execution.</returns>
    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Get configuration from context parameters
        var bucket = context.Parameters.TryGetValue("Bucket", out var bucketObj)
            ? bucketObj?.ToString() ?? "sample-bucket"
            : "sample-bucket";

        var prefix = context.Parameters.TryGetValue("OutputPrefix", out var prefixObj)
            ? prefixObj?.ToString() ?? "output/"
            : "output/";

        Console.WriteLine($"Writing documents to GCS: gs://{bucket}/{prefix}");

        var documentIndex = 0;

        await foreach (var document in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            documentIndex++;
            var objectName = $"{prefix}document-{documentIndex:D4}-{DateTime.UtcNow:yyyyMMddHHmmss}.txt";
            var uri = StorageUri.Parse($"gs://{bucket}/{objectName}?contentType=text/plain");

            Console.WriteLine($"Writing: {uri}");

            try
            {
                await using var stream = await _storageProvider.OpenWriteAsync(uri, cancellationToken);
                var bytes = Encoding.UTF8.GetBytes(document);
                await stream.WriteAsync(bytes, cancellationToken);

                Console.WriteLine($"  Wrote {bytes.Length} bytes");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  Error writing {uri}: {ex.Message}");
            }
        }

        Console.WriteLine($"Wrote {documentIndex} documents to GCS");
    }
}

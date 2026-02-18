using NPipeline.Pipeline;
using Sample_GcsStorageProvider.Nodes;

namespace Sample_GcsStorageProvider;

/// <summary>
///     Pipeline definition that processes documents from Google Cloud Storage.
///     This pipeline demonstrates how to use GCS as both a source and sink
///     for pipeline data processing.
/// </summary>
/// <remarks>
///     The pipeline flow:
///     1. GcsDocumentSource reads text documents from GCS
///     2. TextTransform processes each document
///     3. GcsDocumentSink writes processed documents back to GCS
///     
///     Configuration is passed via PipelineContext parameters:
///     - "Bucket": GCS bucket name
///     - "InputPrefix": Prefix for input objects (default: "input/")
///     - "OutputPrefix": Prefix for output objects (default: "output/")
///     
///     Nodes receive IStorageProvider via constructor injection (DI).
/// </remarks>
public class GcsPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that reads documents from GCS
        // The node receives IStorageProvider via DI and configuration from context.Parameters
        var source = builder.AddSource<GcsDocumentSource, string>("gcs-source");

        // Add the transform node that processes text
        var transform = builder.AddTransform<TextTransform, string, string>("text-transform");

        // Add the sink node that writes documents to GCS
        // The node receives IStorageProvider via DI and configuration from context.Parameters
        var sink = builder.AddSink<GcsDocumentSink, string>("gcs-sink");

        // Connect the nodes in a linear flow: source -> transform -> sink
        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"GCS Storage Provider Pipeline Sample:

This sample demonstrates how to use Google Cloud Storage with NPipeline:
- Reading documents from GCS using GcsDocumentSource
- Processing documents with TextTransform
- Writing processed documents back to GCS using GcsDocumentSink

The pipeline can run against:
1. A real GCS bucket (requires Google Cloud credentials)
2. A local GCS emulator (fake-gcs-server) via Docker

To use the emulator:
1. Run: docker run -d --name fake-gcs-server -p 4443:4443 fsouza/fake-gcs-server -scheme http
2. Set NP_GCS_SERVICE_URL=http://localhost:4443
3. Set NP_GCS_BUCKET=sample-bucket

The emulator allows full testing without real GCS credentials.";
    }
}

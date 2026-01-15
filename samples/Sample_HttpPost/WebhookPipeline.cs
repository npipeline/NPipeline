using NPipeline.Pipeline;
using Sample_HttpPost.Models;
using Sample_HttpPost.Nodes;

namespace Sample_HttpPost;

/// <summary>
///     Webhook processing pipeline that demonstrates the push-to-pull bridge pattern.
///     This pipeline processes HTTP POST requests through a series of validation and processing steps.
/// </summary>
/// <remarks>
///     This implementation follows the IPipelineDefinition pattern, which allows the pipeline
///     structure to be defined once and reused multiple times. Each execution creates fresh
///     instances of all nodes, ensuring proper isolation between runs.
/// </remarks>
public class WebhookPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a linear pipeline flow:
    ///     WebhookSource -> ValidationTransform -> DataProcessingTransform -> ConsoleSink
    ///     The pipeline processes webhook data through these stages:
    ///     1. Source reads webhook data from a channel (pushed by HTTP POST)
    ///     2. Transform validates webhook data according to business rules
    ///     3. Transform processes validated data and generates a summary
    ///     4. Sink outputs the final processed data to the console
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the channel-based source node that receives webhook data from HTTP POST requests
        var source = builder.AddSource<WebhookSource, WebhookData>("webhook-source");

        // Add the validation transform node that validates webhook data
        var validation = builder.AddTransform<ValidationTransform, WebhookData, ValidatedWebhookData>("validation-transform");

        // Add the data processing transform node that processes validated data
        var processing = builder.AddTransform<DataProcessingTransform, ValidatedWebhookData, ProcessedData>("data-processing-transform");

        // Add the sink node that outputs to console
        var sink = builder.AddSink<ConsoleSink, ProcessedData>("console-sink");

        // Connect the nodes in a linear flow: source -> validation -> processing -> sink
        builder.Connect(source, validation);
        builder.Connect(validation, processing);
        builder.Connect(processing, sink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"HTTP POST Webhook Processing Sample:

This sample demonstrates the push-to-pull bridge pattern for processing HTTP POST requests:
- Channel-based source that receives data from HTTP endpoints
- Data validation and transformation through the pipeline
- Real-time processing of webhook events

The pipeline flow:
1. WebhookSource reads webhook data from a channel (pushed by HTTP POST requests)
2. ValidationTransform validates webhook data according to business rules
3. DataProcessingTransform processes validated data and generates a summary
4. ConsoleSink outputs the final processed data to the console

Key concepts demonstrated:
- Push-to-pull bridge pattern using System.Threading.Channels
- Channel-based source node implementation
- Singleton registration for shared state between HTTP and pipeline
- Real-time webhook processing
- Proper async/await patterns throughout the pipeline
- Error handling and validation

This implementation follows the IPipelineDefinition pattern, which provides:
- Reusable pipeline definitions
- Proper node isolation between executions
- Type-safe node connections
- Clear separation of pipeline structure from execution logic";
    }
}

using NPipeline.Pipeline;
using Sample_01_BasicPipeline.Nodes;

namespace Sample_01_BasicPipeline;

/// <summary>
///     Basic pipeline definition demonstrating fundamental NPipeline concepts.
///     This pipeline implements a simple "Hello World" flow:
///     1. HelloWorldSource generates string data
///     2. UppercaseTransform converts strings to uppercase
///     3. ConsoleSink outputs the transformed data to the console
/// </summary>
/// <remarks>
///     This implementation follows the IPipelineDefinition pattern, which allows the pipeline
///     structure to be defined once and reused multiple times. Each execution creates fresh
///     instances of all nodes, ensuring proper isolation between runs.
/// </remarks>
public class BasicPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a linear pipeline flow:
    ///     HelloWorldSource -> UppercaseTransform -> ConsoleSink
    ///     The pipeline processes string data through these stages:
    ///     1. Source generates "Hello World" messages with variations
    ///     2. Transform converts all messages to uppercase
    ///     3. Sink outputs the final messages to the console
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that generates "Hello World" messages
        var source = builder.AddSource<HelloWorldSource, string>("hello-world-source");

        // Add the transform node that converts strings to uppercase
        var transform = builder.AddTransform<UppercaseTransform, string, string>("uppercase-transform");

        // Add the sink node that outputs to console
        var sink = builder.AddSink<ConsoleSink, string>("console-sink");

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
        return @"Basic Pipeline Sample:

This sample demonstrates the fundamental concepts of NPipeline:
- Creating a simple source-transform-sink pipeline
- String-based data processing
- IPipelineDefinition implementation pattern
- Linear data flow with node connections
- Step-by-step execution with logging

The pipeline flow:
1. HelloWorldSource generates 'Hello World' messages with variations
2. UppercaseTransform converts all messages to uppercase
3. ConsoleSink outputs the final results to the console

This implementation follows the IPipelineDefinition pattern, which provides:
- Reusable pipeline definitions
- Proper node isolation between executions
- Type-safe node connections
- Clear separation of pipeline structure from execution logic";
    }
}

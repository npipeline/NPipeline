using NPipeline.Pipeline;
using Sample_02_FileProcessing.Nodes;

namespace Sample_02_FileProcessing;

/// <summary>
///     File processing pipeline demonstrating end-to-end file manipulation with NPipeline.
///     This pipeline implements a complete file processing workflow:
///     1. TextFileSource reads text files line by line
///     2. LineTransform processes each line with configurable transformations
///     3. FileSink writes the processed lines to an output file
/// </summary>
/// <remarks>
///     This implementation follows the IPipelineDefinition pattern, which allows the pipeline
///     structure to be defined once and reused multiple times. Each execution creates fresh
///     instances of all nodes, ensuring proper isolation between runs.
///     The pipeline demonstrates file-based data processing with proper resource management,
///     streaming for memory efficiency, and configurable transformation options.
/// </remarks>
public class FileProcessingPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a linear pipeline flow:
    ///     TextFileSource -> LineTransform -> FileSink
    ///     The pipeline processes file content through these stages:
    ///     1. Source reads text files line by line using streaming
    ///     2. Transform applies line-by-line transformations (prefix, line numbers, case conversion)
    ///     3. Sink writes processed lines to output file with atomic write operations
    ///     File paths are configurable through pipeline context parameters:
    ///     - "FilePath": Input file path for TextFileSource
    ///     - "OutputFilePath": Output file path for FileSink
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that reads text files line by line
        var source = builder.AddSource<TextFileSource, string>("text-file-source");

        // Add the transform node that processes each line with configurable options
        // Default parameters: prefix="PROCESSED: ", addLineNumbers=true, convertToUpperCase=false
        var transform = builder.AddTransform<LineTransform, string, string>("line-transform");

        // Add the sink node that writes processed lines to output file
        var sink = builder.AddSink<FileSink, string>("file-sink");

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
        return @"File Processing Pipeline Sample:

This sample demonstrates file-based data processing with NPipeline:
- Reading text files using streaming source nodes
- Line-by-line transformation with configurable options
- Writing processed data to output files with atomic operations
- Resource management and proper error handling
- Pipeline context parameter configuration

The pipeline flow:
1. TextFileSource reads input text files line by line using streaming
2. LineTransform applies configurable transformations (prefix, line numbers, case conversion)
3. FileSink writes processed lines to output file with atomic write operations

Key features demonstrated:
- Streaming file processing for memory efficiency
- Configurable transformation options through constructor parameters
- File path configuration through pipeline context parameters
- Atomic file writing with temporary files
- Proper resource disposal and error handling
- Type-safe node connections and data flow

This implementation follows the IPipelineDefinition pattern, which provides:
- Reusable pipeline definitions
- Proper node isolation between executions
- Type-safe node connections
- Clear separation of pipeline structure from execution logic";
    }
}

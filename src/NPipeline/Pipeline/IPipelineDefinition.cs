namespace NPipeline.Pipeline;

/// <summary>
///     Defines the contract for a class that defines a pipeline's structure and data flow.
/// </summary>
/// <remarks>
///     <para>
///         A pipeline definition declaratively specifies the nodes that comprise a pipeline and
///         how they are connected. This allows pipeline structures to be:
///         - Defined once and reused multiple times
///         - Tested in isolation
///         - Versioned alongside business logic
///         - Composed into larger pipeline hierarchies
///     </para>
///     <para>
///         Implement this interface to create reusable pipeline templates. Each instance of your
///         pipeline definition will run independently, with fresh node instances created for each run.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Define a simple CSV data processing pipeline
/// public class CsvProcessingPipeline : IPipelineDefinition
/// {
///     private readonly string _filePath;
///     private readonly string _outputPath;
/// 
///     public CsvProcessingPipeline(string filePath, string outputPath)
///     {
///         _filePath = filePath;
///         _outputPath = outputPath;
///     }
/// 
///     public void Define(PipelineBuilder builder, PipelineContext context)
///     {
///         // Add a CSV file source
///         var source = builder.AddSource&lt;CsvSource&gt;("csv-reader");
/// 
///         // Add a transformation to validate and clean data
///         var validation = builder.AddTransform&lt;DataValidation&gt;("validation");
/// 
///         // Add enrichment with external data
///         var enrichment = builder.AddTransform&lt;DataEnrichment&gt;("enrichment");
/// 
///         // Add output to database
///         var sink = builder.AddSink&lt;DatabaseSink&gt;("db-writer");
/// 
///         // Define the connections
///         builder.Connect(source, validation);
///         builder.Connect(validation, enrichment);
///         builder.Connect(enrichment, sink);
///     }
/// }
/// 
/// // Use the pipeline
/// var runner = PipelineRunner.Create();
/// var context = PipelineContext.Default;
/// await runner.RunAsync&lt;CsvProcessingPipeline&gt;(context);
/// </code>
/// </example>
public interface IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline's structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The <see cref="PipelineBuilder" /> used to add and connect nodes.</param>
    /// <param name="context">The <see cref="PipelineContext" /> containing execution configuration and services.</param>
    /// <remarks>
    ///     This method is called once per pipeline execution. Implement it to:
    ///     - Add source, transform, and sink nodes
    ///     - Define connections between nodes
    ///     - Configure execution strategies per node
    ///     - Apply error handling policies
    /// </remarks>
    void Define(PipelineBuilder builder, PipelineContext context);
}

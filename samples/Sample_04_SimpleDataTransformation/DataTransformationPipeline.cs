using NPipeline.Pipeline;
using Sample_04_SimpleDataTransformation.Nodes;

namespace Sample_04_SimpleDataTransformation;

/// <summary>
///     Data transformation pipeline demonstrating CSV processing with validation, filtering, and enrichment.
///     This pipeline implements a complete data transformation flow:
///     1. CsvSource reads CSV data and converts to objects
///     2. ValidationTransform validates data according to business rules
///     3. FilteringTransform filters data based on conditions
///     4. NullFilterTransform filters out placeholder items
///     5. EnrichmentTransform adds additional data from external sources
///     6. ConsoleSink outputs the final transformed data
/// </summary>
/// <remarks>
///     This implementation follows the IPipelineDefinition pattern, which allows the pipeline
///     structure to be defined once and reused multiple times. Each execution creates fresh
///     instances of all nodes, ensuring proper isolation between runs.
/// </remarks>
public class DataTransformationPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a linear pipeline flow:
    ///     CsvSource -> ValidationTransform -> FilteringTransform -> NullFilterTransform -> EnrichmentTransform -> ConsoleSink
    ///     The pipeline processes CSV data through these stages:
    ///     1. Source reads CSV data and converts to Person objects
    ///     2. Transform validates Person objects according to business rules
    ///     3. Transform filters Person objects based on age conditions
    ///     4. Transform filters out placeholder items
    ///     5. Transform enriches Person objects with additional data
    ///     6. Sink outputs the final transformed Person objects to the console
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that reads CSV data and converts to Person objects
        var source = builder.AddSource<CsvSource, Person>("csv-source");

        // Add the validation transform node that validates Person objects
        var validation = builder.AddTransform<ValidationTransform, Person, Person>("validation-transform");

        // Add the filtering transform node that filters Person objects based on conditions
        var filtering = builder.AddTransform<FilteringTransform, Person, Person>("filtering-transform");

        // Add the null filter transform node that filters out placeholder items
        var nullFilter = builder.AddTransform<NullFilterTransform, Person, Person>("null-filter-transform");

        // Add the enrichment transform node that adds additional data to Person objects
        var enrichment = builder.AddTransform<EnrichmentTransform, Person, EnrichedPerson>("enrichment-transform");

        // Add the sink node that outputs to console
        var sink = builder.AddSink<ConsoleSink, EnrichedPerson>("console-sink");

        // Connect the nodes in a linear flow: source -> validation -> filtering -> nullFilter -> enrichment -> sink
        builder.Connect(source, validation);
        builder.Connect(validation, filtering);
        builder.Connect(filtering, nullFilter);
        builder.Connect(nullFilter, enrichment);
        builder.Connect(enrichment, sink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"Simple Data Transformation Sample:

This sample demonstrates data manipulation and type transformation patterns:
- CSV to object transformation
- Data validation rules
- Conditional filtering
- Data enrichment from external sources

The pipeline flow:
1. CsvSource reads CSV data and converts to Person objects
2. ValidationTransform validates Person objects according to business rules
3. FilteringTransform filters Person objects based on age conditions
4. NullFilterTransform filters out placeholder items
5. EnrichmentTransform adds additional data to Person objects
6. ConsoleSink outputs the final transformed Person objects to the console

This implementation follows the IPipelineDefinition pattern, which provides:
- Reusable pipeline definitions
- Proper node isolation between executions
- Type-safe node connections
- Clear separation of pipeline structure from execution logic";
    }
}

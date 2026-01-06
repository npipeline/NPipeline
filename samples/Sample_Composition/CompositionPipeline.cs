using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Extensions.Composition;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_Composition;

/// <summary>
///     Main pipeline demonstrating composition with nested sub-pipelines.
/// </summary>
public class CompositionPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<CustomerSource, Customer>("customers");

        // Use composite node for validation
        var validate = builder.AddComposite<Customer, ValidatedCustomer, ValidationPipeline>(
            "validation",
            CompositeContextConfiguration.Default);

        // Use composite node for enrichment, inheriting parent context
        var enrich = builder.AddComposite<ValidatedCustomer, EnrichedCustomer, EnrichmentPipeline>(
            "enrichment",
            CompositeContextConfiguration.InheritAll);

        var sink = builder.AddSink<ConsoleSink<EnrichedCustomer>, EnrichedCustomer>("console");

        builder.Connect(source, validate);
        builder.Connect(validate, enrich);
        builder.Connect(enrich, sink);
    }
}

/// <summary>
///     Source node that generates sample customer data.
/// </summary>
public sealed class CustomerSource : SourceNode<Customer>
{
    /// <summary>
    ///     Generates a collection of sample customer data.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing generated customer data.</returns>
    public override IDataPipe<Customer> Initialize(
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var customers = new List<Customer>
        {
            new(1, "John Doe", "john.doe@example.com", "555-1234"),
            new(50, "Jane Smith", "jane.smith@example.com"),
            new(200, "Bob Johnson", "bob.johnson@example.com", "555-5678"),
            new(1000, "Alice Williams", "alice.williams@example.com"),
            new(5, "Charlie Brown", "charlie.brown@example.com", "555-9012"),
        };

        return new InMemoryDataPipe<Customer>(customers, "CustomerSource");
    }
}

/// <summary>
///     Sink node that outputs enriched customers to console.
/// </summary>
public sealed class ConsoleSink<T> : SinkNode<T>
{
    /// <summary>
    ///     Processes the input data by writing it to the console.
    /// </summary>
    /// <param name="input">The data pipe containing input data to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the sink execution.</returns>
    public override async Task ExecuteAsync(
        IDataPipe<T> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Processed: {item}");
        }
    }
}

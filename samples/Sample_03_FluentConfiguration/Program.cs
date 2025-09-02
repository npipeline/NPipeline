using NPipeline;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.Parallelism;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Shared;

namespace Sample_03_FluentConfiguration;

/// <summary>
/// Demonstrates the fluent configuration extensions for node handles.
/// Shows how to configure retry, error handling, and execution strategies in a fluent manner.
/// </summary>
public class Program
{
    public static async Task Main()
    {
        var runner = new PipelineRunner();
        await runner.RunAsync<FluentConfigurationPipeline>();

        Console.WriteLine();
        Console.WriteLine("FluentConfigurationPipeline completed successfully");
    }
}

public class FluentConfigurationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add source node
        var source = builder.AddSource<TestSourceNode, SourceData>();

        // Add transform node with fluent configuration
        // This demonstrates configuring retry behavior, error handling, parallelism, and resilience in a single chain
        var transform = builder
            .AddTransform<SampleTransformNode, SourceData, TargetData>()
            .WithRetries(builder, maxRetries: 2)  // Retry up to 2 times on failure
            .WithErrorHandler<SourceData, TargetData, SampleErrorHandler>(builder)  // Add error handler
            .WithBlockingParallelism(builder, maxDegreeOfParallelism: 4)  // Use parallel execution with backpressure
            .WithResilience(builder);  // Wrap with resilient execution strategy

        // Add sink node with fluent configuration
        var sink = builder
            .AddSink<TestSinkNode, TargetData>()
            .WithRetries(builder, maxRetries: 1);  // Retry sink once on failure

        // Connect the pipeline
        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public class TestSourceNode : SourceNode<SourceData>
{
    public override IDataPipe<SourceData> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        var data = new List<SourceData>
        {
            new(1, "A"),
            new(2, "b"),
            new(3, "C"),
            new(4, "d"),
            new(5, "E"),
        };

        return new StreamingDataPipe<SourceData>(data.ToAsyncEnumerable(), "Source Data Stream");
    }
}

public class SampleTransformNode : TransformNode<SourceData, TargetData>
{
    public override Task<TargetData> ExecuteAsync(SourceData item, PipelineContext context, CancellationToken cancellationToken)
    {
        var targetData = new TargetData(item.Id, item.Name.ToUpperInvariant());
        return Task.FromResult(targetData);
    }
}

public class TestSinkNode : SinkNode<TargetData>
{
    public override async Task ExecuteAsync(IDataPipe<TargetData> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            Console.WriteLine($"Sink received: Id={item.Id}, Name={item.Name}");
        }
    }
}

/// <summary>
/// Example error handler that handles errors in the transform node.
/// Demonstrates the fluent configuration pattern for error handling.
/// </summary>
public class SampleErrorHandler : INodeErrorHandler<ITransformNode<SourceData, TargetData>, SourceData>
{
    public Task<NodeErrorDecision> HandleAsync(
        ITransformNode<SourceData, TargetData> node,
        SourceData item,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine($"Error handling: Item {item.Id} caused {exception.GetType().Name}");
        // Skip this item and continue processing
        return Task.FromResult(NodeErrorDecision.Skip);
    }
}

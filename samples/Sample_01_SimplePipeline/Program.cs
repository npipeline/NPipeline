using NPipeline;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Shared;

namespace Sample_01_SimplePipeline;

public class Program
{
    public static async Task Main()
    {
        var runner = new PipelineRunner();
        await runner.RunAsync<SamplePipeline>();

        Console.WriteLine();
        Console.WriteLine("SimplePipeline completed successfully");
    }
}

public class SamplePipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<TestSourceNode, SourceData>();
        var transform = builder.AddTransform<SampleTransformNode, SourceData, TargetData>();
        var sink = builder.AddSink<TestSinkNode, TargetData>();

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
    public override async Task ExecuteAsync(IDataPipe<TargetData> input, PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine("OUTPUT");
        Console.WriteLine("======");

        var data = new List<TargetData>();

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            data.Add(item);
            Console.WriteLine($"{item.Id}: {item.Name}");
        }

        Console.WriteLine("======");
        Console.WriteLine($"Processed {data.Count} items.");
    }
}

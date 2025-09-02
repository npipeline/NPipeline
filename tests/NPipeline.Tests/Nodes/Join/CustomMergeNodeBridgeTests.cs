using System.Collections.Concurrent;
using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Join;

public sealed class CustomMergeNodeBridgeTests
{
    [Fact]
    public async Task Run_UsesUntypedBridgeCustomMerge()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<string>>();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        await runner.RunAsync<PipelineDef>(context);

        var store = provider.GetRequiredService<ConcurrentQueue<string>>();
        store.Should().HaveCount(4);

        // Expect reverse per pipe and data pipes reversed: For sources X1 X2 and Y1 Y2 => process Y then X with per-stream reverse => Y2, Y1, X2, X1
        store.Should().ContainInOrder("Y2", "Y1", "X2", "X1");
    }

    private sealed class TestSourceNode1 : SourceNode<string>
    {
        public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var items = new[] { "X1", "X2" };
            return new StreamingDataPipe<string>(items.ToAsyncEnumerable(), "S1");
        }
    }

    private sealed class TestSourceNode2 : SourceNode<string>
    {
        public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var items = new[] { "Y1", "Y2" };
            return new StreamingDataPipe<string>(items.ToAsyncEnumerable(), "S2");
        }
    }

    // Inherit from CustomMergeNode<T> so untyped path is used (no reflection fallback needed)
    private sealed class BridgedCustomMergeSink(ConcurrentQueue<string> store) : CustomMergeNode<string>, ISinkNode<string>
    {
        public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }

        public Task ExecuteUntypedAsync(IDataPipe input, PipelineContext context, CancellationToken cancellationToken)
        {
            if (input is not IDataPipe<string> typed)
                throw new InvalidOperationException("Unexpected pipe type");

            return ExecuteAsync(typed, context, cancellationToken);
        }

        public override async Task<IDataPipe<string>> MergeAsync(IEnumerable<IDataPipe> pipes, CancellationToken cancellationToken)
        {
            // Custom: alternate full reversed streams starting with last pipe first
            var list = pipes.Select(c => c as IDataPipe<string>).Where(c => c != null)!.ToList();
            list.Reverse();
            var merged = new List<string>();

            foreach (var pipe in list)
            {
                var items = await pipe!.ToListAsync(cancellationToken);
                items.Reverse();
                merged.AddRange(items);
            }

            return new StreamingDataPipe<string>(merged.ToAsyncEnumerable(), "BridgedMerged");
        }
    }

    private sealed class PipelineDef : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s1 = builder.AddSource<TestSourceNode1, string>("s1");
            var s2 = builder.AddSource<TestSourceNode2, string>("s2");
            var sink = builder.AddSink<BridgedCustomMergeSink, string>("sink");
            builder.Connect(s1, sink);
            builder.Connect(s2, sink);
        }
    }
}

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

public sealed class CustomMergeNodeTests
{
    [Fact]
    public async Task Runner_WhenNodeImplementsICustomMergeNode_UsesCustomMergeLogic()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<string>>();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<CustomMergePipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<string>>();
        resultStore.Should().HaveCount(4);

        // The custom logic reverses each stream, so we expect A2, A1, B2, B1
        resultStore.Should().ContainInOrder("A2", "A1", "B2", "B1");
    }

    // Test Node Implementations

    private sealed class TestSourceNode1 : SourceNode<string>
    {
        public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var items = new[] { "A1", "A2" };
            var pipe = new StreamingDataPipe<string>(items.ToAsyncEnumerable(), "TestStream1");
            return pipe;
        }
    }

    private sealed class TestSourceNode2 : SourceNode<string>
    {
        public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var items = new[] { "B1", "B2" };
            var pipe = new StreamingDataPipe<string>(items.ToAsyncEnumerable(), "TestStream2");
            return pipe;
        }
    }

    private sealed class CustomMergeSink(ConcurrentQueue<string> store) : SinkNode<string>, ICustomMergeNode<string>
    {
        public async Task<IDataPipe<string>> MergeAsync(IEnumerable<IDataPipe> pipes, CancellationToken cancellationToken)
        {
            // Custom logic: reverse the items from each stream and concatenate
            var allItems = new List<string>();

            foreach (var pipe in pipes.Cast<IDataPipe<string>>())
            {
                var items = await pipe.ToListAsync(cancellationToken);
                items.Reverse();
                allItems.AddRange(items);
            }

            return new StreamingDataPipe<string>(allItems.ToAsyncEnumerable(), "CustomMergedStream");
        }

        public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }
    }

    // Test Definition

    private sealed class CustomMergePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source1 = builder.AddSource<TestSourceNode1, string>("source1");
            var source2 = builder.AddSource<TestSourceNode2, string>("source2");
            var sink = builder.AddSink<CustomMergeSink, string>("sink");
            builder.Connect(source1, sink);
            builder.Connect(source2, sink);
        }
    }
}

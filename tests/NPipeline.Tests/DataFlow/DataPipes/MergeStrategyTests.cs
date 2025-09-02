using System.Collections.Concurrent;
using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Attributes.Nodes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.DataFlow.DataPipes;

public sealed class MergeStrategyTests
{
    [Fact]
    public async Task Runner_WhenMultipleInputs_UsesConcatenateMergeStrategy()
    {
        // Arrange
        ServiceCollection services = new();
        _ = services.AddSingleton<ConcurrentQueue<string>>();
        _ = services.AddNPipeline(Assembly.GetExecutingAssembly());
        IServiceProvider provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<FanInTestPipeline<ConcatenateTestSink>>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<string>>();
        _ = resultStore.Should().HaveCount(4);
        _ = resultStore.Should().ContainInOrder("A1", "A2", "B1", "B2");
    }

    [Fact]
    public async Task Runner_WhenMultipleInputs_UsesInterleaveMergeStrategyByDefault()
    {
        // Arrange
        ServiceCollection services = new();
        _ = services.AddSingleton<ConcurrentQueue<string>>();
        _ = services.AddNPipeline(Assembly.GetExecutingAssembly());
        IServiceProvider provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<FanInTestPipeline<InterleaveTestSink>>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<string>>();
        _ = resultStore.Should().HaveCount(4);
        _ = resultStore.Should().Contain("A1");
        _ = resultStore.Should().Contain("A2");
        _ = resultStore.Should().Contain("B1");
        _ = resultStore.Should().Contain("B2");

        // For interleave, we cannot guarantee order, only presence
    }

    private sealed class TestSourceNode1 : SourceNode<string>
    {
        public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            string[] items = ["A1", "A2"];
            StreamingDataPipe<string> pipe = new(items.ToAsyncEnumerable(), "TestStream1");
            return pipe;
        }
    }

    private sealed class TestSourceNode2 : SourceNode<string>
    {
        public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            string[] items = ["B1", "B2"];
            StreamingDataPipe<string> pipe = new(items.ToAsyncEnumerable(), "TestStream2");
            return pipe;
        }
    }

    [MergeStrategy(MergeType.Concatenate)]
    private sealed class ConcatenateTestSink(ConcurrentQueue<string> store) : SinkNode<string>
    {
        public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }
    }

    private sealed class InterleaveTestSink(ConcurrentQueue<string> store) : SinkNode<string>
    {
        public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }
    }

    private sealed class FanInTestPipeline<TSink> : IPipelineDefinition where TSink : ISinkNode<string>
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source1 = builder.AddSource<TestSourceNode1, string>("source1");
            var source2 = builder.AddSource<TestSourceNode2, string>("source2");
            var sink = builder.AddSink<TSink, string>("sink");
            _ = builder.Connect(source1, sink);
            _ = builder.Connect(source2, sink);
        }
    }
}

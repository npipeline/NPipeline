using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Execution.Factories;
using NPipeline.Execution.Services;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Execution;

public sealed class NodeInstantiationServiceTests
{
    [Fact]
    public void BuildPlans_StreamTransformWithNonStreamStrategy_ThrowsClearError()
    {
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<TestSourceNode, int>("source");
        var stream = builder.AddStreamTransform<NonStreamStrategyPassthroughNode, int, int>("stream");
        builder.WithExecutionStrategy(stream, new SequentialExecutionStrategy());
        var sink = builder.AddSink<TestSinkNode, int>("sink");
        builder.Connect(source, stream).Connect(stream, sink);

        var graph = builder.Build().Graph;

        var service = new NodeInstantiationService();
        var nodeInstances = service.InstantiateNodes(graph, new DefaultNodeFactory());

        var ex = Assert.Throws<InvalidOperationException>(() => service.BuildPlans(graph, nodeInstances));
        Assert.Contains("cannot run a stream transform", ex.Message, StringComparison.Ordinal);
        Assert.Contains("IStreamExecutionStrategy", ex.Message, StringComparison.Ordinal);
        Assert.Contains("stream", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class NonStreamStrategyPassthroughNode : IStreamTransformNode<int, int>
    {

        public async IAsyncEnumerable<int> TransformAsync(
            IAsyncEnumerable<int> items,
            PipelineContext context,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var item in items.WithCancellation(cancellationToken))
            {
                yield return item;
            }
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new DataStream<int>(Array.Empty<int>().ToAsyncEnumerable(), "test-source");
        }
    }

    private sealed class TestSinkNode : SinkNode<int>
    {
        public override Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
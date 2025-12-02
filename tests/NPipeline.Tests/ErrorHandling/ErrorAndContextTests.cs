using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.ErrorHandling;

public sealed class ErrorAndContextTests
{
    private const string ContextKey = "ErrorHandled";

    [Fact]
    public async Task RunAsync_WhenNodeThrowsException_ShouldBeHandledAndSkipped()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<INodeErrorHandler<ITransformNode<int, int>, int>, TestErrorHandler>();

        // Replace the transient registration with a singleton to access the node's state.
        var descriptor = services.Single(d => d.ServiceType == typeof(FinalSinkNode));
        services.Remove(descriptor);
        var sink = new FinalSinkNode();
        services.AddSingleton(sink);

        var serviceProvider = services.BuildServiceProvider();
        var context = PipelineContext.Default;
        var runner = serviceProvider.GetRequiredService<IPipelineRunner>();

        // Act
        await runner.RunAsync<ErrorTestPipeline>(context);

        // Assert
        sink.Results.Should().NotContain(3, "the item that caused the error should be skipped");
        sink.Results.Should().HaveCount(4, "the other items should be processed");
        sink.Results.Should().BeEquivalentTo([1, 2, 4, 5]);

        context.Items.Should().ContainKey(ContextKey, "the error handler should have been called");
        context.Items[ContextKey].Should().Be(true, "the error handler should have modified the context");
    }

    // Test Nodes
    private sealed class FaultySourceNode : SourceNode<int>
    {
        public override IDataPipe<int> Execute(PipelineContext context, CancellationToken cancellationToken)
        {
            var numbers = Enumerable.Range(1, 5).ToAsyncEnumerable();
            return new StreamingDataPipe<int>(numbers);
        }
    }

    private sealed class CrashingNode : TransformNode<int, int>
    {
        public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Yield(); // Ensure asynchronicity

            if (item == 3)
                throw new InvalidOperationException("This node always crashes on 3!");

            return item;
        }
    }

    private sealed class FinalSinkNode : SinkNode<int>
    {
        public List<int> Results { get; } = [];

        public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input)
            {
                Results.Add(item);
            }
        }
    }

    // Test Error Handler
    private sealed class TestErrorHandler : INodeErrorHandler<ITransformNode<int, int>, int>
    {
        public Task<NodeErrorDecision> HandleAsync(ITransformNode<int, int> node, int item, Exception exception, PipelineContext context,
            CancellationToken cancellationToken)
        {
            context.Items[ContextKey] = true;
            return Task.FromResult(NodeErrorDecision.Skip);
        }
    }

    // Test Pipeline Definition
    private sealed class ErrorTestPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<FaultySourceNode, int>("source");
            var crash = builder.AddTransform<CrashingNode, int, int>("crash");
            var sink = builder.AddSink<FinalSinkNode, int>("sink");

            crash.WithErrorHandler<int, int, TestErrorHandler>(builder);

            builder.Connect(source, crash);
            builder.Connect(crash, sink);
        }
    }
}

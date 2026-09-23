using NPipeline.Execution;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;
using Xunit.Abstractions;

namespace NPipeline.Tests.ErrorHandling;

public sealed class ErrorHandlingTests(ITestOutputHelper output)
{
    [Fact]
    public async Task Runner_WithNodeErrorHandler_ShouldSkipFailedItem()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(typeof(FailingNode).Assembly);

        var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.CreateDefault();

        // Set source data on context
        var sourceData = new List<string> { "item1", "fail", "item2" };
        context.SetSourceData(sourceData);

        // Act
        await runner.RunAsync<FailingPipelineDefinition>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.Items.Should().BeEquivalentTo("item1", "item2");
    }

    public sealed class FailingNode : TransformNode<string, string>
    {
        public override ValueTask<string> TransformAsync(string item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item == "fail")
                throw new InvalidOperationException("Failed on purpose");

            return ValueTask.FromResult<string>(item);
        }
    }

    public sealed class TestResiliencePolicy : IResiliencePolicy
    {
        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Skip);
        }

    }

    public sealed class FailingPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<string>("source");
            var failing = builder.AddTransform<FailingNode, string, string>("failing");
            var sink = builder.AddInMemorySink<string>("sink");

            builder.AddResiliencePolicy(failing, new TestResiliencePolicy());

            builder.Connect(source, failing);
            builder.Connect(failing, sink);
        }
    }
}

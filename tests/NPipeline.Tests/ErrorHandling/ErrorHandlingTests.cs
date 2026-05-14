using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;
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
        var context = PipelineContext.Default;

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
        public override Task<string> TransformAsync(string item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item == "fail")
                throw new InvalidOperationException("Failed on purpose");

            return Task.FromResult(item);
        }
    }

    public sealed class TestResiliencePolicy : IResiliencePolicy
    {
        public Task<ResilienceDecision> DecideNodeFailureAsync(
            NodeDefinition nodeDefinition,
            INode node,
            Exception exception,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecidePipelineFailureAsync(
            string nodeId,
            Exception exception,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
            ITransformNode<TIn, TOut> node,
            TIn failedItem,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Skip);
        }

        public ValueTask<TimeSpan> GetRetryDelayAsync(PipelineContext context, int attemptNumber, CancellationToken cancellationToken)
        {
            return context.GetRetryDelayStrategy().GetDelayAsync(attemptNumber, cancellationToken);
        }

        public IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId)
        {
            return DefaultResiliencePolicy.Instance.GetCircuitBreaker(context, nodeId);
        }
    }

    public sealed class FailingPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<string>("source");
            var failing = builder.AddTransform<FailingNode, string, string>("failing");
            var sink = builder.AddInMemorySink<string>("sink");

            builder.SetNodeResiliencePolicy(failing, new TestResiliencePolicy());

            builder.Connect(source, failing);
            builder.Connect(failing, sink);
        }
    }
}

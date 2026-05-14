using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.Tests.ErrorHandling;

public sealed class GraphLevelErrorHandlingTests
{
    [Fact]
    public async Task RunAsync_WhenGraphHandlerRestartsNode_ShouldSucceed()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());

        var flakyNodeDescriptor = services.Single(d => d.ServiceType == typeof(FlakyNode));
        services.Remove(flakyNodeDescriptor);
        var flakyNode = new FlakyNode(2); // Fails twice, succeeds on the 3rd try
        services.AddSingleton(flakyNode);

        var serviceProvider = services.BuildServiceProvider();
        var context = PipelineContext.Default;
        var runner = serviceProvider.GetRequiredService<IPipelineRunner>();

        // Set source data on the context
        context.SetSourceData([1]);

        // Act
        await runner.RunAsync<RestartTestPipeline>(context);

        // Assert
        // Get sink results
        var sink = context.GetSink<InMemorySinkNode<int>>();

        // Resilient restart now buffers and replays the source stream per attempt; the source emits a single '1' per attempt.
        // The flaky node fails twice (failCount=2) then succeeds on the 3rd attempt, so the sink will observe three '1' values.
        // We assert at least one successful result and that all values are the expected item.
        sink.Items.Should().NotBeEmpty();
        sink.Items.Should().AllBeEquivalentTo(1);
    }

    // Test Nodes
    private sealed class FlakyNode(int failCount = 1) : TransformNode<int, int>
    {
        private int _callCount;

        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _callCount++;

            if (_callCount <= failCount)
                throw new InvalidOperationException($"FlakyNode failed on call {_callCount}");

            return Task.FromResult(item);
        }
    }

    // Test Error Handler
    private sealed class RestartingResiliencePolicy : IResiliencePolicy
    {
        public Task<ResilienceDecision> DecideNodeFailureAsync(
            NodeDefinition nodeDefinition,
            INode node,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecidePipelineFailureAsync(
            string nodeId,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.RestartNode);
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
            return Task.FromResult(ResilienceDecision.Fail);
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

    // Test Pipeline Definition
    private sealed class RestartTestPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<int>("source");
            var flaky = builder.AddTransform<FlakyNode, int, int>("flaky");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, flaky)
                .Connect(flaky, sink)
                .WithResilience(flaky);

            builder.AddResiliencePolicy<RestartingResiliencePolicy>();
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 3, maxMaterializedItems: 1000));
        }
    }
}

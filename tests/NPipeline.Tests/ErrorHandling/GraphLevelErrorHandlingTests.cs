using NPipeline.Execution;
using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

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
        var context = PipelineContext.CreateDefault();
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

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _callCount++;

            if (_callCount <= failCount)
                throw new InvalidOperationException($"FlakyNode failed on call {_callCount}");

            return ValueTask.FromResult<int>(item);
        }
    }

    // Test Error Handler
    private sealed class RestartingResiliencePolicy : IResiliencePolicy
    {
        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.RestartNode);
        }

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
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
                .Connect(flaky, sink);

            builder.AddResiliencePolicy<RestartingResiliencePolicy>();
            builder.WithResilience(o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 3, MaxReplayWindow = 1000, Backoff = RetryBackoff.None } });
        }
    }
}

using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

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

        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _callCount++;

            if (_callCount <= failCount)
                throw new InvalidOperationException($"FlakyNode failed on call {_callCount}");

            return Task.FromResult(item);
        }
    }

    // Test Error Handler
    private sealed class RestartingErrorHandler : IPipelineErrorHandler
    {
        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception error, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(PipelineErrorDecision.RestartNode);
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

            builder.AddPipelineErrorHandler<RestartingErrorHandler>();
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 3, maxMaterializedItems: 1000));
        }
    }
}

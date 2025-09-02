using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Resilience.Restart;

/// <summary>
///     Verifies that exceeding MaxNodeRestartAttempts for a resilient node surfaces of last failure
///     and does not allow a silent success on a subsequent attempt.
/// </summary>
public sealed class ResilientRestartLimitTests
{
    [Fact]
    public async Task ResilientNode_ShouldThrowAfterConfiguredRestartFailures()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddSingleton<RestartingHandler>();
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        // Create a new context for each test to ensure isolation
        var ctx = new PipelineContextBuilder()
            .WithErrorHandlerFactory(new DefaultErrorHandlerFactory())
            .WithLineageFactory(new DefaultLineageFactory())
            .WithObservabilityFactory(new DefaultObservabilityFactory())
            .Build();

        // Set source data on the context
        ctx.SetSourceData([1]);

        var act = async () => await runner.RunAsync<TestPipeline>(ctx);

        await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(RetryExhaustedException));
    }

    private sealed class FlakyTransform : TransformNode<int, int>
    {
        private readonly object _lock = new();
        private int _attempt;

        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                _attempt++;
            }

            // Reset attempt counter for each new item (not just each test run)
            // This ensures the node behaves consistently for each item
            if (item == 1)
            {
                lock (_lock)
                {
                    if (_attempt > 3)
                        _attempt = 1; // Reset if we're processing a new test run
                }
            }

            // Fail first three attempts to exhaust the configured limit of 2 restarts (3 total attempts)
            if (_attempt <= 3)
                throw new InvalidOperationException("boom");

            return Task.FromResult(item);
        }
    }

    private sealed class RestartingHandler : IPipelineErrorHandler
    {
        private int _fails;

        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception exception, PipelineContext context,
            CancellationToken cancellationToken)
        {
            _fails++;

            // Request restart for first 3 failures; retry options should stop earlier (limit=2) causing failure before success.
            return Task.FromResult(_fails < 4
                ? PipelineErrorDecision.RestartNode
                : PipelineErrorDecision.FailPipeline);
        }
    }

    private sealed class TestPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("srcRL");
            var t = builder.AddTransform<FlakyTransform, int, int>("txRL");
            var k = builder.AddInMemorySink<int>("snkRL");
            _ = builder.Connect(s, t).Connect(t, k);
            builder.AddPipelineErrorHandler<RestartingHandler>();
            builder.WithResilience(t);
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 2)); // gate at 2 failures
        }
    }
}

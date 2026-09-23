using NPipeline.Execution;
using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

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
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        // Create a new context for each test to ensure isolation
        var ctx = new PipelineContext(PipelineContextConfiguration.Default);

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

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
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

            return ValueTask.FromResult<int>(item);
        }
    }

    private sealed class RestartingPolicy : IResiliencePolicy
    {
        private int _fails;

        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
        {
            _fails++;

            // Request restart for first 3 failures; retry options should stop earlier (limit=2) causing failure before success.
            return ValueTask.FromResult(_fails < 4
                ? ResilienceDecision.RestartNode
                : ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
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
            builder.AddResiliencePolicy<RestartingPolicy>();
            builder.WithResilience(t);
            builder.WithResilience(o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 2, MaxReplayWindow = 128, Backoff = RetryBackoff.None } }); // gate at 2 failures
        }
    }
}

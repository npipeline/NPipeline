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

namespace NPipeline.Tests.Resilience.Retry;

public sealed class ResilienceRetryTests
{
    [Fact]
    public async Task ResilientStrategy_Retries_Then_Succeeds_On_TransientFailure()
    {
        // Arrange DI runner
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());

        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var ctx = PipelineContext.CreateDefault();

        // Provide a single item source
        ctx.SetSourceData([42]);

        // Act
        await runner.RunAsync<TestPipeline>(ctx);

        // Assert: at least one output produced and every produced value equals the expected item.
        var sink = ctx.GetSink<InMemorySinkNode<int>>();
        sink.Items.Should().NotBeEmpty();

        foreach (var i in sink.Items)
        {
            i.Should().Be(42);
        }
    }

    private sealed class TransientFailTransform : TransformNode<int, int>
    {
        private int _attempts;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _attempts++;

            // fail first two attempts then succeed
            if (_attempts <= 2)
                throw new InvalidOperationException("transient");

            return ValueTask.FromResult<int>(item);
        }
    }

    private sealed class RestartResiliencePolicy : IResiliencePolicy
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

    private sealed class TestPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("srcR");
            var t = builder.AddTransform<TransientFailTransform, int, int>("txR");
            var k = builder.AddInMemorySink<int>("snkR");
            builder.Connect(s, t).Connect(t, k);
            builder.AddResiliencePolicy<RestartResiliencePolicy>();
            builder.WithResilience(t);
            builder.WithResilience(o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 3, MaxReplayWindow = 1000, Backoff = RetryBackoff.None } });
        }
    }
}

using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

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

        var ctx = PipelineContext.Default;

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

        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _attempts++;

            // fail first two attempts then succeed
            if (_attempts <= 2)
                throw new InvalidOperationException("transient");

            return Task.FromResult(item);
        }
    }

    private sealed class RestartResiliencePolicy : IResiliencePolicy
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
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 3, maxMaterializedItems: 1000));
        }
    }
}

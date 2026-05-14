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
using NPipeline.Resilience;
using NPipeline.Tests.Common;

namespace NPipeline.Extensions.Parallelism.Tests;

[Collection("StatefulTests")]
public class ParallelRetryTests
{
    [Fact]
    public async Task ParallelTransform_Should_StopAfterConfiguredItemRetries()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var ctx = new PipelineContext();

        // Set source data on the context
        ctx.SetSourceData([1]);

        // Act & Assert
        var act = async () => await runner.RunAsync<ParallelRetryPipeline>(ctx);

        _ = await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException));
    }

    [Fact]
    public async Task ParallelTransform_PerNodeOverride_ShouldApply()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var ctx = new PipelineContext();

        // Set source data on the context
        ctx.SetSourceData([1]);

        // Act & Assert
        var act = async () => await runner.RunAsync<OverrideParallelRetryPipeline>(ctx);

        _ = await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException));
    }

    private sealed class FlakyParallelTransform : TransformNode<int, int>
    {
        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            var attempt = SharedTestState.AttemptCounts.AddOrUpdate(item, 1, (_, i) => i + 1);
            throw new InvalidOperationException($"fail-{item}-{attempt}");
        }
    }

    private sealed class RetryAllHandler : IResiliencePolicy
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
            return Task.FromResult(ResilienceDecision.Retry);
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

    private sealed class ParallelRetryPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("ps");
            var t = builder.AddTransform<FlakyParallelTransform, int, int>("pt");
            var k = builder.AddInMemorySink<int>("pk");

            _ = builder.WithExecutionStrategy(t, new ParallelExecutionStrategy(2))
                .SetNodeResiliencePolicy(t, new RetryAllHandler())
                .WithRetryOptions(o => o.With(1)) // allow only 1 retry => attempt > 1 should throw
                .Connect(s, t).Connect(t, k);
        }
    }

    private sealed class OverrideParallelRetryPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("os");
            var t = builder.AddTransform<FlakyParallelTransform, int, int>("ot");
            var k = builder.AddInMemorySink<int>("ok");

            builder.WithExecutionStrategy(t, new ParallelExecutionStrategy(4))
                .SetNodeResiliencePolicy(t, new RetryAllHandler())
                .WithRetryOptions(o => o.With(5)) // global high
                .WithRetryOptions(t, PipelineRetryOptions.Default.With(2)) // node override lower
                .Connect(s, t).Connect(t, k);
        }
    }
}

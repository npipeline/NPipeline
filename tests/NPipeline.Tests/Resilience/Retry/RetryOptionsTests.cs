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

namespace NPipeline.Tests.Resilience.Retry;

public sealed class RetryOptionsTests
{
    [Fact]
    public async Task Should_StopAfterConfiguredItemRetries()
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
        var act = async () => await runner.RunAsync<SequentialRetryPipeline>(ctx);

        await act.Should().ThrowAsync<NodeExecutionException>()
            .Where(ex => ex.InnerException is InvalidOperationException);
    }

    [Fact]
    public async Task Should_FailAfterMaxNodeRestartAttempts()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var ctx = new PipelineContext();

        // Set source data on the context
        ctx.SetSourceData([1]);

        var act = async () => await runner.RunAsync<ResilientPipeline>(ctx);

        await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(RetryExhaustedException));
    }

    [Fact]
    public async Task PerNodeOverride_ShouldApplyInsteadOfGlobal()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var sp = services.BuildServiceProvider();
        var runner = sp.GetRequiredService<IPipelineRunner>();

        var ctx = new PipelineContext();

        // Set source data on the context
        ctx.SetSourceData([1]);

        var act = async () => await runner.RunAsync<PerNodeOverridePipeline>(ctx);

        await act.Should().ThrowAsync<NodeExecutionException>()
            .WithInnerException(typeof(InvalidOperationException));
    }

    private sealed class FlakyTransform : TransformNode<int, int>
    {
        private int _attempts;

        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _attempts++;
            throw new InvalidOperationException($"fail-{_attempts}");
        }
    }

    private sealed class FlakyNodeErrorHandler : IResiliencePolicy
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

    private sealed class SequentialRetryPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("s");
            var t = builder.AddTransform<FlakyTransform, int, int>("t");
            var k = builder.AddInMemorySink<int>("k");
            builder.Connect(s, t).Connect(t, k);
            builder.SetNodeResiliencePolicy(t, new FlakyNodeErrorHandler());
            builder.WithRetryOptions(o => o.With(2));
        }
    }

    private sealed class FailingTransform : TransformNode<int, int>
    {
        private int _attempt;

        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _attempt++;

            if (_attempt < 3)
                throw new InvalidOperationException("boom");

            return Task.FromResult(item);
        }
    }

    private sealed class NodeRestartingErrorHandler : IResiliencePolicy
    {
        private int _fails;

        public Task<ResilienceDecision> DecideNodeFailureAsync(
            NodeDefinition nodeDefinition,
            INode node,
            Exception exception,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecidePipelineFailureAsync(string nodeId, Exception exception, PipelineContext context,
            CancellationToken cancellationToken)
        {
            _fails++;

            return Task.FromResult(_fails < 3
                ? ResilienceDecision.RestartNode
                : ResilienceDecision.Fail);
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

    private sealed class ResilientPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("s2");
            var t = builder.AddTransform<FailingTransform, int, int>("ft");
            var k = builder.AddInMemorySink<int>("k2");
            builder.Connect(s, t).Connect(t, k);
            builder.AddResiliencePolicy<NodeRestartingErrorHandler>();
            builder.WithResilience(t);
            builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 2, maxMaterializedItems: 128));
        }
    }

    private sealed class PerNodeOverridePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource<int>("s3");
            var t = builder.AddTransform<FlakyTransform, int, int>("ot");
            var k = builder.AddInMemorySink<int>("k3");
            builder.Connect(s, t).Connect(t, k);
            builder.SetNodeResiliencePolicy(t, new FlakyNodeErrorHandler());
            builder.WithRetryOptions(o => o.With(5)); // global
            builder.WithRetryOptions(t, PipelineRetryOptions.Default.With(1)); // override
        }
    }
}

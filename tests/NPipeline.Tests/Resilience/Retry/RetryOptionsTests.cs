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

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _attempts++;
            throw new InvalidOperationException($"fail-{_attempts}");
        }
    }

    private sealed class FlakyNodeErrorHandler : IResiliencePolicy
    {
        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Retry);
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
            builder.AddResiliencePolicy(t, new FlakyNodeErrorHandler());
            builder.WithResilience(o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 2 } });
        }
    }

    private sealed class FailingTransform : TransformNode<int, int>
    {
        private int _attempt;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _attempt++;

            if (_attempt < 3)
                throw new InvalidOperationException("boom");

            return ValueTask.FromResult<int>(item);
        }
    }

    private sealed class NodeRestartingErrorHandler : ResiliencePolicyBase
    {
        public override ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
        {
            // The node's MaxRestarts is advice; this policy follows it.
            return ValueTask.FromResult(failure.CanRestart
                ? ResilienceDecision.RestartNode
                : ResilienceDecision.Fail);
        }

        public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(ResilienceDecision.Fail);
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

            // One restart: the transform fails twice, so the restarted run fails too and the restarts are exhausted.
            builder.WithResilience(o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 1, MaxReplayWindow = 128, Backoff = RetryBackoff.None } });
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
            builder.AddResiliencePolicy(t, new FlakyNodeErrorHandler());
            builder.WithResilience(o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 5 } }); // global
            builder.WithResilience(t, o => o with { ItemRetry = o.ItemRetry with { MaxRetries = 1 } }); // override
        }
    }
}

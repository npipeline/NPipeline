using System.Collections.Concurrent;
using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Services;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.Tests.Resilience.Retry;

/// <summary>
///     Retrying one item and restarting a whole node have very different costs, but both used to arrive at
///     <see cref="IResiliencePolicy.GetRetryDelayAsync" /> as an indistinguishable attempt number, so a policy had no
///     way to back off differently for the two. The call now carries a <see cref="RetryKind" />.
/// </summary>
public sealed class RetryKindTests
{
    [Fact]
    public void TheDelayCallCarriesTheRetryKind()
    {
        var parameters = typeof(IResiliencePolicy)
            .GetMethod(nameof(IResiliencePolicy.GetRetryDelayAsync))!
            .GetParameters()
            .Select(p => p.ParameterType)
            .ToArray();

        parameters.Should().Contain(typeof(RetryKind), "a policy cannot tell item retries from node restarts without it");
    }

    [Fact]
    public async Task AnItemRetry_AsksForAnItemRetryDelay()
    {
        var policy = new RecordingKindPolicy();
        var context = new PipelineContext();
        context.RunIdentity.PipelineId = Guid.NewGuid();
        context.RunIdentity.RunId = Guid.NewGuid();
        context.ExecutionConfiguration.ResiliencePolicy = policy;

        try
        {
            var result = await PerItemRetryExecutor.Instance.ExecuteWithRetryAsync(
                item: 7,
                node: new FlakyItemTransform(failuresBeforeSuccess: 2),
                context,
                "transform",
                maxItemRetries: 3,
                hasLineageIndex: false,
                lineageInputIndex: 0,
                lineageOutcomeWriter: LineageNodeOutcomeRegistry.GetWriter(context.RunIdentity.PipelineId, "transform"),
                itemActivity: null,
                CancellationToken.None);

            result.Outcome.Should().Be(ItemExecutionOutcome.Emitted);
            policy.ObservedKinds.Should().Equal([RetryKind.ItemRetry, RetryKind.ItemRetry]);
        }
        finally
        {
            await context.DisposeAsync();
        }
    }

    [Fact]
    public async Task ANodeRestart_AsksForANodeRestartDelay()
    {
        RestartRecorder.Reset();

        var services = new ServiceCollection();
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredService<IPipelineRunner>();

        var context = new PipelineContext(PipelineContextConfiguration.Default);
        context.SetSourceData([1]);

        var act = async () => await runner.RunAsync<RestartingPipeline>(context);
        await act.Should().ThrowAsync<NodeExecutionException>();

        RestartRecorder.ObservedKinds.Should().NotBeEmpty("a node restart must consult the delay strategy");
        RestartRecorder.ObservedKinds.Should().OnlyContain(kind => kind == RetryKind.NodeRestart);
    }

    private sealed class FlakyItemTransform(int failuresBeforeSuccess) : TransformNode<int, int>
    {
        private int _attempts;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (_attempts++ < failuresBeforeSuccess)
                throw new InvalidOperationException("transient");

            return ValueTask.FromResult(item);
        }
    }

    private static class RestartRecorder
    {
        public static ConcurrentQueue<RetryKind> ObservedKinds { get; private set; } = new();

        public static void Reset()
        {
            ObservedKinds = new ConcurrentQueue<RetryKind>();
        }
    }

    private sealed class RecordingKindPolicy : IResiliencePolicy
    {
        public List<RetryKind> ObservedKinds { get; } = [];

        public ValueTask<TimeSpan> GetRetryDelayAsync(PipelineContext context, RetryKind retryKind, int attemptNumber, CancellationToken cancellationToken)
        {
            ObservedKinds.Add(retryKind);
            return ValueTask.FromResult(TimeSpan.Zero);
        }

        public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(ITransformNode<TIn, TOut> node, TIn failedItem, Exception exception,
            PipelineContext context, string nodeId, int retryAttempt, CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Retry);
        }

        public Task<ResilienceDecision> DecideNodeFailureAsync(NodeDefinition nodeDefinition, INode node, Exception exception,
            PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecidePipelineFailureAsync(string nodeId, Exception exception, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId)
        {
            return DefaultResiliencePolicy.Instance.GetCircuitBreaker(context, nodeId);
        }
    }

    private sealed class AlwaysFailingTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("boom");
        }
    }

    private sealed class RestartRecordingPolicy : IResiliencePolicy
    {
        public ValueTask<TimeSpan> GetRetryDelayAsync(PipelineContext context, RetryKind retryKind, int attemptNumber, CancellationToken cancellationToken)
        {
            RestartRecorder.ObservedKinds.Enqueue(retryKind);
            return ValueTask.FromResult(TimeSpan.Zero);
        }

        public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(ITransformNode<TIn, TOut> node, TIn failedItem, Exception exception,
            PipelineContext context, string nodeId, int retryAttempt, CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecideNodeFailureAsync(NodeDefinition nodeDefinition, INode node, Exception exception,
            PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.RestartNode);
        }

        public Task<ResilienceDecision> DecidePipelineFailureAsync(string nodeId, Exception exception, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.RestartNode);
        }

        public IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId)
        {
            return DefaultResiliencePolicy.Instance.GetCircuitBreaker(context, nodeId);
        }
    }

    private sealed class RestartingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<int>("retrykind-src");
            var transform = builder.AddTransform<AlwaysFailingTransform, int, int>("retrykind-tx");
            var sink = builder.AddInMemorySink<int>("retrykind-snk");
            _ = builder.Connect(source, transform).Connect(transform, sink);
            builder.AddResiliencePolicy<RestartRecordingPolicy>();
            builder.WithResilience(transform);
            builder.WithRetryOptions(o => o with { MaxNodeRestartAttempts = 2, MaxMaterializedItems = 128 });
        }
    }
}

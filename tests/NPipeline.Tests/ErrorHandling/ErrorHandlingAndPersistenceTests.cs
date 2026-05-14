using System.Collections.Immutable;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Services;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;
using NPipeline.State;

namespace NPipeline.Tests.ErrorHandling;

public sealed class ErrorHandlingAndPersistenceTests
{
    [Fact]
    public async Task ErrorHandling_Retries_Until_Limit()
    {
        // Arrange
        var svc = new ErrorHandlingService();
        var context = PipelineContext.Default;
        var policy = new RestartingPolicy(2);
        context.ResiliencePolicy = policy;

        var nodeDef = new NodeDefinition(
            new NodeIdentity("s1", "s1"),
            new NodeTypeSystem(typeof(FailingSourceNode), NodeKind.Source, null, typeof(object)),
            new NodeExecutionConfig(
                new ResilientExecutionStrategy(new SequentialExecutionStrategy())),
            new NodeMergeConfig(),
            new NodeLineageConfig());

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableList.Create(nodeDef))
            .WithEdges(ImmutableList<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();

        var node = new FailingSourceNode();

        // Act & Assert
        await Assert.ThrowsAsync<NodeExecutionException>(async () =>
            await svc.ExecuteWithRetriesAsync(
                nodeDef,
                node,
                graph,
                context,
                () =>
                {
                    _ = node.OpenStream(context, context.CancellationToken);
                    return Task.CompletedTask;
                },
                context.CancellationToken));

        Assert.Equal(3, node.Attempts); // initial + 2 restarts
        Assert.Equal(3, policy.Calls);
    }

    [Fact]
    public void Persistence_Attempts_Snapshot()
    {
        var persistence = new PersistenceService();

        var ctx = new PipelineContext(
            PipelineContextConfiguration.WithParameters(new Dictionary<string, object>()));

        var sm = new SnapshotStateManager();
        ctx.StateManager = sm;
        var completed = new NodeExecutionCompleted("n1", "Dummy", TimeSpan.FromMilliseconds(5), true, null, Guid.Empty);
        persistence.TryPersistAfterNode(ctx, completed);

        // can't await continuation, but ensure at least snapshot task started
        Assert.Equal(1, sm.Snapshots);
    }

    private sealed class FailingSourceNode : ISourceNode<object>
    {
        public int Attempts { get; private set; }

        public IDataStream<object> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            Attempts++;
            throw new InvalidOperationException("fail");
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DummyStrategy : IExecutionStrategy
    {
        public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(IDataStream<TIn> input, ITransformNode<TIn, TOut> node, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<IDataStream<TOut>>(new NPipeline.DataFlow.DataStreams.InMemoryDataStream<TOut>(new List<TOut>(), "empty"));
        }
    }

    private sealed class RestartingPolicy(int restartLimit) : IResiliencePolicy
    {
        public int Calls { get; private set; }

        public Task<ResilienceDecision> DecideNodeFailureAsync(
            NodeDefinition nodeDefinition,
            INode node,
            Exception exception,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            Calls++;

            if (Calls <= restartLimit)
                return Task.FromResult(ResilienceDecision.Retry);

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

    private sealed class SnapshotStateManager : IPipelineStateManager
    {
        public int Snapshots { get; private set; }

        public ValueTask CreateSnapshotAsync(PipelineContext context, CancellationToken cancellationToken, bool forceFullSnapshot = false)
        {
            Snapshots++;
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TryRestoreAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(false);
        }

        public void MarkNodeCompleted(string nodeId, PipelineContext context)
        {
        }

        public void MarkNodeError(string nodeId, PipelineContext context)
        {
        }
    }
}

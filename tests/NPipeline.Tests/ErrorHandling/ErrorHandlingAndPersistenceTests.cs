using System.Collections.Immutable;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.State;

namespace NPipeline.Tests.ErrorHandling;

public sealed class ErrorHandlingAndPersistenceTests
{
    [Fact]
    public async Task ErrorHandling_Retries_Until_Limit()
    {
        // Arrange
        var svc = ErrorHandlingService.Instance;
        var context = PipelineContext.Default;
        var handler = new TestErrorHandler(2);
        context.PipelineErrorHandler = handler;

        var nodeDef = new NodeDefinition(
            "s1", "s1", typeof(FailingSourceNode), NodeKind.Source,
            null, null, null, typeof(object));

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
                    _ = node.Initialize(context, context.CancellationToken);
                    return Task.CompletedTask;
                },
                context.CancellationToken));

        Assert.Equal(3, node.Attempts); // initial + 2 restarts
        Assert.Equal(3, handler.Calls);
    }

    [Fact]
    public void Persistence_Attempts_Snapshot()
    {
        var persistence = PersistenceService.Instance;

        var ctx = new PipelineContextBuilder()
            .WithParameters(new Dictionary<string, object>())
            .Build();

        var sm = new SnapshotStateManager();
        ctx.Properties[PipelineContextKeys.StateManager] = sm;
        var completed = new NodeExecutionCompleted("n1", "Dummy", TimeSpan.FromMilliseconds(5), true, null);
        persistence.TryPersistAfterNode(ctx, completed);

        // can't await continuation, but ensure at least snapshot task started
        Assert.Equal(1, sm.Snapshots);
    }

    private sealed class FailingSourceNode : ISourceNode<object>
    {
        public int Attempts { get; private set; }

        public IDataPipe<object> Initialize(PipelineContext context, CancellationToken cancellationToken)
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
        public Task<IDataPipe<TOut>> ExecuteAsync<TIn, TOut>(IDataPipe<TIn> input, ITransformNode<TIn, TOut> node, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<IDataPipe<TOut>>(new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<TOut>(new List<TOut>(), "empty"));
        }
    }

    private sealed class TestErrorHandler(int restartLimit) : IPipelineErrorHandler
    {
        public int Calls { get; private set; }

        public Task<PipelineErrorDecision> HandleNodeFailureAsync(string nodeId, Exception ex, PipelineContext context, CancellationToken cancellationToken)
        {
            Calls++;

            if (Calls <= restartLimit)
                return Task.FromResult(PipelineErrorDecision.RestartNode);

            return Task.FromResult(PipelineErrorDecision.FailPipeline);
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

using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Services;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;
using NPipeline.State;

namespace NPipeline.Tests.ErrorHandling;

public sealed class ErrorHandlingAndPersistenceTests
{
    [Fact]
    public async Task ErrorHandling_Retries_Until_Limit()
    {
        // Arrange
        var svc = new ErrorHandlingService();
        var context = PipelineContext.CreateDefault();
        var policy = new RestartingPolicy(2);
        context.ExecutionConfiguration.ResiliencePolicy = policy;

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
    public async Task Persistence_Attempts_Snapshot()
    {
        var persistence = new PersistenceService();

        var ctx = new PipelineContext(
            PipelineContextConfiguration.WithParameters(new Dictionary<string, object>()));

        var sm = new SnapshotStateManager();
        ctx.StateManager = sm;
        var completed = new NodeExecutionCompleted("n1", "Dummy", TimeSpan.FromMilliseconds(5), true, null, Guid.Empty);
        await persistence.TryPersistAfterNode(ctx, completed);

        Assert.Equal(1, sm.Snapshots);
    }

    [Fact]
    public async Task SnapshotCompletesBeforeTheRunReturns()
    {
        // Arrange
        var manager = new SlowSnapshotStateManager();
        await using var context = PipelineContext.CreateDefault();
        context.StateManager = manager;

        // Act
        await PipelineRunner.Create().RunAsync<SnapshotPipeline>(context, CancellationToken.None);

        // Assert - a fire-and-forget snapshot could still be running after the run returned and the context was disposed
        manager.CompletedBeforeContextDisposed.Should().BeTrue();
    }

    private sealed class SlowSnapshotStateManager : IPipelineStateManager
    {
        public volatile bool CompletedBeforeContextDisposed;

        public async ValueTask CreateSnapshotAsync(PipelineContext context, CancellationToken cancellationToken, bool forceFullSnapshot = false)
        {
            await Task.Delay(200, CancellationToken.None).ConfigureAwait(false);

            // The context's Items are cleared when it is disposed; reading one after that throws.
            _ = context.Items.Count;
            CompletedBeforeContextDisposed = true;
        }

        public ValueTask<bool> TryRestoreAsync(PipelineContext context, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public void MarkNodeCompleted(string nodeId, PipelineContext context)
        {
        }

        public void MarkNodeError(string nodeId, PipelineContext context)
        {
        }
    }

    private sealed class SnapshotSource : ISourceNode<int>
    {
        public IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([1], "snapshot-source");

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class SnapshotSink : ISinkNode<int>
    {
        public async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.ToAsyncEnumerable(cancellationToken).ConfigureAwait(false))
            {
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class SnapshotPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SnapshotSource, int>("source");
            var sink = builder.AddSink<SnapshotSink, int>("sink");
            _ = builder.Connect(source, sink);
        }
    }

    private sealed class FailingSourceNode : ISourceNode<object>
    {
        public int Attempts { get; private set; }

        public IDataStream<object> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            Attempts++;
            throw new InvalidOperationException("fail");
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class DummyStrategy : IExecutionStrategy
    {
        public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(IDataStream<TIn> input, ITransformNode<TIn, TOut> node, PipelineContext context,
            string nodeId, CancellationToken cancellationToken) =>
            Task.FromResult<IDataStream<TOut>>(new NPipeline.DataFlow.DataStreams.InMemoryDataStream<TOut>(new List<TOut>(), "empty"));
    }

    private sealed class RestartingPolicy(int restartLimit) : IResiliencePolicy
    {
        public int Calls { get; private set; }

        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
        {
            Calls++;

            if (Calls <= restartLimit)
                return ValueTask.FromResult(ResilienceDecision.Retry);

            return ValueTask.FromResult(ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);
    }

    private sealed class SnapshotStateManager : IPipelineStateManager
    {
        public int Snapshots { get; private set; }

        public ValueTask CreateSnapshotAsync(PipelineContext context, CancellationToken cancellationToken, bool forceFullSnapshot = false)
        {
            Snapshots++;
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TryRestoreAsync(PipelineContext context, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public void MarkNodeCompleted(string nodeId, PipelineContext context)
        {
        }

        public void MarkNodeError(string nodeId, PipelineContext context)
        {
        }
    }
}

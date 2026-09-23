using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.ErrorHandling;

/// <summary>
///     <see cref="PipelineContext.LastRetryExhaustedException" /> is a hand-off slot: a node that exhausts its retries
///     leaves the root cause there so the error handler reporting the resulting downstream failure can name it. It used
///     to be written and never cleared, so once any node exhausted its retries, every later failure in the run was
///     reported with the earlier node's message and inner exception but attributed to the later node's id.
/// </summary>
public sealed class RetryExhaustedAttributionTests
{
    [Fact]
    public async Task TakingThePendingException_ClearsTheSlot()
    {
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);
        var exhausted = new RetryExhaustedException("upstream", 3, new InvalidOperationException("root cause"));

        context.ExecutionConfiguration.LastRetryExhaustedException = exhausted;

        context.ExecutionConfiguration.TakeLastRetryExhaustedException().Should().BeSameAs(exhausted);
        context.ExecutionConfiguration.TakeLastRetryExhaustedException().Should().BeNull("a second failure must not inherit the first one's root cause");
        context.ExecutionConfiguration.LastRetryExhaustedException.Should().BeNull();
    }

    [Fact]
    public async Task TakingFromAnEmptySlot_ReturnsNull()
    {
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);

        context.ExecutionConfiguration.TakeLastRetryExhaustedException().Should().BeNull();
    }

    [Fact]
    public async Task ReadingThePropertyDoesNotConsumeTheSlot()
    {
        // The public property stays a plain read; only the internal take consumes. Anything observing the context
        // for diagnostics must not silently steal the hand-off from error handling.
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);
        var exhausted = new RetryExhaustedException("upstream", 3, new InvalidOperationException("root cause"));

        context.ExecutionConfiguration.LastRetryExhaustedException = exhausted;

        _ = context.ExecutionConfiguration.LastRetryExhaustedException;
        _ = context.ExecutionConfiguration.LastRetryExhaustedException;

        context.ExecutionConfiguration.TakeLastRetryExhaustedException().Should().BeSameAs(exhausted);
    }

    /// <summary>
    ///     Under parallel node execution two nodes can fail at once. Exactly one of them may claim the pending root
    ///     cause; the other must report its own failure rather than duplicating someone else's.
    /// </summary>
    [Fact]
    public async Task ConcurrentTakes_HandTheExceptionToExactlyOneCaller()
    {
        const int attempts = 64;

        await using var context = new PipelineContext(PipelineContextConfiguration.Default);
        var exhausted = new RetryExhaustedException("upstream", 3, new InvalidOperationException("root cause"));

        context.ExecutionConfiguration.LastRetryExhaustedException = exhausted;

        using Barrier gate = new(4);

        var claims = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            gate.SignalAndWait();
            var claimed = 0;

            for (var i = 0; i < attempts; i++)
            {
                if (context.ExecutionConfiguration.TakeLastRetryExhaustedException() is not null)
                    claimed++;
            }

            return claimed;
        })));

        claims.Sum().Should().Be(1, "the slot holds one exception and exactly one caller may take it");
    }

    /// <summary>
    ///     The defect end to end: with a root cause pending, two consecutive node failures both used to be reported
    ///     with the pending exception's message and inner exception, so the second node's own failure vanished.
    /// </summary>
    [Fact]
    public async Task OnlyTheFirstFailingNode_ReportsThePendingRootCause()
    {
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);

        var rootCause = new RetryExhaustedException("upstream", 3, new InvalidOperationException("the real problem"));
        context.ExecutionConfiguration.LastRetryExhaustedException = rootCause;

        ErrorHandlingService service = new();

        var first = await CaptureFailureAsync(service, context, "first", new InvalidDataException("first node's own failure"));
        var second = await CaptureFailureAsync(service, context, "second", new InvalidDataException("second node's own failure"));

        first.NodeId.Should().Be("first");
        first.InnerException.Should().BeSameAs(rootCause, "the node that consumed the hand-off reports the real root cause");

        second.NodeId.Should().Be("second");
        second.InnerException.Should().NotBeSameAs(rootCause, "the root cause was already consumed by the first failure");
        second.InnerException?.Message.Should().Be("second node's own failure", "a node must be reported with its own failure");
    }

    private static async Task<NodeExecutionException> CaptureFailureAsync(
        ErrorHandlingService service,
        PipelineContext context,
        string nodeId,
        Exception failure)
    {
        var nodeDef = new NodeDefinition(
            new NodeIdentity(nodeId, nodeId),
            new NodeTypeSystem(typeof(FailingNode), NodeKind.Source, null, typeof(object)),
            new NodeExecutionConfig(),
            new NodeMergeConfig(),
            new NodeLineageConfig());

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableList.Create(nodeDef))
            .WithEdges(ImmutableList<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();

        var act = () => service.ExecuteWithRetriesAsync(
            nodeDef,
            new FailingNode(),
            graph,
            context,
            () => throw failure,
            CancellationToken.None);

        var thrown = await act.Should().ThrowAsync<NodeExecutionException>();
        return thrown.Which;
    }

    private sealed class FailingNode : INode
    {
        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}

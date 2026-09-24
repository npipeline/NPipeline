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
///     A node that exhausted its retries is reported by the <see cref="RetryExhaustedException" /> it throws, which names
///     the node and its attempts. The node reading the failed stream reports that exception as its cause. There is no
///     hand-off slot on the context any more: it used to attribute one node's root cause to a later node's failure.
/// </summary>
public sealed class RetryExhaustedAttributionTests
{
    [Fact]
    public async Task AnUpstreamRetryExhaustedFailure_IsReportedAsTheCause()
    {
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);
        var exhausted = new RetryExhaustedException("upstream", 3, new InvalidOperationException("the real problem"));

        // The downstream node sees the upstream failure wrapped by whatever it was doing when its input failed.
        var failure = await CaptureFailureAsync(new ErrorHandlingService(), context, "downstream",
            new InvalidDataException("reading the input failed", exhausted));

        failure.NodeId.Should().Be("downstream");
        failure.InnerException.Should().BeSameAs(exhausted);
        ((RetryExhaustedException)failure.InnerException!).NodeId.Should().Be("upstream");
    }

    [Fact]
    public async Task ConsecutiveFailures_AreEachReportedWithTheirOwnCause()
    {
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);
        var exhausted = new RetryExhaustedException("upstream", 3, new InvalidOperationException("the real problem"));
        ErrorHandlingService service = new();

        var first = await CaptureFailureAsync(service, context, "first", exhausted);
        var second = await CaptureFailureAsync(service, context, "second", new InvalidDataException("second node's own failure"));

        first.InnerException.Should().BeSameAs(exhausted);
        second.NodeId.Should().Be("second");
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
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

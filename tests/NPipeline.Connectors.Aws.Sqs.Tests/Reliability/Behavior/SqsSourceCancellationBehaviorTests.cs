using Amazon.SQS;
using Amazon.SQS.Model;
using FakeItEasy;
using NPipeline.Connectors.Aws.Sqs.Nodes;
using NPipeline.Pipeline;
using static NPipeline.Connectors.Aws.Sqs.Tests.SqsTestSupport;

namespace NPipeline.Connectors.Aws.Sqs.Tests.Reliability.Behavior;

/// <summary>
///     Cancellation handling in the SQS source's receive loop (S1 in <c>plans/resilience-improvements.md</c>).
/// </summary>
public sealed class SqsSourceCancellationBehaviorTests
{
    [Fact]
    public async Task PipelineCancellation_SurfacesAsCancellation()
    {
        var client = A.Fake<IAmazonSQS>();

        A.CallTo(() => client.ReceiveMessageAsync(A<ReceiveMessageRequest>._, A<CancellationToken>._))
            .Returns(new ReceiveMessageResponse { Messages = [] });

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await using var node = CreateNode(client);

        var act = () => DrainAsync(node, cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>("a cancelled source must not look like one that drained");
    }

    [Fact]
    public async Task PipelineCancellation_DuringALongPoll_SurfacesAsCancellation()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await using var node = CreateNode(ReceivingClient());

        var act = () => DrainAsync(node, cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task CancellationNotRequestedByThePipeline_FailsTheStream()
    {
        // An SDK timeout surfaces as TaskCanceledException. It is a failure, not a request to shut down.
        var client = A.Fake<IAmazonSQS>();

        A.CallTo(() => client.ReceiveMessageAsync(A<ReceiveMessageRequest>._, A<CancellationToken>._))
            .ThrowsAsync(new TaskCanceledException("request timed out"));

        await using var node = CreateNode(client);

        var act = () => DrainAsync(node, CancellationToken.None);

        _ = await act.Should().ThrowAsync<OperationCanceledException>("the stream must not end as if it had succeeded");
    }

    private static SqsSourceNode<string> CreateNode(IAmazonSQS client) => SqsConnector.Source<string>(QueueUrl, o => o with { Client = client });

    private static async Task DrainAsync(SqsSourceNode<string> node, CancellationToken cancellationToken)
    {
        await foreach (var _ in node.OpenStream(new PipelineContext(), cancellationToken).WithCancellation(cancellationToken))
        {
        }
    }
}

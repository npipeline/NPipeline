using Amazon.SQS;
using Amazon.SQS.Model;
using FakeItEasy;
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Aws.Sqs.Tests.Reliability.Behavior;

/// <summary>
///     Cancellation handling in the SQS source's polling loop (S1 in <c>plans/resilience-improvements.md</c>).
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

        var act = () => DrainAsync(CreateNode(client), cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>("a cancelled source must not look like one that drained");
    }

    [Fact]
    public async Task CancellationNotRequestedByThePipeline_FailsTheStream()
    {
        // An SDK timeout surfaces as TaskCanceledException. It is a failure, not a request to shut down.
        var client = A.Fake<IAmazonSQS>();

        A.CallTo(() => client.ReceiveMessageAsync(A<ReceiveMessageRequest>._, A<CancellationToken>._))
            .ThrowsAsync(new TaskCanceledException("request timed out"));

        var act = () => DrainAsync(CreateNode(client), CancellationToken.None);

        _ = await act.Should().ThrowAsync<OperationCanceledException>("the stream must not end as if it had succeeded");
    }

    private static SqsSourceNode<string> CreateNode(IAmazonSQS client)
    {
        var configuration = new SqsConfiguration
        {
            SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/source-queue",
            PollingIntervalMs = 10,
        };

        return new SqsSourceNode<string>(client, configuration);
    }

    private static async Task DrainAsync(SqsSourceNode<string> node, CancellationToken cancellationToken)
    {
        await foreach (var _ in node.OpenStream(new PipelineContext(), cancellationToken).WithCancellation(cancellationToken))
        {
        }
    }
}

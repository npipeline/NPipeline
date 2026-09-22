using Confluent.Kafka;
using FakeItEasy;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Metrics;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.Connectors.Kafka.Retry;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Kafka.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the Kafka source's consume retry loop (K1, K2 in
///     <c>plans/resilience-improvements.md</c>).
/// </summary>
public sealed class KafkaSourceRetryBehaviorTests
{
    [Fact]
    public async Task PersistentConsumeError_SurfacesAfterMaxRetries()
    {
        const int maxRetries = 3;
        var consumer = A.Fake<IConsumer<string, string>>();

        A.CallTo(() => consumer.Consume(A<TimeSpan>._))
            .Throws(() => new ConsumeException(new ConsumeResult<byte[], byte[]>(), new Error(ErrorCode.Local_Transport)));

        var retryStrategy = new ExponentialBackoffRetryStrategy { MaxRetries = maxRetries, BaseDelayMs = 1, JitterFactor = 0 };
        var node = CreateNode(consumer, retryStrategy);

        // Bounds the test if the defect returns: an endless retry loop is cut off here instead of hanging the run.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var act = () => DrainAsync(node, cts.Token);

        _ = await act.Should().ThrowAsync<ConsumeException>("a consume error that never clears must fail the stream once retries run out");

        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).MustHaveHappened(maxRetries + 1, Times.OrLess);
    }

    [Fact]
    public async Task CancellationNotRequestedByThePipeline_FailsTheStream()
    {
        // An internal timeout surfacing as OperationCanceledException is a failure, not a request to shut down.
        var consumer = A.Fake<IConsumer<string, string>>();
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).Throws(() => new OperationCanceledException("internal timeout"));

        var node = CreateNode(consumer, new ExponentialBackoffRetryStrategy { BaseDelayMs = 1 });

        var act = () => DrainAsync(node, CancellationToken.None);

        _ = await act.Should().ThrowAsync<OperationCanceledException>("the stream must not end as if it had succeeded");
    }

    [Fact]
    public async Task PipelineCancellation_EndsTheStreamGracefully()
    {
        var consumer = A.Fake<IConsumer<string, string>>();
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).Returns(null!);

        var node = CreateNode(consumer, new ExponentialBackoffRetryStrategy { BaseDelayMs = 1 });
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        var act = () => DrainAsync(node, cts.Token);

        await act.Should().NotThrowAsync("shutting the pipeline down closes the consumer and ends the stream");
    }

    private static KafkaSourceNode<string> CreateNode(IConsumer<string, string> consumer, IRetryStrategy retryStrategy)
    {
        var configuration = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "orders",
            ConsumerGroupId = "reliability-tests",
            PollTimeoutMs = 1,
        };

        return new KafkaSourceNode<string>(consumer, configuration, NullKafkaMetrics.Instance, retryStrategy);
    }

    private static async Task DrainAsync(KafkaSourceNode<string> node, CancellationToken cancellationToken)
    {
        await foreach (var _ in node.OpenStream(new PipelineContext(), cancellationToken).WithCancellation(cancellationToken))
        {
        }
    }
}

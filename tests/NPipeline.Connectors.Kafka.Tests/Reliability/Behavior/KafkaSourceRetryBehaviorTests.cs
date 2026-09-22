using Confluent.Kafka;
using FakeItEasy;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Metrics;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.Connectors.Kafka.Retry;
using NPipeline.Pipeline;

// Tests skipped with a defect ID pin known bugs; the phase that fixes each one removes its skip.
#pragma warning disable xUnit1004

namespace NPipeline.Connectors.Kafka.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the Kafka source's consume retry loop. IDs refer to the defect register in
///     <c>plans/resilience-improvements.md</c>.
/// </summary>
public sealed class KafkaSourceRetryBehaviorTests
{
    private const string K1 = "K1 (Phase 1): consume errors retry forever because the attempt counter resets after every batch";

    [Fact(Skip = K1)]
    public async Task PersistentConsumeError_SurfacesAfterMaxRetries()
    {
        const int maxRetries = 3;
        var consumer = A.Fake<IConsumer<string, string>>();

        A.CallTo(() => consumer.Consume(A<TimeSpan>._))
            .Throws(() => new ConsumeException(new ConsumeResult<byte[], byte[]>(), new Error(ErrorCode.Local_Transport)));

        var configuration = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "orders",
            ConsumerGroupId = "reliability-tests",
        };

        var retryStrategy = new ExponentialBackoffRetryStrategy { MaxRetries = maxRetries, BaseDelayMs = 1, JitterFactor = 0 };
        var node = new KafkaSourceNode<string>(consumer, configuration, NullKafkaMetrics.Instance, retryStrategy);

        // Bounds the test when the defect is present. Today the timeout ends the stream silently, as a success (K2).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var act = async () =>
        {
            await foreach (var _ in node.OpenStream(new PipelineContext(), cts.Token).WithCancellation(cts.Token))
            {
            }
        };

        _ = await act.Should().ThrowAsync<ConsumeException>("a consume error that never clears must fail the stream once retries run out");

        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).MustHaveHappened(maxRetries + 1, Times.OrLess);
    }
}

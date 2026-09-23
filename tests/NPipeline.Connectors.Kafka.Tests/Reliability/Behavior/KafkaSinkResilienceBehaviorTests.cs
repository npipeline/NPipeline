using Confluent.Kafka;
using FakeItEasy;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Metrics;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.Connectors.Kafka.Tests.Fixtures;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Kafka.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the Kafka sink's produce (K3 in <c>plans/resilience-improvements.md</c>): librdkafka is the
///     only layer that retries a produce.
/// </summary>
/// <remarks>
///     The producer is a fake. The sink still reads the topic's partition count from a broker when it starts, so these
///     tests use the shared container to keep that lookup fast.
/// </remarks>
[Collection("Kafka")]
public sealed class KafkaSinkResilienceBehaviorTests(KafkaTestContainerFixture fixture)
{
    [Theory]
    [InlineData(ErrorCode.Local_MsgTimedOut)]
    [InlineData(ErrorCode.NotEnoughReplicas)]
    [InlineData(ErrorCode.Local_Transport)]
    [InlineData(ErrorCode.MsgSizeTooLarge)]
    public async Task ProduceError_IsNotRetriedAboveLibrdkafka(ErrorCode code)
    {
        var producer = FailingProducer(code);
        var metrics = A.Fake<IKafkaMetrics>();
        var sink = new KafkaSinkNode<string>(producer, CreateConfiguration(1), metrics);

        var act = () => RunAsync(sink, "order-1");

        _ = await act.Should().ThrowAsync<ProduceException<string, string>>();

        // librdkafka has already retried anything retriable. A second produce is a new record the idempotent producer
        // cannot deduplicate, so a delivery that timed out but reached the broker would be written twice.
        ProduceCount(producer).Should().Be(1);
        A.CallTo(() => metrics.RecordProduceError(A<string>._, A<Exception>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task ProduceError_WithContinueOnError_SkipsTheMessageWithoutRetrying()
    {
        var producer = FailingProducer(ErrorCode.Local_MsgTimedOut);
        var configuration = CreateConfiguration(1);
        configuration.ContinueOnError = true;

        var sink = new KafkaSinkNode<string>(producer, configuration, NullKafkaMetrics.Instance);

        await RunAsync(sink, "order-1", "order-2");

        ProduceCount(producer).Should().Be(2, "each message is produced once");
    }

    [Fact]
    public async Task BatchedProduceError_IsNotRetried()
    {
        var producer = FailingProducer(ErrorCode.Local_MsgTimedOut);
        var sink = new KafkaSinkNode<string>(producer, CreateConfiguration(2), NullKafkaMetrics.Instance);

        await RunAsync(sink, "order-1", "order-2");

        ProduceCount(producer).Should().Be(2, "each message is produced once");
    }

    [Fact]
    public async Task PipelineCancellation_StopsTheSink()
    {
        var producer = A.Fake<IProducer<string, string>>();
        using var cts = new CancellationTokenSource();

        // The pipeline is cancelled while the produce waits for its delivery report.
        A.CallTo(() => producer.ProduceAsync(A<string>._, A<Message<string, string>>._, A<CancellationToken>._))
            .ReturnsLazily(async call =>
            {
                cts.CancelAfter(TimeSpan.FromMilliseconds(50));
                await Task.Delay(Timeout.Infinite, call.GetArgument<CancellationToken>(2)).ConfigureAwait(false);
                return new DeliveryResult<string, string>();
            });

        var sink = new KafkaSinkNode<string>(producer, CreateConfiguration(1), NullKafkaMetrics.Instance);

        var act = () => RunAsync(sink, cts.Token, "order-1", "order-2");

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        ProduceCount(producer).Should().Be(1);
    }

    private static IProducer<string, string> FailingProducer(ErrorCode code)
    {
        var producer = A.Fake<IProducer<string, string>>();

        A.CallTo(() => producer.ProduceAsync(A<string>._, A<Message<string, string>>._, A<CancellationToken>._))
            .ThrowsAsync(() => new ProduceException<string, string>(new Error(code), new DeliveryResult<string, string>()));

        return producer;
    }

    private static int ProduceCount(IProducer<string, string> producer)
    {
        return Fake.GetCalls(producer).Count(call => call.Method.Name == nameof(IProducer<string, string>.ProduceAsync));
    }

    private KafkaConfiguration CreateConfiguration(int batchSize)
    {
        return new KafkaConfiguration
        {
            BootstrapServers = fixture.BootstrapServers,
            SinkTopic = $"resilience-{Guid.NewGuid():N}",
            BatchSize = batchSize,
            BatchLingerMs = 0,
        };
    }

    private static Task RunAsync(KafkaSinkNode<string> sink, params string[] items)
    {
        return RunAsync(sink, CancellationToken.None, items);
    }

    private static async Task RunAsync(KafkaSinkNode<string> sink, CancellationToken cancellationToken, params string[] items)
    {
        await using var input = new InMemoryDataStream<string>(items);
        await sink.ConsumeAsync(input, new PipelineContext(), cancellationToken);
    }
}

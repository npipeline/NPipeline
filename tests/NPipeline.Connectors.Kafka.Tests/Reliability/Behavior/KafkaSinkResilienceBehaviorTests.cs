using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FakeItEasy;
using NPipeline.Connectors.Abstractions;
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
    public async Task BatchedProduceError_FailsTheNode_WithoutRetrying()
    {
        var producer = FailingProducer(ErrorCode.Local_MsgTimedOut);
        var metrics = A.Fake<IKafkaMetrics>();
        var sink = new KafkaSinkNode<string>(producer, CreateConfiguration(2), metrics);

        var act = () => RunAsync(sink, "order-1", "order-2");

        _ = await act.Should().ThrowAsync<ProduceException<string, string>>("ContinueOnError is off");
        ProduceCount(producer).Should().Be(2, "each message is produced once");
        A.CallTo(() => metrics.RecordProduceError(A<string>._, A<Exception>._)).MustHaveHappenedTwiceExactly();
    }

    [Fact]
    public async Task BatchedProduceError_WithContinueOnError_IsRecordedAndSkipped()
    {
        var producer = FailingProducer(ErrorCode.Local_MsgTimedOut);
        var metrics = A.Fake<IKafkaMetrics>();
        var configuration = CreateConfiguration(2);
        configuration.ContinueOnError = true;

        var sink = new KafkaSinkNode<string>(producer, configuration, metrics);

        await RunAsync(sink, "order-1", "order-2");

        ProduceCount(producer).Should().Be(2);
        A.CallTo(() => metrics.RecordProduceError(A<string>._, A<Exception>._)).MustHaveHappenedTwiceExactly();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BatchedProduce_AcknowledgesTheMessagesThatWereDelivered_EvenWhenOneFails(bool continueOnError)
    {
        var producer = A.Fake<IProducer<string, IAcknowledgableMessage>>();

        A.CallTo(() => producer.ProduceAsync(A<string>._, A<Message<string, IAcknowledgableMessage>>._, A<CancellationToken>._))
            .ReturnsLazily(call =>
            {
                var message = call.GetArgument<Message<string, IAcknowledgableMessage>>(1)!;

                return (string)message.Value.Body == "order-2"
                    ? Task.FromException<DeliveryResult<string, IAcknowledgableMessage>>(
                        new ProduceException<string, IAcknowledgableMessage>(new Error(ErrorCode.MsgSizeTooLarge),
                            new DeliveryResult<string, IAcknowledgableMessage>()))
                    : Task.FromResult(new DeliveryResult<string, IAcknowledgableMessage> { Status = PersistenceStatus.Persisted });
            });

        var messages = Enumerable.Range(1, 3).Select(i => AcknowledgableMessage($"order-{i}")).ToList();
        var configuration = CreateConfiguration(3);
        configuration.ContinueOnError = continueOnError;

        var sink = new KafkaSinkNode<IAcknowledgableMessage>(producer, configuration, NullKafkaMetrics.Instance);
        await using var input = new InMemoryDataStream<IAcknowledgableMessage>(messages);

        var act = () => sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);

        if (continueOnError)
            await act.Should().NotThrowAsync();
        else
            _ = await act.Should().ThrowAsync<ProduceException<string, IAcknowledgableMessage>>();

        A.CallTo(() => messages[0].AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => messages[1].AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => messages[2].AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    // Acknowledgable messages

    [Fact]
    public async Task AcknowledgableMessage_IsProducedAndThenAcknowledged()
    {
        var producer = A.Fake<IProducer<string, IAcknowledgableMessage>>();

        A.CallTo(() => producer.ProduceAsync(A<string>._, A<Message<string, IAcknowledgableMessage>>._, A<CancellationToken>._))
            .Returns(new DeliveryResult<string, IAcknowledgableMessage> { Status = PersistenceStatus.Persisted });

        var message = AcknowledgableMessage("order-1");
        var sink = new KafkaSinkNode<IAcknowledgableMessage>(producer, CreateConfiguration(1), NullKafkaMetrics.Instance);

        await using var input = new InMemoryDataStream<IAcknowledgableMessage>([message]);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);

        A.CallTo(() => producer.ProduceAsync(
                A<string>._,
                A<Message<string, IAcknowledgableMessage>>.That.Matches(m => m.Value == message && m.Key == "order-1"),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly()
            .Then(A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly());
    }

    [Fact]
    public async Task AcknowledgableMessage_IsNotAcknowledged_WhenItsProduceFails()
    {
        var producer = A.Fake<IProducer<string, IAcknowledgableMessage>>();

        A.CallTo(() => producer.ProduceAsync(A<string>._, A<Message<string, IAcknowledgableMessage>>._, A<CancellationToken>._))
            .ThrowsAsync(new ProduceException<string, IAcknowledgableMessage>(new Error(ErrorCode.Local_MsgTimedOut),
                new DeliveryResult<string, IAcknowledgableMessage>()));

        var message = AcknowledgableMessage("order-1");
        var configuration = CreateConfiguration(1);
        configuration.ContinueOnError = true;

        var sink = new KafkaSinkNode<IAcknowledgableMessage>(producer, configuration, NullKafkaMetrics.Instance);

        await using var input = new InMemoryDataStream<IAcknowledgableMessage>([message]);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);

        A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task AcknowledgableMessage_IsWrittenAsItsBody_ToARealBroker()
    {
        var configuration = CreateConfiguration(1);

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = fixture.BootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = configuration.SinkTopic, NumPartitions = 1, ReplicationFactor = 1 }]);
        }

        var message = AcknowledgableMessage("order-1");
        await using var sink = new KafkaSinkNode<IAcknowledgableMessage>(configuration);

        await using (var input = new InMemoryDataStream<IAcknowledgableMessage>([message]))
        {
            await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
        }

        A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = fixture.BootstrapServers,
            GroupId = $"verify-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).Build();

        consumer.Subscribe(configuration.SinkTopic);
        var record = consumer.Consume(TimeSpan.FromSeconds(20));
        consumer.Close();

        record.Should().NotBeNull();
        record!.Message.Key.Should().Be("order-1");
        record.Message.Value.Should().Be("\"order-1\"", "the body is serialized, not the wrapper around it");
    }

    // Startup

    [Fact]
    public async Task UnreachableBrokers_FailStartupPromptly_WithAClearError()
    {
        var configuration = UnreachableConfiguration();
        configuration.MetadataTimeoutMs = 500;

        var sink = new KafkaSinkNode<string>(A.Fake<IProducer<string, string>>(), configuration, NullKafkaMetrics.Instance);
        var started = DateTime.UtcNow;

        var act = () => RunAsync(sink, "order-1");

        (await act.Should().ThrowAsync<InvalidOperationException>())
            .WithMessage("*metadata*MetadataTimeoutMs*")
            .WithInnerException<KafkaException>();

        (DateTime.UtcNow - started).Should().BeLessThan(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task PipelineCancellation_DuringStartup_IsHonored()
    {
        var configuration = UnreachableConfiguration();
        configuration.MetadataTimeoutMs = 60000;

        var producer = A.Fake<IProducer<string, string>>();
        var sink = new KafkaSinkNode<string>(producer, configuration, NullKafkaMetrics.Instance);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var started = DateTime.UtcNow;

        var act = () => RunAsync(sink, cts.Token, "order-1");

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        (DateTime.UtcNow - started).Should().BeLessThan(TimeSpan.FromSeconds(5), "cancellation must not wait out the metadata timeout");
        ProduceCount(producer).Should().Be(0);
    }

    [Fact]
    public async Task PipelineCancellation_DuringInitTransactions_IsHonored_AndDisposeWaitsForTheInit()
    {
        using var release = new ManualResetEventSlim();
        var initReturned = false;
        var flushedBeforeInitReturned = false;

        var producer = A.Fake<IProducer<string, string>>();

        A.CallTo(() => producer.Flush(A<CancellationToken>._)).Invokes(() => flushedBeforeInitReturned |= !initReturned);

        var configuration = CreateConfiguration(1);
        configuration.EnableTransactions = true;
        configuration.TransactionalId = $"tx-{Guid.NewGuid():N}";
        configuration.DeliverySemantic = DeliverySemantic.ExactlyOnce;
        configuration.TransactionInitTimeoutMs = 30000;

        var sink = new KafkaSinkNode<string>(producer, configuration, NullKafkaMetrics.Instance);

        // A cluster that never answers: InitTransactions blocks until the test releases it. The pipeline is cancelled
        // once the sink is waiting on it, after the metadata lookup.
        using var cts = new CancellationTokenSource();
        A.CallTo(() => producer.InitTransactions(A<TimeSpan>._)).Invokes(() =>
        {
            cts.CancelAfter(TimeSpan.FromMilliseconds(100));
            _ = release.Wait(TimeSpan.FromSeconds(30));
            initReturned = true;
        });

        var started = DateTime.UtcNow;
        var act = () => RunAsync(sink, cts.Token, "order-1");

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        (DateTime.UtcNow - started).Should().BeLessThan(TimeSpan.FromSeconds(10), "cancellation must not wait out TransactionInitTimeoutMs");
        A.CallTo(() => producer.BeginTransaction()).MustNotHaveHappened();
        ProduceCount(producer).Should().Be(0);

        // Disposing while the abandoned InitTransactions still runs must wait for it before touching the producer.
        var dispose = sink.DisposeAsync().AsTask();
        await Task.Delay(200);
        dispose.IsCompleted.Should().BeFalse("the producer is still inside InitTransactions");

        release.Set();
        await dispose;

        initReturned.Should().BeTrue();
        flushedBeforeInitReturned.Should().BeFalse();
    }

    [Fact]
    public async Task InitTransactionsFailure_FailsTheNode()
    {
        var producer = A.Fake<IProducer<string, string>>();
        A.CallTo(() => producer.InitTransactions(A<TimeSpan>._)).Throws(new KafkaException(ErrorCode.Local_TimedOut));

        var configuration = CreateConfiguration(1);
        configuration.EnableTransactions = true;
        configuration.TransactionalId = $"tx-{Guid.NewGuid():N}";
        configuration.DeliverySemantic = DeliverySemantic.ExactlyOnce;

        var sink = new KafkaSinkNode<string>(producer, configuration, NullKafkaMetrics.Instance);

        var act = () => RunAsync(sink, "order-1");

        _ = await act.Should().ThrowAsync<KafkaException>();
        A.CallTo(() => producer.BeginTransaction()).MustNotHaveHappened();
    }

    private static KafkaConfiguration UnreachableConfiguration()
    {
        // Nothing listens on port 1.
        return new KafkaConfiguration { BootstrapServers = "127.0.0.1:1", SinkTopic = "orders", BatchSize = 1 };
    }

    private static IAcknowledgableMessage AcknowledgableMessage(string body)
    {
        var message = A.Fake<IAcknowledgableMessage>();
        A.CallTo(() => message.Body).Returns(body);
        A.CallTo(() => message.Metadata).Returns(new Dictionary<string, object>());
        return message;
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

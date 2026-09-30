using System.Text;
using Confluent.Kafka;
using FakeItEasy;
using NPipeline.Connectors.Kafka.DeadLetter;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Messaging;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Kafka.Tests.DeadLetter;

/// <summary>
///     Unit tests for <see cref="KafkaDeadLetterSink" />.
/// </summary>
public sealed class KafkaDeadLetterSinkTests
{
    private static readonly NodeFailureAttribution Attribution = new("transform", "handler", Guid.NewGuid(), Guid.NewGuid());

    private readonly IProducer<byte[]?, byte[]> _producer = A.Fake<IProducer<byte[]?, byte[]>>();
    private readonly List<(string Topic, Message<byte[]?, byte[]> Message)> _produced = [];

    public KafkaDeadLetterSinkTests()
    {
        A.CallTo(() => _producer.ProduceAsync(A<string>._, A<Message<byte[]?, byte[]>>._, A<CancellationToken>._))
            .ReturnsLazily((string topic, Message<byte[]?, byte[]> message, CancellationToken _) =>
            {
                _produced.Add((topic, message));
                return Task.FromResult(new DeliveryResult<byte[]?, byte[]> { Status = PersistenceStatus.Persisted });
            });
    }

    [Fact]
    public void Constructor_RejectsMissingArguments()
    {
        ((Action)(() => _ = new KafkaDeadLetterSink(null!, "dlq"))).Should().Throw<ArgumentNullException>();
        ((Action)(() => _ = new KafkaDeadLetterSink(_producer, null!))).Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public async Task AnyItem_IsSerialized_WithTheErrorInHeaders()
    {
        var sink = new KafkaDeadLetterSink(_producer, "dlq");

        await sink.HandleAsync(new DeadLetterEnvelope(new Order(1), new InvalidOperationException("boom"), Attribution), new PipelineContext(),
            CancellationToken.None);

        var (topic, message) = _produced.Should().ContainSingle().Subject;
        topic.Should().Be("dlq");
        message.Key.Should().BeNull();
        Encoding.UTF8.GetString(message.Value).Should().Be("""{"id":1}""");
        Header(message, "x-dead-letter-reason").Should().Be("boom");
        Header(message, "x-dead-letter-exception").Should().Be(typeof(InvalidOperationException).FullName);
        Header(message, "x-dead-letter-node").Should().Be("handler");
        Header(message, "x-dead-letter-origin-node").Should().Be("transform");
    }

    [Fact]
    public async Task MessageFailure_IsProducedWithItsOriginalBodyAndKey()
    {
        var sink = new KafkaDeadLetterSink(_producer, "dlq");
        var body = Encoding.UTF8.GetBytes("not json");
        var failure = new MessageFailure("orders", "orders/0/7", body, new Dictionary<string, object> { ["Key"] = "order-7" });

        await sink.HandleAsync(new DeadLetterEnvelope(failure, new FormatException("bad"), Attribution), new PipelineContext(), CancellationToken.None);

        var message = _produced.Should().ContainSingle().Subject.Message;
        message.Value.Should().Equal(body);
        Encoding.UTF8.GetString(message.Key!).Should().Be("order-7");
        Header(message, "x-dead-letter-source").Should().Be("orders");
        Header(message, "x-dead-letter-message-id").Should().Be("orders/0/7");
    }

    [Fact]
    public async Task ReceivedKafkaMessage_IsProducedAsItsBody_WithItsKey()
    {
        var serializer = A.Fake<IMessageSerializer>();
        A.CallTo(() => serializer.Serialize(A<object?>._, A<MessageContext>._)).Returns([1, 2, 3]);

        var sink = new KafkaDeadLetterSink(_producer, "dlq", serializer);
        var received = new KafkaMessage<Order>(new Order(3), new TopicPartitionOffset("orders", 1, 42), "order-3", DateTimeOffset.UnixEpoch, [], false,
            new MessageSettlement(_ => Task.CompletedTask, (_, _) => Task.CompletedTask), null);

        await sink.HandleAsync(new DeadLetterEnvelope(received, new InvalidOperationException("boom"), Attribution), new PipelineContext(),
            CancellationToken.None);

        var message = _produced.Should().ContainSingle().Subject.Message;
        message.Value.Should().Equal(1, 2, 3);
        Encoding.UTF8.GetString(message.Key!).Should().Be("order-3");
        Header(message, "x-dead-letter-message-id").Should().Be("orders/1/42");

        A.CallTo(() => serializer.Serialize<object?>(A<object?>.That.IsEqualTo(new Order(3)), new MessageContext("dlq", false)))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task HandleAsync_RejectsANullEnvelope()
    {
        var sink = new KafkaDeadLetterSink(_producer, "dlq");

        var act = () => sink.HandleAsync(null!, new PipelineContext(), CancellationToken.None);

        _ = await act.Should().ThrowAsync<ArgumentNullException>();
    }

    private static string Header(Message<byte[]?, byte[]> message, string key) => Encoding.UTF8.GetString(message.Headers.GetLastBytes(key));

    private sealed record Order(int Id);
}

using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.Tests.Sql;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Connectors.Tests.Messaging;

public sealed class JsonMessageSerializerTests
{
    private static readonly MessageContext Queue = new("orders");

    [Fact]
    public void Writes_the_json_connectors_defaults()
    {
        var bytes = JsonMessageSerializer.Default.Serialize(new Parcel(1, "Ada", Size.Large), Queue);

        Encoding.UTF8.GetString(bytes).Should().Be("""{"id":1,"recipient":"Ada","size":"Large"}""");
    }

    [Fact]
    public void Reads_any_casing()
    {
        JsonMessageSerializer.Default.Deserialize<Parcel>("""{"ID":1,"Recipient":"Ada","size":"large"}"""u8, Queue)
            .Should().Be(new Parcel(1, "Ada", Size.Large));
    }

    [Fact]
    public void Honours_column_attributes()
    {
        var bytes = JsonMessageSerializer.Default.Serialize(new Labelled { Code = "x", Secret = "s" }, Queue);

        Encoding.UTF8.GetString(bytes).Should().Be("""{"label_code":"x"}""");
    }

    [Fact]
    public void Uses_a_source_generated_context()
    {
        var serializer = new JsonMessageSerializer(ParcelContext.Default);

        var bytes = serializer.Serialize(new Parcel(2, "Grace", Size.Small), Queue);

        serializer.Deserialize<Parcel>(bytes, Queue).Should().Be(new Parcel(2, "Grace", Size.Small));
        Encoding.UTF8.GetString(bytes).Should().Contain("\"recipient\"");
    }

    [Fact]
    public void A_json_null_body_is_an_error()
    {
        var read = () => JsonMessageSerializer.Default.Deserialize<Parcel>("null"u8, Queue);

        read.Should().Throw<JsonException>();
    }

    public sealed class Labelled
    {
        [Column("label_code")]
        public string Code { get; set; } = "";

        [IgnoreColumn]
        public string Secret { get; set; } = "";
    }
}

public sealed class MessageDecoderTests
{
    private static readonly IReadOnlyDictionary<string, object> NoMetadata = new Dictionary<string, object>();

    [Fact]
    public void Decodes_a_valid_body()
    {
        var decoder = new MessageDecoder<Parcel>("test", "orders", JsonMessageSerializer.Default, null, 256);

        decoder.TryDecode("""{"id":1,"recipient":"Ada","size":"Small"}"""u8, out var parcel, out _).Should().BeTrue();
        parcel.Should().Be(new Parcel(1, "Ada", Size.Small));
    }

    [Fact]
    public async Task By_default_a_bad_body_fails_with_the_destination_and_an_excerpt()
    {
        var decoder = new MessageDecoder<Parcel>("test", "orders", JsonMessageSerializer.Default, null, 5);
        var body = "not json at all"u8.ToArray();

        decoder.TryDecode(body, out _, out var failure).Should().BeFalse();
        var handle = async () => await decoder.HandleFailureAsync(failure!, body, "m1", 7, NoMetadata, Channel(), default);

        var error = (await handle.Should().ThrowAsync<RecordMappingException>()).Which;
        error.RecordSource.Should().Be("orders");
        error.RecordNumber.Should().Be(7);
        error.RawExcerpt.Should().Be("not j…");
    }

    [Fact]
    public async Task A_skipped_body_returns_the_action()
    {
        var decoder = new MessageDecoder<Parcel>("test", "orders", JsonMessageSerializer.Default, _ => RowErrorAction.Skip, 256);

        (await decoder.HandleFailureAsync(new JsonException("bad"), "{"u8.ToArray(), "m1", 1, NoMetadata, Channel(), default))
            .Should().Be(RowErrorAction.Skip);
    }

    [Fact]
    public async Task A_dead_lettered_body_goes_whole_to_the_dead_letter_sink()
    {
        var sink = new CapturingDeadLetterSink();
        var decoder = new MessageDecoder<Parcel>("test", "orders", JsonMessageSerializer.Default, _ => RowErrorAction.DeadLetter, 2);
        var metadata = new Dictionary<string, object> { ["header"] = "value" };

        var action = await decoder.HandleFailureAsync(new JsonException("bad"), "not json"u8.ToArray(), "m9", 1, metadata, Channel(sink), default);

        action.Should().Be(RowErrorAction.DeadLetter);
        var failure = sink.Captured.Should().ContainSingle().Which.Item.Should().BeOfType<MessageFailure>().Subject;
        failure.Source.Should().Be("orders");
        failure.MessageId.Should().Be("m9");
        Encoding.UTF8.GetString(failure.Body.Span).Should().Be("not json");
        failure.Metadata.Should().ContainKey("header");
    }

    private static DeadLetterChannel Channel(IDeadLetterSink? sink = null) =>
        new ChannelSource().Open(new PipelineContext(new PipelineContextConfiguration(DeadLetterSink: sink)));

    private sealed class ChannelSource : SourceNode<int>
    {
        public DeadLetterChannel Open(PipelineContext context) => OpenDeadLetterChannel(context);

        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}

public sealed class MessageSettlementTests
{
    [Fact]
    public async Task Settles_once_whoever_asks()
    {
        var acknowledged = 0;
        var rejected = 0;
        var settlement = new MessageSettlement(_ => Task.FromResult(++acknowledged), (_, _) => Task.FromResult(++rejected));

        await settlement.AcknowledgeAsync();
        await settlement.AcknowledgeAsync();
        await settlement.RejectAsync(true);

        (acknowledged, rejected).Should().Be((1, 0));
        settlement.IsSettled.Should().BeTrue();
    }

    [Fact]
    public async Task A_copy_with_another_body_shares_the_settlement()
    {
        var message = TestMessage.Create<int>(1);
        var copy = message.WithBody("one");

        await copy.AcknowledgeAsync();

        message.IsSettled.Should().BeTrue();
        message.Acknowledged.Should().Be(1);
    }
}

public sealed class AcknowledgingSinkTests
{
    private readonly FakeDatabase _database = new();

    [Fact]
    public async Task Acknowledges_each_batch_once_the_sql_sink_commits_it()
    {
        var sink = new TestSink<SqlSinkNodeTests.Order>(new TestWriteOptions { Database = _database, Table = "orders", BatchSize = 2 }, new TestDialect());
        var commitsSeen = new List<int>();
        var messages = Enumerable.Range(1, 5).Select(i => TestMessage.Create<SqlSinkNodeTests.Order>(SqlSinkNodeTests.Order.Create(i), () => commitsSeen.Add(_database.Commits))).ToList();

        await WriteAsync(sink.Acknowledging(), messages);

        // Each message is acknowledged after the commit of its own batch, never before.
        commitsSeen.Should().Equal(1, 1, 2, 2, 3);
    }

    [Fact]
    public async Task A_partial_batch_is_written_and_acknowledged_after_the_linger()
    {
        var sink = new TestSink<SqlSinkNodeTests.Order>(
            new TestWriteOptions { Database = _database, Table = "orders", BatchSize = 100, BatchLinger = TimeSpan.FromMilliseconds(50) }, new TestDialect());

        var input = Channel.CreateUnbounded<IAcknowledgableMessage<SqlSinkNodeTests.Order>>();
        var message = TestMessage.Create<SqlSinkNodeTests.Order>(SqlSinkNodeTests.Order.Create(1));
        await input.Writer.WriteAsync(message);

        var write = sink.Acknowledging().ConsumeAsync(new DataStream<IAcknowledgableMessage<SqlSinkNodeTests.Order>>(input.Reader.ReadAllAsync(), "slow"), new PipelineContext(),
            CancellationToken.None);

        // The stream stays open; the linger alone writes the batch.
        var deadline = DateTime.UtcNow.AddSeconds(5);

        while (!message.IsSettled && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        message.IsSettled.Should().BeTrue();
        _database.Commits.Should().Be(1);

        input.Writer.Complete();
        await write;
    }

    [Fact]
    public async Task A_sink_that_reports_nothing_acknowledges_when_the_stream_is_written()
    {
        var inner = new CollectingSink<int>();
        var messages = Enumerable.Range(1, 3).Select(i => TestMessage.Create<int>(i, () => inner.Items.Should().HaveCount(3))).ToList();

        await WriteAsync(inner.Acknowledging(), messages);

        messages.Should().OnlyContain(m => m.Acknowledged == 1);
    }

    [Fact]
    public async Task Nothing_is_acknowledged_when_the_sink_fails()
    {
        var messages = Enumerable.Range(1, 3).Select(i => TestMessage.Create<int>(i)).ToList();

        var write = () => WriteAsync(new FailingSink<int>().Acknowledging(), messages);

        await write.Should().ThrowAsync<InvalidOperationException>();
        messages.Should().OnlyContain(m => !m.IsSettled);
    }

    [Fact]
    public async Task A_message_sink_settles_the_messages_itself()
    {
        var inner = new MessageSinkStub<int>();

        await WriteAsync(inner.Acknowledging(), [TestMessage.Create<int>(1)]);

        inner.Received.Should().Equal(1);
    }

    private static async Task WriteAsync<T>(SinkNode<IAcknowledgableMessage<T>> sink, IEnumerable<IAcknowledgableMessage<T>> messages)
    {
        await using var input = new InMemoryDataStream<IAcknowledgableMessage<T>>([.. messages]);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
    }

    private sealed class CollectingSink<T> : SinkNode<T>
    {
        public List<T> Items { get; } = [];

        public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                Items.Add(item);
            }
        }
    }

    private sealed class FailingSink<T> : SinkNode<T>
    {
        public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }

            throw new InvalidOperationException("write failed");
        }
    }

    private sealed class MessageSinkStub<T> : SinkNode<T>, IMessageSink<T>
    {
        public List<T> Received { get; } = [];

        public override Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("The body path should not be used.");

        public async Task ConsumeMessagesAsync(IDataStream<IAcknowledgableMessage<T>> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var message in input.WithCancellation(cancellationToken))
            {
                Received.Add(message.Body);
                await message.AcknowledgeAsync(cancellationToken);
            }
        }
    }
}

public sealed record Parcel(int Id, string Recipient, Size Size);

public enum Size
{
    Small,
    Large,
}

[JsonSerializable(typeof(Parcel))]
public sealed partial class ParcelContext : JsonSerializerContext;

/// <summary>A message whose settlement is counted, and optionally observed as it happens.</summary>
public sealed class TestMessage<T> : IAcknowledgableMessage<T>
{
    private readonly Counter _counter;
    private readonly MessageSettlement _settlement;

    private TestMessage(T body, MessageSettlement settlement, Counter counter)
    {
        Body = body;
        _settlement = settlement;
        _counter = counter;
    }

    public int Acknowledged => _counter.Acknowledged;

    public T Body { get; }

    object? IAcknowledgableMessage.Body => Body;

    public string MessageId => "test";

    public bool IsSettled => _settlement.IsSettled;

    public IReadOnlyDictionary<string, object> Metadata { get; } = new Dictionary<string, object>();

    internal static TestMessage<T> Create(T body, Action? onAcknowledge = null)
    {
        var counter = new Counter();

        var settlement = new MessageSettlement(_ =>
        {
            onAcknowledge?.Invoke();
            counter.Acknowledged++;
            return Task.CompletedTask;
        }, (_, _) => Task.CompletedTask);

        return new TestMessage<T>(body, settlement, counter);
    }

    public Task AcknowledgeAsync(CancellationToken cancellationToken = default) => _settlement.AcknowledgeAsync(cancellationToken);

    public Task RejectAsync(bool requeue, CancellationToken cancellationToken = default) => _settlement.RejectAsync(requeue, cancellationToken);

    public IAcknowledgableMessage<TNew> WithBody<TNew>(TNew body) => new TestMessage<TNew>(body, _settlement, _counter);
}

/// <summary>Creates test messages.</summary>
public static class TestMessage
{
    public static TestMessage<T> Create<T>(T body, Action? onAcknowledge = null) => TestMessage<T>.Create(body, onAcknowledge);
}

public sealed class Counter
{
    public int Acknowledged { get; set; }
}

internal sealed class CapturingDeadLetterSink : IDeadLetterSink
{
    public List<DeadLetterEnvelope> Captured { get; } = [];

    public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
    {
        Captured.Add(envelope);
        return Task.CompletedTask;
    }
}

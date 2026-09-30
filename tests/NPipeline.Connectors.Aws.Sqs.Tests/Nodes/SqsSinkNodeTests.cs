using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using FakeItEasy;
using NPipeline.Configuration;
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Nodes;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using static NPipeline.Connectors.Aws.Sqs.Tests.SqsTestSupport;

namespace NPipeline.Connectors.Aws.Sqs.Tests.Nodes;

public sealed class SqsSinkNodeTests
{
    private static readonly Order[] Orders = [.. Enumerable.Range(1, 5).Select(i => new Order(i, $"order-{i}"))];

    private readonly IAmazonSQS _client = A.Fake<IAmazonSQS>();
    private readonly List<SendMessageBatchRequest> _requests = [];
    private Func<SendMessageBatchRequest, SendMessageBatchResponse> _respond = _ => new SendMessageBatchResponse();

    public SqsSinkNodeTests()
    {
        A.CallTo(() => _client.SendMessageBatchAsync(A<SendMessageBatchRequest>._, A<CancellationToken>._))
            .ReturnsLazily((SendMessageBatchRequest request, CancellationToken _) =>
            {
                lock (_requests)
                {
                    _requests.Add(request);
                }

                return Task.FromResult(_respond(request));
            });
    }

    private IEnumerable<SendMessageBatchRequestEntry> Entries => _requests.SelectMany(request => request.Entries);

    [Fact]
    public void Constructor_WithNullOptions_Throws()
    {
        var act = () => new SqsSinkNode<Order>(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_ValidatesTheOptions()
    {
        var act = () => new SqsSinkNode<Order>(new SqsWriteOptions<Order> { QueueUrl = QueueUrl, Client = _client, BatchSize = 11 });

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(SqsWriteOptions<Order>.BatchSize));
    }

    [Fact]
    public async Task Consume_SendsEachBodyAsJson()
    {
        await RunAsync(Sink(), Orders[..2]);

        _requests.Should().ContainSingle().Which.QueueUrl.Should().Be(QueueUrl);
        Entries.Select(entry => entry.Id).Should().Equal("0", "1");
        Entries.Select(entry => entry.MessageBody).Should().Equal("""{"id":1,"name":"order-1"}""", """{"id":2,"name":"order-2"}""");
        Entries.Should().AllSatisfy(entry =>
        {
            entry.DelaySeconds.Should().BeNull("a zero delay is left unset, since FIFO queues reject one");
            entry.MessageAttributes.Should().BeNullOrEmpty();
            entry.MessageGroupId.Should().BeNull();
            entry.MessageDeduplicationId.Should().BeNull();
        });
    }

    [Fact]
    public async Task Consume_WithACustomSerializer_UsesIt()
    {
        await RunAsync(Sink(o => o with { Serializer = new TextSerializer() }), Orders[..1]);

        Entries.Single().MessageBody.Should().Be(Orders[0].ToString());
    }

    [Fact]
    public async Task Consume_SendsBatchesOfBatchSize()
    {
        await RunAsync(Sink(o => o with { BatchSize = 2 }), Orders);

        _requests.Select(request => request.Entries.Count).Should().Equal(2, 2, 1);
        _requests.Should().AllSatisfy(request => request.Entries.Select(entry => entry.Id).Should().Equal(
            Enumerable.Range(0, request.Entries.Count).Select(i => $"{i}")));
    }

    [Fact]
    public async Task Consume_SplitsABatchOverTheSizeLimit()
    {
        var bodies = Enumerable.Range(0, 3).Select(i => new string((char)('a' + i), 100_000)).ToArray();

        var sink = new SqsSinkNode<string>(new SqsWriteOptions<string> { QueueUrl = QueueUrl, Client = _client, BatchLinger = TimeSpan.FromMinutes(1) });
        await using var input = new InMemoryDataStream<string>(bodies);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);

        _requests.Select(request => request.Entries.Count).Should().Equal(2, 1);
        Entries.Select(entry => JsonSerializer.Deserialize<string>(entry.MessageBody)).Should().Equal(bodies);
    }

    [Fact]
    public async Task Consume_WithADelay_SetsDelaySeconds()
    {
        await RunAsync(Sink(o => o with { Delay = TimeSpan.FromSeconds(30) }), Orders[..2]);

        Entries.Should().AllSatisfy(entry => entry.DelaySeconds.Should().Be(30));
    }

    [Fact]
    public async Task Consume_AddsTheConfiguredMessageAttributes()
    {
        var attributes = new Dictionary<string, MessageAttributeValue> { ["source"] = new() { DataType = "String", StringValue = "tests" } };

        await RunAsync(Sink(o => o with { MessageAttributes = attributes }), Orders[..2]);

        Entries.Should().AllSatisfy(entry => entry.MessageAttributes.Should().ContainKey("source")
            .WhoseValue.StringValue.Should().Be("tests"));
    }

    [Fact]
    public async Task ConsumeMessages_CopiesTheReceivedAttributes_AndTheConfiguredOnesWin()
    {
        var received = new Dictionary<string, MessageAttributeValue>
        {
            ["tenant"] = new() { DataType = "String", StringValue = "acme" },
            ["source"] = new() { DataType = "String", StringValue = "upstream" },
        };

        var configured = new Dictionary<string, MessageAttributeValue> { ["source"] = new() { DataType = "String", StringValue = "sink" } };

        await RunMessagesAsync(Sink(o => o with { MessageAttributes = configured }), Message(Orders[0], attributes: received));

        var attributes = Entries.Single().MessageAttributes;
        attributes["tenant"].StringValue.Should().Be("acme");
        attributes["source"].StringValue.Should().Be("sink");
    }

    [Fact]
    public async Task ConsumeMessages_WithoutCopyMessageAttributes_DropsTheReceivedAttributes()
    {
        var received = new Dictionary<string, MessageAttributeValue> { ["tenant"] = new() { DataType = "String", StringValue = "acme" } };

        await RunMessagesAsync(Sink(o => o with { CopyMessageAttributes = false }), Message(Orders[0], attributes: received));

        Entries.Single().MessageAttributes.Should().BeNullOrEmpty();
    }

    [Fact]
    public async Task ConsumeMessages_CopiesAttributesThroughWithBody()
    {
        var received = new Dictionary<string, MessageAttributeValue> { ["tenant"] = new() { DataType = "String", StringValue = "acme" } };
        var message = Message("raw", attributes: received).WithBody(Orders[0]);

        await RunMessagesAsync(Sink(), message);

        Entries.Single().MessageAttributes["tenant"].StringValue.Should().Be("acme");
    }

    [Fact]
    public async Task Fifo_SetsTheGroup_AndDeduplicatesOnTheSourceMessageId()
    {
        await RunMessagesAsync(Sink(o => o with { MessageGroupId = order => $"group-{order.Id % 2}" }),
            Message(Orders[0], "source-1"), Message(Orders[1], "source-2"));

        Entries.Select(entry => entry.MessageGroupId).Should().Equal("group-1", "group-0");
        Entries.Select(entry => entry.MessageDeduplicationId).Should().Equal("source-1", "source-2");
    }

    [Fact]
    public async Task Fifo_WithADeduplicationId_UsesIt()
    {
        await RunMessagesAsync(Sink(o => o with { MessageGroupId = _ => "g", DeduplicationId = order => $"order-{order.Id}" }),
            Message(Orders[0], "source-1"));

        Entries.Single().MessageDeduplicationId.Should().Be("order-1");
    }

    [Fact]
    public async Task Fifo_WithoutASourceMessage_LeavesDeduplicationToTheQueue()
    {
        await RunAsync(Sink(o => o with { MessageGroupId = _ => "g" }), Orders[..1]);

        Entries.Single().MessageGroupId.Should().Be("g");
        Entries.Single().MessageDeduplicationId.Should().BeNull();
    }

    [Fact]
    public async Task WithoutAGroup_TheSourceMessageIdIsNotUsed()
    {
        await RunMessagesAsync(Sink(), Message(Orders[0], "source-1"));

        Entries.Single().MessageDeduplicationId.Should().BeNull();
    }

    [Fact]
    public async Task ConsumeMessages_AcknowledgesEachMessageAfterItIsSent()
    {
        var messages = FakeMessages(3);

        await RunMessagesAsync(Sink(), messages);

        foreach (var message in messages)
        {
            A.CallTo(() => _client.SendMessageBatchAsync(A<SendMessageBatchRequest>._, A<CancellationToken>._)).MustHaveHappenedOnceExactly()
                .Then(A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly());

            A.CallTo(() => message.RejectAsync(A<bool>._, A<CancellationToken>._)).MustNotHaveHappened();
        }
    }

    [Fact]
    public async Task FailedEntry_WithFail_ThrowsAfterSettlingTheSentMessages()
    {
        FailEntries("1");
        var messages = FakeMessages(3);

        var act = () => RunMessagesAsync(Sink(), messages);

        (await act.Should().ThrowAsync<AmazonSQSException>()).Which.ErrorCode.Should().Be("InvalidParameterValue");
        A.CallTo(() => messages[0].AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => messages[2].AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => messages[1].AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => messages[1].RejectAsync(A<bool>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task FailedEntry_WithFail_FailsAPlainWrite()
    {
        FailEntries("0");

        var act = () => RunAsync(Sink(), Orders[..2]);

        await act.Should().ThrowAsync<AmazonSQSException>();
    }

    [Fact]
    public async Task FailedEntry_WithRequeue_RejectsItWithRequeue_AndContinues()
    {
        FailEntries("1");
        var messages = FakeMessages(4);

        await RunMessagesAsync(Sink(o => o with { FailedMessages = FailedMessageAction.Requeue, BatchSize = 2 }), messages);

        // Entry "1" fails in both batches: the second message of each.
        _requests.Should().HaveCount(2);

        foreach (var failed in new[] { messages[1], messages[3] })
        {
            A.CallTo(() => failed.RejectAsync(true, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
            A.CallTo(() => failed.AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
        }

        foreach (var sent in new[] { messages[0], messages[2] })
        {
            A.CallTo(() => sent.AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        }
    }

    [Fact]
    public async Task FailedEntry_WithDeadLetter_SendsTheBodyToTheDeadLetterSink_AndAcknowledges()
    {
        FailEntries("1");
        var messages = FakeMessages(3);
        var deadLetters = new CapturingDeadLetterSink();

        await using var context = new PipelineContext(new PipelineContextConfiguration(DeadLetterSink: deadLetters));
        await using var input = new InMemoryDataStream<IAcknowledgableMessage<Order>>(messages);
        await Sink(o => o with { FailedMessages = FailedMessageAction.DeadLetter }).ConsumeMessagesAsync(input, context, CancellationToken.None);

        var envelope = deadLetters.Captured.Should().ContainSingle().Subject;
        envelope.Item.Should().Be(Orders[1]);
        envelope.Error.Should().BeOfType<AmazonSQSException>().Which.ErrorCode.Should().Be("InvalidParameterValue");

        foreach (var message in messages)
        {
            A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
            A.CallTo(() => message.RejectAsync(A<bool>._, A<CancellationToken>._)).MustNotHaveHappened();
        }
    }

    [Fact]
    public async Task FailedRequest_WithRequeue_RejectsEveryMessageOfTheBatch()
    {
        A.CallTo(() => _client.SendMessageBatchAsync(A<SendMessageBatchRequest>._, A<CancellationToken>._))
            .ThrowsAsync(new AmazonSQSException("service unavailable"));

        var messages = FakeMessages(2);

        await RunMessagesAsync(Sink(o => o with { FailedMessages = FailedMessageAction.Requeue }), messages);

        foreach (var message in messages)
        {
            A.CallTo(() => message.RejectAsync(true, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
            A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
        }
    }

    [Fact]
    public async Task FailedRequest_WithFail_Throws()
    {
        A.CallTo(() => _client.SendMessageBatchAsync(A<SendMessageBatchRequest>._, A<CancellationToken>._))
            .ThrowsAsync(new AmazonSQSException("service unavailable"));

        var act = () => RunAsync(Sink(), Orders[..1]);

        await act.Should().ThrowAsync<AmazonSQSException>().WithMessage("service unavailable");
    }

    [Fact]
    public async Task Acknowledging_RoutesMessagesThroughTheSinksOwnSettlement()
    {
        FailEntries("1");
        var messages = FakeMessages(2);

        var sink = Sink(o => o with { FailedMessages = FailedMessageAction.Requeue }).Acknowledging();
        await using var input = new InMemoryDataStream<IAcknowledgableMessage<Order>>(messages);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);

        // The generic wrapper would acknowledge everything at the end; the sink's own handling requeues the failure.
        A.CallTo(() => messages[0].AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => messages[1].RejectAsync(true, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => messages[1].AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task Dispose_LeavesACallerSuppliedClientOpen()
    {
        await Sink().DisposeAsync();

        A.CallTo(() => _client.Dispose()).MustNotHaveHappened();
    }

    private SqsSinkNode<Order> Sink(Func<SqsWriteOptions<Order>, SqsWriteOptions<Order>>? configure = null) =>
        SqsConnector.Sink<Order>(QueueUrl, o =>
        {
            // One batch per BatchSize, whatever the timing.
            var options = o with { Client = _client, BatchLinger = TimeSpan.FromMinutes(1) };
            return configure?.Invoke(options) ?? options;
        });

    private void FailEntries(params string[] ids) =>
        _respond = request => new SendMessageBatchResponse
        {
            Successful = [.. request.Entries.Where(entry => !ids.Contains(entry.Id)).Select(entry => new SendMessageBatchResultEntry { Id = entry.Id })],
            Failed =
            [
                .. request.Entries.Where(entry => ids.Contains(entry.Id))
                    .Select(entry => new BatchResultErrorEntry { Id = entry.Id, Code = "InvalidParameterValue", Message = "bad", SenderFault = true }),
            ],
        };

    private static IAcknowledgableMessage<Order>[] FakeMessages(int count) =>
    [
        .. Orders.Take(count).Select(order =>
        {
            var message = A.Fake<IAcknowledgableMessage<Order>>();
            A.CallTo(() => message.Body).Returns(order);
            A.CallTo(() => message.MessageId).Returns($"source-{order.Id}");
            return message;
        }),
    ];

    private static async Task RunAsync(SqsSinkNode<Order> sink, params Order[] items)
    {
        await using var input = new InMemoryDataStream<Order>(items);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
    }

    private static async Task RunMessagesAsync(SqsSinkNode<Order> sink, params IAcknowledgableMessage<Order>[] messages)
    {
        await using var input = new InMemoryDataStream<IAcknowledgableMessage<Order>>(messages);
        await sink.ConsumeMessagesAsync(input, new PipelineContext(), CancellationToken.None);
    }
}

using System.Text;
using Amazon.SQS;
using Amazon.SQS.Model;
using FakeItEasy;
using NPipeline.Configuration;
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Internal;
using NPipeline.Connectors.Aws.Sqs.Nodes;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Messaging;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;
using static NPipeline.Connectors.Aws.Sqs.Tests.SqsTestSupport;

namespace NPipeline.Connectors.Aws.Sqs.Tests.Nodes;

public sealed class SqsSourceNodeTests
{
    // Long enough that only a full batch or the end of the read sends deletes, so the tests see whole batches.
    private static readonly TimeSpan NoLinger = TimeSpan.FromMinutes(1);

    [Fact]
    public void Constructor_WithNullOptions_Throws()
    {
        var act = () => new SqsSourceNode<Order>(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_ValidatesTheOptions()
    {
        var act = () => new SqsSourceNode<Order>(new SqsReadOptions { QueueUrl = QueueUrl, WaitTime = TimeSpan.FromSeconds(30) });

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(SqsReadOptions.WaitTime));
    }

    [Fact]
    public async Task Receive_DeserializesEachBody()
    {
        var client = ReceivingClient([ReceivedOrder(1), ReceivedOrder(2)], [ReceivedOrder(3)]);
        await using var source = Source(client);
        await using var context = new PipelineContext();

        var messages = await ReadAsync(source.OpenStream(context, CancellationToken.None), 3);

        messages.Select(message => message.Body).Should().Equal(new Order(1, "order-1"), new Order(2, "order-2"), new Order(3, "order-3"));
        messages.Select(message => message.MessageId).Should().Equal("m1", "m2", "m3");
        messages.Select(message => message.ReceiptHandle).Should().Equal("receipt-m1", "receipt-m2", "receipt-m3");
        messages.Should().AllSatisfy(message => message.QueueUrl.Should().Be(QueueUrl));
    }

    [Fact]
    public async Task Receive_WithACustomSerializer_UsesIt()
    {
        var client = ReceivingClient([Received("m1", "raw text")]);
        await using var source = new SqsSourceNode<string>(new SqsReadOptions { QueueUrl = QueueUrl, Client = client, Serializer = new TextSerializer() });
        await using var context = new PipelineContext();

        var messages = await ReadAsync(source.OpenStream(context, CancellationToken.None), 1);

        messages.Single().Body.Should().Be("raw text");
    }

    [Fact]
    public async Task Receive_SendsTheConfiguredRequest()
    {
        var client = ReceivingClient([ReceivedOrder(1)]);
        await using var source = Source(client, o => o with { MaxMessages = 5, WaitTime = TimeSpan.FromSeconds(3), VisibilityTimeout = TimeSpan.FromSeconds(45) });
        await using var context = new PipelineContext();

        _ = await ReadAsync(source.OpenStream(context, CancellationToken.None), 1);

        A.CallTo(() => client.ReceiveMessageAsync(A<ReceiveMessageRequest>.That.Matches(request =>
                    request.QueueUrl == QueueUrl
                    && request.MaxNumberOfMessages == 5
                    && request.WaitTimeSeconds == 3
                    && request.VisibilityTimeout == 45
                    && request.MessageSystemAttributeNames.Contains("All")
                    && request.MessageAttributeNames.Contains("All")),
                A<CancellationToken>._))
            .MustHaveHappened();
    }

    [Fact]
    public async Task Receive_WithoutAVisibilityTimeout_LeavesItToTheQueue()
    {
        var client = ReceivingClient([ReceivedOrder(1)]);
        await using var source = Source(client);
        await using var context = new PipelineContext();

        _ = await ReadAsync(source.OpenStream(context, CancellationToken.None), 1);

        A.CallTo(() => client.ReceiveMessageAsync(
                A<ReceiveMessageRequest>.That.Matches(request => request.VisibilityTimeout == null && request.MaxNumberOfMessages == 10 && request.WaitTimeSeconds == 20),
                A<CancellationToken>._))
            .MustHaveHappened();
    }

    [Fact]
    public async Task Receive_WhenNothingArrives_KeepsPolling()
    {
        // The AWS SDK v4 returns null Messages for an empty receive.
        var client = ReceivingClient(null, [], [ReceivedOrder(1)]);
        await using var source = Source(client);
        await using var context = new PipelineContext();

        var messages = await ReadAsync(source.OpenStream(context, CancellationToken.None), 1);

        messages.Single().Body.Id.Should().Be(1);
        A.CallTo(() => client.ReceiveMessageAsync(A<ReceiveMessageRequest>._, A<CancellationToken>._)).MustHaveHappened(3, Times.Exactly);
    }

    [Fact]
    public async Task Receive_CarriesTheMessageAndSystemAttributes()
    {
        var received = Received("m1", """{"id":1,"name":"one"}""",
            new Dictionary<string, MessageAttributeValue> { ["tenant"] = new() { DataType = "String", StringValue = "acme" } },
            new Dictionary<string, string> { ["ApproximateReceiveCount"] = "4", ["SentTimestamp"] = "1700000000000" });

        var client = ReceivingClient([received]);
        await using var source = Source(client);
        await using var context = new PipelineContext();

        var message = (await ReadAsync(source.OpenStream(context, CancellationToken.None), 1)).Single();

        message.Attributes["tenant"].StringValue.Should().Be("acme");
        message.SystemAttributes.Should().ContainKey("ApproximateReceiveCount");
        message.ReceiveCount.Should().Be(4);
        message.SentAt.Should().Be(DateTimeOffset.FromUnixTimeMilliseconds(1700000000000));
        message.Metadata.Should().Contain("Attribute.tenant", "acme").And.Contain("QueueUrl", QueueUrl);
    }

    [Fact]
    public async Task Receive_WithoutAttributes_HasEmptyAttributes()
    {
        var client = ReceivingClient([ReceivedOrder(1)]);
        await using var source = Source(client);
        await using var context = new PipelineContext();

        var message = (await ReadAsync(source.OpenStream(context, CancellationToken.None), 1)).Single();

        message.Attributes.Should().BeEmpty();
        message.SystemAttributes.Should().BeEmpty();
        message.ReceiveCount.Should().Be(1);
    }

    [Fact]
    public async Task Acknowledge_DeletesInBatchesOfUpToTen()
    {
        var client = ReceivingClient([.. Enumerable.Range(1, 10).Select(ReceivedOrder)], [ReceivedOrder(11), ReceivedOrder(12)]);
        var source = Source(client, o => o with { DeleteLinger = NoLinger });
        await using var context = new PipelineContext();

        var messages = await ReadAsync(source.OpenStream(context, CancellationToken.None), 12);

        foreach (var message in messages)
        {
            await message.AcknowledgeAsync();
        }

        await source.DisposeAsync();

        DeleteBatches(client).Select(batch => batch.Count).Should().Equal(10, 2);
        DeletedHandles(client).Should().Equal(messages.Select(message => message.ReceiptHandle));
        messages.Should().AllSatisfy(message => message.IsSettled.Should().BeTrue());
    }

    [Fact]
    public async Task Acknowledge_AfterTheReadEnds_SendsTheDeletesOnceEveryMessageIsSettled()
    {
        var client = ReceivingClient([ReceivedOrder(1), ReceivedOrder(2)]);
        await using var source = Source(client, o => o with { DeleteLinger = NoLinger });
        await using var context = new PipelineContext();

        var messages = await ReadAsync(source.OpenStream(context, CancellationToken.None), 2);
        await messages[0].AcknowledgeAsync();
        await Task.Delay(100);

        // One message is still in flight, so the source keeps its deletes open.
        DeletedHandles(client).Should().BeEmpty();

        await messages[1].AcknowledgeAsync();

        await EventuallyAsync(() => DeletedHandles(client).Count == 2);
        DeleteBatches(client).Should().ContainSingle().Which.Should().Equal("receipt-m1", "receipt-m2");
    }

    [Fact]
    public async Task Dispose_SendsTheAcknowledgedDeletesWithoutWaitingForUnsettledMessages()
    {
        var client = ReceivingClient([ReceivedOrder(1), ReceivedOrder(2)]);
        var source = Source(client, o => o with { DeleteLinger = NoLinger, SettleTimeout = TimeSpan.FromMinutes(5) });
        await using var context = new PipelineContext();

        var messages = await ReadAsync(source.OpenStream(context, CancellationToken.None), 2);
        await messages[0].AcknowledgeAsync();

        var dispose = source.DisposeAsync().AsTask();
        (await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromSeconds(5)))).Should().BeSameAs(dispose);

        DeletedHandles(client).Should().Equal("receipt-m1");
    }

    [Fact]
    public async Task Acknowledge_Twice_DeletesOnce()
    {
        var client = ReceivingClient([ReceivedOrder(1)]);
        var source = Source(client, o => o with { DeleteLinger = NoLinger });
        await using var context = new PipelineContext();

        var message = (await ReadAsync(source.OpenStream(context, CancellationToken.None), 1)).Single();
        await message.AcknowledgeAsync();
        await message.AcknowledgeAsync();
        await message.WithBody("mapped").AcknowledgeAsync();
        await source.DisposeAsync();

        DeletedHandles(client).Should().Equal("receipt-m1");
    }

    [Fact]
    public async Task RejectWithRequeue_MakesTheMessageVisibleAgain()
    {
        var client = ReceivingClient([ReceivedOrder(1)]);
        var source = Source(client);
        await using var context = new PipelineContext();

        var message = (await ReadAsync(source.OpenStream(context, CancellationToken.None), 1)).Single();
        await message.RejectAsync(true);
        await source.DisposeAsync();

        A.CallTo(() => client.ChangeMessageVisibilityAsync(QueueUrl, "receipt-m1", 0, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        DeletedHandles(client).Should().BeEmpty();
        message.IsSettled.Should().BeTrue();
    }

    [Fact]
    public async Task RejectWithoutRequeue_DeletesTheMessage()
    {
        var client = ReceivingClient([ReceivedOrder(1)]);
        var source = Source(client);
        await using var context = new PipelineContext();

        var message = (await ReadAsync(source.OpenStream(context, CancellationToken.None), 1)).Single();
        await message.RejectAsync(false);
        await source.DisposeAsync();

        DeletedHandles(client).Should().Equal("receipt-m1");
        A.CallTo(() => client.ChangeMessageVisibilityAsync(A<string>._, A<string>._, A<int?>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task UndeserializableBody_ByDefault_FailsTheReadAndLeavesTheMessage()
    {
        var client = ReceivingClient([Received("bad", "not json")]);
        var source = Source(client);
        await using var context = new PipelineContext();

        var act = () => ReadAsync(source.OpenStream(context, CancellationToken.None), 1);

        var thrown = await act.Should().ThrowAsync<RecordMappingException>();
        thrown.Which.RecordSource.Should().Be(QueueUrl);
        thrown.Which.RecordNumber.Should().Be(1);

        await source.DisposeAsync();
        DeletedHandles(client).Should().BeEmpty();
    }

    [Fact]
    public async Task UndeserializableBody_WithFail_FailsTheRead()
    {
        var client = ReceivingClient([ReceivedOrder(1), Received("bad", "not json")]);
        var source = Source(client, o => o with { RowErrorHandler = _ => RowErrorAction.Fail });
        await using var context = new PipelineContext();

        var act = () => ReadAsync(source.OpenStream(context, CancellationToken.None), 2);

        (await act.Should().ThrowAsync<RecordMappingException>()).Which.RecordNumber.Should().Be(2);

        await source.DisposeAsync();
        DeletedHandles(client).Should().NotContain("receipt-bad");
    }

    [Fact]
    public async Task UndeserializableBody_WithSkip_DeletesItAndContinues()
    {
        RowError? seen = null;
        var client = ReceivingClient([Received("bad", "not json"), ReceivedOrder(2)]);

        var source = Source(client, o => o with
        {
            RowErrorHandler = error =>
            {
                seen = error;
                return RowErrorAction.Skip;
            },
        });

        await using var context = new PipelineContext();

        var messages = await ReadAsync(source.OpenStream(context, CancellationToken.None), 1);
        await source.DisposeAsync();

        messages.Single().Body.Id.Should().Be(2);
        seen!.Source.Should().Be(QueueUrl);
        seen.RecordNumber.Should().Be(1);
        seen.RawExcerpt.Should().Be("not json");
        DeletedHandles(client).Should().Equal("receipt-bad");
    }

    [Fact]
    public async Task UndeserializableBody_WithDeadLetter_SendsAMessageFailureAndDeletesIt()
    {
        var client = ReceivingClient([Received("bad", "not json", new Dictionary<string, MessageAttributeValue>
        {
            ["tenant"] = new() { DataType = "String", StringValue = "acme" },
        }), ReceivedOrder(2)]);

        var deadLetters = new CapturingDeadLetterSink();
        var source = Source(client, o => o with { RowErrorHandler = _ => RowErrorAction.DeadLetter });
        await using var context = new PipelineContext(new PipelineContextConfiguration(DeadLetterSink: deadLetters));

        var messages = await ReadAsync(source.OpenStream(context, CancellationToken.None), 1);
        await source.DisposeAsync();

        messages.Single().Body.Id.Should().Be(2);

        var envelope = deadLetters.Captured.Should().ContainSingle().Subject;
        var failure = envelope.Item.Should().BeOfType<MessageFailure>().Subject;
        failure.Source.Should().Be(QueueUrl);
        failure.MessageId.Should().Be("bad");
        Encoding.UTF8.GetString(failure.Body.Span).Should().Be("not json");
        failure.Metadata.Should().Contain("QueueUrl", QueueUrl).And.Contain("Attribute.tenant", "acme");
        envelope.Error.Should().NotBeNull();

        DeletedHandles(client).Should().Equal("receipt-bad");
    }

    [Fact]
    public async Task ReceiveFailure_FailsTheRead()
    {
        var client = A.Fake<IAmazonSQS>();

        A.CallTo(() => client.ReceiveMessageAsync(A<ReceiveMessageRequest>._, A<CancellationToken>._))
            .ThrowsAsync(new AmazonSQSException("access denied"));

        await using var source = Source(client);
        await using var context = new PipelineContext();

        var act = () => ReadAsync(source.OpenStream(context, CancellationToken.None), 1);

        await act.Should().ThrowAsync<AmazonSQSException>();
    }

    [Fact]
    public async Task ACallerSuppliedClient_IsNotDisposed()
    {
        var client = ReceivingClient([ReceivedOrder(1)]);
        var source = Source(client);
        await using var context = new PipelineContext();

        var message = (await ReadAsync(source.OpenStream(context, CancellationToken.None), 1)).Single();
        await message.AcknowledgeAsync();
        await source.DisposeAsync();

        A.CallTo(() => client.Dispose()).MustNotHaveHappened();
    }

    [Fact]
    public void ClientFactory_UsesTheCallersClient_OrCreatesOneItOwns()
    {
        var client = A.Fake<IAmazonSQS>();

        SqsClientFactory.For(new SqsReadOptions { QueueUrl = QueueUrl, Client = client }).Should().Be((client, false));

        var (created, owned) = SqsClientFactory.For(new SqsReadOptions
        {
            QueueUrl = QueueUrl, Region = "ap-southeast-2", Credentials = new Amazon.Runtime.AnonymousAWSCredentials(),
        });

        using (created)
        {
            owned.Should().BeTrue();
            created.Should().BeOfType<AmazonSQSClient>();
        }
    }

    private static SqsSourceNode<Order> Source(IAmazonSQS client, Func<SqsReadOptions, SqsReadOptions>? configure = null) =>
        SqsConnector.Source<Order>(QueueUrl, o => configure?.Invoke(o with { Client = client }) ?? o with { Client = client });
}

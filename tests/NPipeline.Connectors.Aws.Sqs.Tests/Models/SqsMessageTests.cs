using Amazon.SQS.Model;
using NPipeline.Connectors.Messaging;
using static NPipeline.Connectors.Aws.Sqs.Tests.SqsTestSupport;

namespace NPipeline.Connectors.Aws.Sqs.Tests.Models;

public sealed class SqsMessageTests
{
    [Fact]
    public void Properties_ReturnWhatWasReceived()
    {
        var attributes = new Dictionary<string, MessageAttributeValue> { ["tenant"] = new() { DataType = "String", StringValue = "acme" } };
        var system = new Dictionary<string, string> { ["MessageGroupId"] = "g1" };
        var order = new Order(1, "one");

        var message = Message(order, "id-1", attributes, system);

        message.Body.Should().BeSameAs(order);
        ((IAcknowledgableMessage)message).Body.Should().BeSameAs(order);
        message.MessageId.Should().Be("id-1");
        message.ReceiptHandle.Should().Be("receipt-id-1");
        message.QueueUrl.Should().Be(QueueUrl);
        message.Attributes.Should().BeSameAs(attributes);
        message.SystemAttributes.Should().BeSameAs(system);
        message.IsSettled.Should().BeFalse();
    }

    [Fact]
    public void SentAtAndReceiveCount_ComeFromTheSystemAttributes()
    {
        var sent = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);
        var system = new Dictionary<string, string>
        {
            ["SentTimestamp"] = sent.ToUnixTimeMilliseconds().ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["ApproximateReceiveCount"] = "3",
        };

        var message = Message("body", systemAttributes: system);

        message.SentAt.Should().Be(sent);
        message.ReceiveCount.Should().Be(3);
    }

    [Fact]
    public void SentAtAndReceiveCount_WithoutSystemAttributes_HaveDefaults()
    {
        var message = Message("body", systemAttributes: new Dictionary<string, string> { ["SentTimestamp"] = "not-a-number" });

        message.SentAt.Should().BeNull();
        message.ReceiveCount.Should().Be(1);
    }

    [Fact]
    public void Metadata_HasTheQueueTheSystemAttributesAndPrefixedMessageAttributes()
    {
        var binary = new MemoryStream([1, 2, 3]);

        var attributes = new Dictionary<string, MessageAttributeValue>
        {
            ["tenant"] = new() { DataType = "String", StringValue = "acme" },
            ["priority"] = new() { DataType = "Number", StringValue = "5" },
            ["blob"] = new() { DataType = "Binary", BinaryValue = binary },
            ["empty"] = new() { DataType = "String" },
        };

        var message = Message("body", attributes: attributes, systemAttributes: new Dictionary<string, string> { ["ApproximateReceiveCount"] = "2" });

        message.Metadata.Should().BeEquivalentTo(new Dictionary<string, object>
        {
            ["QueueUrl"] = QueueUrl,
            ["ApproximateReceiveCount"] = "2",
            ["Attribute.tenant"] = "acme",
            ["Attribute.priority"] = "5",
            ["Attribute.blob"] = binary,
            ["Attribute.empty"] = string.Empty,
        });
    }

    [Fact]
    public void Metadata_WithNoAttributes_HasOnlyTheQueue()
    {
        Message("body").Metadata.Should().BeEquivalentTo(new Dictionary<string, object> { ["QueueUrl"] = QueueUrl });
    }

    [Fact]
    public async Task AcknowledgeAsync_SettlesOnce()
    {
        var acknowledgements = 0;
        var message = Message("body", acknowledge: _ =>
        {
            Interlocked.Increment(ref acknowledgements);
            return Task.CompletedTask;
        });

        await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => Task.Run(() => message.AcknowledgeAsync())));
        await message.RejectAsync(true);

        acknowledgements.Should().Be(1);
        message.IsSettled.Should().BeTrue();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RejectAsync_PassesRequeueAndSettles(bool requeue)
    {
        bool? rejected = null;
        var acknowledged = false;

        var message = Message("body", acknowledge: _ =>
        {
            acknowledged = true;
            return Task.CompletedTask;
        }, reject: (value, _) =>
        {
            rejected = value;
            return Task.CompletedTask;
        });

        await message.RejectAsync(requeue);
        await message.AcknowledgeAsync();

        rejected.Should().Be(requeue);
        acknowledged.Should().BeFalse();
        message.IsSettled.Should().BeTrue();
    }

    [Fact]
    public async Task WithBody_KeepsTheMessageAndSharesItsSettlement()
    {
        var acknowledgements = 0;
        var attributes = new Dictionary<string, MessageAttributeValue> { ["tenant"] = new() { DataType = "String", StringValue = "acme" } };

        var original = Message(new Order(7, "seven"), "id-7", attributes, acknowledge: _ =>
        {
            acknowledgements++;
            return Task.CompletedTask;
        });

        var mapped = original.WithBody("seven");

        mapped.Body.Should().Be("seven");
        mapped.MessageId.Should().Be("id-7");
        mapped.Metadata.Should().BeSameAs(original.Metadata);
        mapped.Should().BeOfType<Aws.Sqs.Models.SqsMessage<string>>()
            .Which.Should().BeEquivalentTo(new { ReceiptHandle = "receipt-id-7", QueueUrl, Attributes = attributes });

        await mapped.AcknowledgeAsync();

        original.IsSettled.Should().BeTrue();
        mapped.IsSettled.Should().BeTrue();

        await original.AcknowledgeAsync();
        await original.RejectAsync(false);
        acknowledgements.Should().Be(1);
    }

    [Fact]
    public async Task AcknowledgeAsync_WhenTheSettlementFails_ReturnsTheSameFailedTaskEachTime()
    {
        var message = Message("body", acknowledge: _ => Task.FromException(new InvalidOperationException("boom")));

        var first = message.AcknowledgeAsync();
        var second = message.AcknowledgeAsync();

        var act = () => first;
        await act.Should().ThrowAsync<InvalidOperationException>();
        second.Should().BeSameAs(first);
    }
}

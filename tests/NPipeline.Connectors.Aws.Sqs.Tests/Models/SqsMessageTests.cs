using System.Reflection;
using Amazon.SQS;
using Amazon.SQS.Model;
using FakeItEasy;
using NPipeline.Connectors.Aws.Sqs.Models;

namespace NPipeline.Connectors.Aws.Sqs.Tests.Models;

public class SqsMessageTests
{
    // Helper methods to create SqsMessage instances using reflection
    private static SqsMessage<T> CreateSqsMessageWithDirectConstructor<T>(
        T body,
        string messageId,
        string receiptHandle,
        IDictionary<string, MessageAttributeValue> attributes,
        DateTime timestamp,
        IAmazonSQS sqsClient,
        string queueUrl)
    {
        var messageType = typeof(SqsMessage<>).MakeGenericType(typeof(T));

        var constructor = messageType.GetConstructors(
                BindingFlags.NonPublic | BindingFlags.Instance)
            .First(c => c.GetParameters().Length == 7);

        return (SqsMessage<T>)constructor.Invoke(
            [body, messageId, receiptHandle, attributes, timestamp, sqsClient!, queueUrl!]);
    }

    private static SqsMessage<T> CreateSqsMessageWithBatchCallbackConstructor<T>(
        T body,
        string messageId,
        string receiptHandle,
        IDictionary<string, MessageAttributeValue> attributes,
        DateTime timestamp,
        Func<CancellationToken, Task> acknowledgeCallback)
    {
        var messageType = typeof(SqsMessage<>).MakeGenericType(typeof(T));

        var constructor = messageType.GetConstructors(
                BindingFlags.NonPublic | BindingFlags.Instance)
            .First(c => c.GetParameters().Length == 6);

        return (SqsMessage<T>)constructor.Invoke(
            [body, messageId, receiptHandle, attributes, timestamp, acknowledgeCallback]);
    }

    public class DirectConstructor
    {
        [Fact]
        public void Constructor_WithDirectAcknowledgment_InitializesAllProperties()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            var sqsClientFake = A.Fake<IAmazonSQS>();
            var queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";

            // Act
            var message = CreateSqsMessageWithDirectConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                sqsClientFake,
                queueUrl);

            // Assert
            message.Body.Should().Be(body);
            message.MessageId.Should().Be(messageId);
            message.ReceiptHandle.Should().Be(receiptHandle);
            message.Attributes.Should().BeEquivalentTo(attributes);
            message.Timestamp.Should().Be(timestamp);
            message.IsAcknowledged.Should().BeFalse();
            message.Metadata.Should().ContainKey("Timestamp");
            message.Metadata.Should().ContainKey("ReceiptHandle");
            message.Metadata["ReceiptHandle"].Should().Be(receiptHandle);
        }

        [Fact]
        public void Constructor_WithNullSqsClient_DoesNotThrow()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;

            // Act
            var message = CreateSqsMessageWithDirectConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                null!,
                null!);

            // Assert
            message.Body.Should().Be(body);
            message.MessageId.Should().Be(messageId);
        }

        [Fact]
        public void Constructor_WithEmptyAttributes_InitializesMetadata()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            var sqsClientFake = A.Fake<IAmazonSQS>();
            var queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";

            // Act
            var message = CreateSqsMessageWithDirectConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                sqsClientFake,
                queueUrl);

            // Assert
            message.Metadata.Should().HaveCount(2); // Timestamp and ReceiptHandle
        }
    }

    public class BatchCallbackConstructor
    {
        [Fact]
        public void Constructor_WithBatchCallback_InitializesAllProperties()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            // Act
            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Assert
            message.Body.Should().Be(body);
            message.MessageId.Should().Be(messageId);
            message.ReceiptHandle.Should().Be(receiptHandle);
            message.Attributes.Should().BeEquivalentTo(attributes);
            message.Timestamp.Should().Be(timestamp);
            message.IsAcknowledged.Should().BeFalse();
            message.Metadata.Should().ContainKey("Timestamp");
            message.Metadata.Should().ContainKey("ReceiptHandle");
            message.Metadata["ReceiptHandle"].Should().Be(receiptHandle);
        }

        [Fact]
        public void Constructor_WithBatchCallback_DoesNotInvokeCallbackImmediately()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            var callbackInvoked = false;

            Func<CancellationToken, Task> callback = _ =>
            {
                callbackInvoked = true;
                return Task.CompletedTask;
            };

            // Act
            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Assert
            callbackInvoked.Should().BeFalse();
        }
    }

    public class Acknowledgment_Idempotency
    {
        [Fact]
        public async Task AcknowledgeAsync_WithDirectConstructor_CallsDeleteMessageOnce()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            var sqsClientFake = A.Fake<IAmazonSQS>();
            var queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";

            A.CallTo(() => sqsClientFake.DeleteMessageAsync(queueUrl, receiptHandle, A<CancellationToken>._))
                .Returns(new DeleteMessageResponse());

            var message = CreateSqsMessageWithDirectConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                sqsClientFake,
                queueUrl);

            // Act
            await message.AcknowledgeAsync();
            await message.AcknowledgeAsync();
            await message.AcknowledgeAsync();

            // Assert
            message.IsAcknowledged.Should().BeTrue();

            A.CallTo(() => sqsClientFake.DeleteMessageAsync(queueUrl, receiptHandle, A<CancellationToken>._))
                .MustHaveHappenedOnceExactly();
        }

        [Fact]
        public async Task AcknowledgeAsync_WithBatchCallback_InvokesCallbackOnce()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            var callbackCount = 0;

            Func<CancellationToken, Task> callback = _ =>
            {
                Interlocked.Increment(ref callbackCount);
                return Task.CompletedTask;
            };

            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Act
            await message.AcknowledgeAsync();
            await message.AcknowledgeAsync();
            await message.AcknowledgeAsync();

            // Assert
            message.IsAcknowledged.Should().BeTrue();
            callbackCount.Should().Be(1);
        }

        [Fact]
        public async Task AcknowledgeAsync_WithConcurrentCalls_IsThreadSafe()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            var callbackCount = 0;

            Func<CancellationToken, Task> callback = _ =>
            {
                Interlocked.Increment(ref callbackCount);
                return Task.CompletedTask;
            };

            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Act
            var tasks = Enumerable.Range(0, 100).Select(_ => message.AcknowledgeAsync()).ToArray();
            await Task.WhenAll(tasks);

            // Assert
            message.IsAcknowledged.Should().BeTrue();
            callbackCount.Should().Be(1);
        }

        [Fact]
        public async Task AcknowledgeAsync_WithCancellation_CanBeCancelled()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            var cts = new CancellationTokenSource();

            Func<CancellationToken, Task> callback = ct =>
            {
                cts.Cancel();
                return Task.CompletedTask;
            };

            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Act
            await message.AcknowledgeAsync(cts.Token);

            // Assert
            message.IsAcknowledged.Should().BeTrue();
        }

        [Fact]
        public async Task AcknowledgeAsync_AfterAcknowledgment_DoesNotThrow()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            await message.AcknowledgeAsync();

            // Act & Assert - Should not throw when acknowledging again
            await message.AcknowledgeAsync();
        }
    }

    public class Metadata_Building
    {
        [Fact]
        public void Metadata_WithNoAttributes_ContainsTimestampAndReceiptHandle()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            // Act
            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Assert
            message.Metadata.Should().ContainKey("Timestamp");
            message.Metadata.Should().ContainKey("ReceiptHandle");
            message.Metadata["Timestamp"].Should().Be(timestamp);
            message.Metadata["ReceiptHandle"].Should().Be(receiptHandle);
        }

        [Fact]
        public void Metadata_WithStringAttribute_IncludesAttributeWithPrefix()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";

            var attributes = new Dictionary<string, MessageAttributeValue>
            {
                ["CustomAttribute"] = new()
                {
                    DataType = "String",
                    StringValue = "custom-value",
                },
            };

            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            // Act
            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Assert
            message.Metadata.Should().ContainKey("Attribute.CustomAttribute");
            message.Metadata["Attribute.CustomAttribute"].Should().Be("custom-value");
        }

        [Fact]
        public void Metadata_WithNumberAttribute_IncludesAttributeWithPrefix()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";

            var attributes = new Dictionary<string, MessageAttributeValue>
            {
                ["Count"] = new()
                {
                    DataType = "Number",
                    StringValue = "42",
                },
            };

            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            // Act
            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Assert
            message.Metadata.Should().ContainKey("Attribute.Count");
            message.Metadata["Attribute.Count"].Should().Be("42");
        }

        [Fact]
        public void Metadata_WithBinaryAttribute_IncludesAttributeWithPrefix()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var binaryData = new byte[] { 1, 2, 3, 4, 5 };

            var attributes = new Dictionary<string, MessageAttributeValue>
            {
                ["BinaryData"] = new()
                {
                    DataType = "Binary",
                    BinaryValue = new MemoryStream(binaryData),
                },
            };

            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            // Act
            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Assert
            message.Metadata.Should().ContainKey("Attribute.BinaryData");
            var metadataValue = message.Metadata["Attribute.BinaryData"] as MemoryStream;
            metadataValue.Should().NotBeNull();
            metadataValue!.ToArray().Should().BeEquivalentTo(binaryData);
        }

        [Fact]
        public void Metadata_WithUnknownDataType_IncludesStringValue()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";

            var attributes = new Dictionary<string, MessageAttributeValue>
            {
                ["UnknownType"] = new()
                {
                    DataType = "CustomType",
                    StringValue = "custom-value",
                },
            };

            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            // Act
            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Assert
            message.Metadata.Should().ContainKey("Attribute.UnknownType");
            message.Metadata["Attribute.UnknownType"].Should().Be("custom-value");
        }

        [Fact]
        public void Metadata_WithMultipleAttributes_IncludesAllAttributes()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";

            var attributes = new Dictionary<string, MessageAttributeValue>
            {
                ["StringAttr"] = new()
                {
                    DataType = "String",
                    StringValue = "string-value",
                },
                ["NumberAttr"] = new()
                {
                    DataType = "Number",
                    StringValue = "123",
                },
                ["AnotherString"] = new()
                {
                    DataType = "String",
                    StringValue = "another-value",
                },
            };

            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            // Act
            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Assert
            message.Metadata.Should().ContainKey("Attribute.StringAttr");
            message.Metadata.Should().ContainKey("Attribute.NumberAttr");
            message.Metadata.Should().ContainKey("Attribute.AnotherString");
            message.Metadata.Should().HaveCount(5); // 2 built-in + 3 attributes
        }

        [Fact]
        public void Metadata_IsReadOnly()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Act & Assert
            message.Metadata.Should().BeAssignableTo<IReadOnlyDictionary<string, object>>();
        }
    }

    public class Properties
    {
        [Fact]
        public void Body_ReturnsOriginalBody()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Act & Assert
            message.Body.Should().Be(body);
            message.Body.Id.Should().Be(1);
            message.Body.Name.Should().Be("Test");
        }

        [Fact]
        public void MessageId_ReturnsProvidedMessageId()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Act & Assert
            message.MessageId.Should().Be(messageId);
        }

        [Fact]
        public void ReceiptHandle_ReturnsProvidedReceiptHandle()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Act & Assert
            message.ReceiptHandle.Should().Be(receiptHandle);
        }

        [Fact]
        public void Timestamp_ReturnsProvidedTimestamp()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Act & Assert
            message.Timestamp.Should().Be(timestamp);
        }

        [Fact]
        public void Attributes_ReturnsOriginalAttributes()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";

            var attributes = new Dictionary<string, MessageAttributeValue>
            {
                ["Attr1"] = new() { DataType = "String", StringValue = "value1" },
                ["Attr2"] = new() { DataType = "String", StringValue = "value2" },
            };

            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Act & Assert
            message.Attributes.Should().BeEquivalentTo(attributes);
        }

        [Fact]
        public void IsAcknowledged_InitiallyFalse()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Act & Assert
            message.IsAcknowledged.Should().BeFalse();
        }

        [Fact]
        public async Task IsAcknowledged_AfterAcknowledgment_IsTrue()
        {
            // Arrange
            var body = new TestModel { Id = 1, Name = "Test" };
            var messageId = "test-message-id";
            var receiptHandle = "test-receipt-handle";
            var attributes = new Dictionary<string, MessageAttributeValue>();
            var timestamp = DateTime.UtcNow;
            Func<CancellationToken, Task> callback = _ => Task.CompletedTask;

            var message = CreateSqsMessageWithBatchCallbackConstructor(
                body,
                messageId,
                receiptHandle,
                attributes,
                timestamp,
                callback);

            // Act
            await message.AcknowledgeAsync();

            // Assert
            message.IsAcknowledged.Should().BeTrue();
        }
    }

    private class TestModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

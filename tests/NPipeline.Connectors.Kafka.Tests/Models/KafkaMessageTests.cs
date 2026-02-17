using System.Text;
using Confluent.Kafka;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Kafka.Models;

namespace NPipeline.Connectors.Kafka.Tests.Models;

/// <summary>
///     Unit tests for <see cref="KafkaMessage{T}" />.
/// </summary>
public class KafkaMessageTests
{
    #region IKafkaMessageMetadata Tests

    [Fact]
    public void KafkaMessage_ShouldImplementIKafkaMessageMetadata()
    {
        // Arrange
        var headers = new Headers();
        headers.Add("h", Encoding.UTF8.GetBytes("v"));

        var message = new KafkaMessage<TestMessage>(
            new TestMessage { Id = 1, Name = "Test" },
            "test-topic",
            1,
            100,
            "key",
            DateTime.UtcNow,
            headers,
            _ => Task.CompletedTask);

        // Act
        var metadata = (IKafkaMessageMetadata)message;

        // Assert
        metadata.Topic.Should().Be("test-topic");
        metadata.Partition.Should().Be(1);
        metadata.Offset.Should().Be(100);
        metadata.Key.Should().Be("key");
    }

    #endregion

    #region IAcknowledgableMessage Tests

    [Fact]
    public void KafkaMessage_ShouldImplementIAcknowledgableMessage()
    {
        // Arrange & Act
        var message = new KafkaMessage<TestMessage>(
            new TestMessage { Id = 1, Name = "Test" },
            "test-topic",
            0,
            0,
            "key",
            DateTime.UtcNow,
            new Headers(),
            _ => Task.CompletedTask);

        // Assert
        message.Should().BeAssignableTo<IAcknowledgableMessage<TestMessage>>();
    }

    #endregion

    #region MessageId Tests

    [Fact]
    public void MessageId_ShouldReturnTopicPartitionOffsetFormat()
    {
        // Arrange
        var message = new KafkaMessage<TestMessage>(
            new TestMessage { Id = 1, Name = "Test" },
            "my-topic",
            5,
            12345,
            "key",
            DateTime.UtcNow,
            new Headers(),
            _ => Task.CompletedTask);

        // Act & Assert
        message.MessageId.Should().Be("my-topic-5-12345");
    }

    #endregion

    #region Test Helpers

    private sealed class TestMessage
    {
        public int Id { get; init; }
        public string Name { get; init; } = string.Empty;
    }

    #endregion

    #region Constructor Tests

    [Fact]
    public void Constructor_ShouldSetProperties()
    {
        // Arrange
        var body = new TestMessage { Id = 1, Name = "Test" };
        var topic = "test-topic";
        var partition = 1;
        var offset = 100L;
        var key = "test-key";
        var timestamp = DateTime.UtcNow;
        var headers = new Headers();
        headers.Add("header1", Encoding.UTF8.GetBytes("value1"));
        Func<CancellationToken, Task> acknowledgeCallback = _ => Task.CompletedTask;

        // Act
        var message = new KafkaMessage<TestMessage>(
            body,
            topic,
            partition,
            offset,
            key,
            timestamp,
            headers,
            acknowledgeCallback);

        // Assert
        message.Body.Should().Be(body);
        message.Topic.Should().Be(topic);
        message.Partition.Should().Be(partition);
        message.Offset.Should().Be(offset);
        message.Key.Should().Be(key);
        message.Timestamp.Should().Be(timestamp);
        message.Headers.Should().NotBeEmpty();
        message.IsAcknowledged.Should().BeFalse();
    }

    [Fact]
    public void Constructor_WithNullHeaders_ShouldInitializeEmptyHeaders()
    {
        // Arrange & Act
        var message = new KafkaMessage<TestMessage>(
            new TestMessage { Id = 1, Name = "Test" },
            "test-topic",
            0,
            0,
            "key",
            DateTime.UtcNow,
            null!,
            _ => Task.CompletedTask);

        // Assert
        message.Headers.Should().NotBeNull();
        message.Headers.Should().BeEmpty();
    }

    [Fact]
    public void Constructor_WithNullAcknowledgeCallback_ShouldNotThrow()
    {
        // Arrange & Act - null acknowledge callback is valid for exactly-once semantics
        // where acknowledgment is handled via SendOffsetsToTransaction in the sink
        var message = new KafkaMessage<TestMessage>(
            new TestMessage { Id = 1, Name = "Test" },
            "test-topic",
            0,
            0,
            "key",
            DateTime.UtcNow,
            new Headers(),
            null);

        // Assert
        message.Should().NotBeNull();
        message.IsAcknowledged.Should().BeFalse();
    }

    #endregion

    #region AcknowledgeAsync Tests

    [Fact]
    public async Task AcknowledgeAsync_WhenCalled_ShouldSetIsAcknowledgedToTrue()
    {
        // Arrange
        var message = new KafkaMessage<TestMessage>(
            new TestMessage { Id = 1, Name = "Test" },
            "test-topic",
            0,
            0,
            "key",
            DateTime.UtcNow,
            new Headers(),
            _ => Task.CompletedTask);

        // Act
        await message.AcknowledgeAsync();

        // Assert
        message.IsAcknowledged.Should().BeTrue();
    }

    [Fact]
    public async Task AcknowledgeAsync_WhenCalledMultipleTimes_ShouldOnlyInvokeOnce()
    {
        // Arrange
        var invokeCount = 0;

        var message = new KafkaMessage<TestMessage>(
            new TestMessage { Id = 1, Name = "Test" },
            "test-topic",
            0,
            0,
            "key",
            DateTime.UtcNow,
            new Headers(),
            _ =>
            {
                invokeCount++;
                return Task.CompletedTask;
            });

        // Act
        await message.AcknowledgeAsync();
        await message.AcknowledgeAsync();
        await message.AcknowledgeAsync();

        // Assert
        invokeCount.Should().Be(1);
        message.IsAcknowledged.Should().BeTrue();
    }

    [Fact]
    public async Task AcknowledgeAsync_WhenAcknowledgeFuncThrows_ShouldNotMarkAsAcknowledged()
    {
        // Arrange
        var message = new KafkaMessage<TestMessage>(
            new TestMessage { Id = 1, Name = "Test" },
            "test-topic",
            0,
            0,
            "key",
            DateTime.UtcNow,
            new Headers(),
            _ => throw new InvalidOperationException("Commit failed"));

        // Act
        var act = () => message.AcknowledgeAsync();

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>();
        message.IsAcknowledged.Should().BeFalse();
    }

    #endregion

    #region WithBody Tests

    [Fact]
    public void WithBody_ShouldCreateNewMessageWithNewBody()
    {
        // Arrange
        var originalBody = new TestMessage { Id = 1, Name = "Original" };
        var newBody = new TestMessage { Id = 2, Name = "New" };
        var headers = new Headers();
        headers.Add("h", Encoding.UTF8.GetBytes("v"));

        var message = new KafkaMessage<TestMessage>(
            originalBody,
            "test-topic",
            1,
            100,
            "key",
            DateTime.UtcNow,
            headers,
            _ => Task.CompletedTask);

        // Act
        var newMessage = message.WithBody(newBody);

        // Assert
        newMessage.Body.Should().Be(newBody);

        // Cast to access KafkaMessage properties
        var kafkaMessage = (KafkaMessage<TestMessage>)newMessage;
        kafkaMessage.Topic.Should().Be(message.Topic);
        kafkaMessage.Partition.Should().Be(message.Partition);
        kafkaMessage.Offset.Should().Be(message.Offset);
        kafkaMessage.Key.Should().Be(message.Key);
    }

    [Fact]
    public void WithBody_WithDifferentType_ShouldCreateNewMessageWithNewType()
    {
        // Arrange
        var originalBody = new TestMessage { Id = 1, Name = "Original" };
        var newBody = "String body";

        var message = new KafkaMessage<TestMessage>(
            originalBody,
            "test-topic",
            1,
            100,
            "key",
            DateTime.UtcNow,
            new Headers(),
            _ => Task.CompletedTask);

        // Act
        var newMessage = message.WithBody(newBody);

        // Assert
        newMessage.Body.Should().Be(newBody);
    }

    #endregion

    #region Metadata Tests

    [Fact]
    public void Metadata_ShouldContainKafkaProperties()
    {
        // Arrange
        var timestamp = DateTime.UtcNow;

        var message = new KafkaMessage<TestMessage>(
            new TestMessage { Id = 1, Name = "Test" },
            "test-topic",
            1,
            100,
            "test-key",
            timestamp,
            new Headers(),
            _ => Task.CompletedTask);

        // Act & Assert
        message.Metadata.Should().ContainKey("Topic");
        message.Metadata["Topic"].Should().Be("test-topic");
        message.Metadata.Should().ContainKey("Partition");
        message.Metadata["Partition"].Should().Be(1);
        message.Metadata.Should().ContainKey("Offset");
        message.Metadata["Offset"].Should().Be(100L);
        message.Metadata.Should().ContainKey("Key");
        message.Metadata["Key"].Should().Be("test-key");
    }

    [Fact]
    public void Metadata_ShouldContainHeaders()
    {
        // Arrange
        var headers = new Headers();
        headers.Add("Content-Type", Encoding.UTF8.GetBytes("application/json"));

        var message = new KafkaMessage<TestMessage>(
            new TestMessage { Id = 1, Name = "Test" },
            "test-topic",
            0,
            0,
            "key",
            DateTime.UtcNow,
            headers,
            _ => Task.CompletedTask);

        // Act & Assert
        message.Metadata.Should().ContainKey("Header.Content-Type");
        message.Metadata["Header.Content-Type"].Should().Be("application/json");
    }

    #endregion
}

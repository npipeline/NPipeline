using System.Text;
using System.Text.Json;
using NPipeline.Connectors.Kafka.Serialization;

namespace NPipeline.Connectors.Kafka.Tests.Serialization;

/// <summary>
///     Unit tests for <see cref="JsonMessageSerializer" />.
/// </summary>
public class JsonMessageSerializerTests
{
    private readonly JsonMessageSerializer _serializer = new();

    #region Serialize Tests

    [Fact]
    public void Serialize_ShouldSerializeObjectToJsonBytes()
    {
        // Arrange
        var message = new TestMessage
        {
            Id = 123,
            Name = "Test Message",
            Timestamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc),
        };

        // Act
        var result = _serializer.Serialize(message);

        // Assert
        result.Should().NotBeNullOrEmpty();
        var json = Encoding.UTF8.GetString(result);
        json.Should().Contain("\"Id\":123");
        json.Should().Contain("\"Name\":\"Test Message\"");
    }

    [Fact]
    public void Serialize_WithNull_ShouldReturnEmptyArray()
    {
        // Act
        var result = _serializer.Serialize<object>(null!);

        // Assert - null values return empty array (tombstone message)
        result.Should().BeEmpty();
    }

    #endregion

    #region Deserialize Tests

    [Fact]
    public void Deserialize_ShouldDeserializeJsonBytesToObject()
    {
        // Arrange
        var json = "{\"Id\":456,\"Name\":\"Deserialized Message\",\"Timestamp\":\"2024-01-15T10:30:00Z\"}";
        var bytes = Encoding.UTF8.GetBytes(json);

        // Act
        var result = _serializer.Deserialize<TestMessage>(bytes);

        // Assert
        result.Should().NotBeNull();
        result!.Id.Should().Be(456);
        result.Name.Should().Be("Deserialized Message");
    }

    [Fact]
    public void Deserialize_WithEmptyArray_ShouldReturnDefault()
    {
        // Act - Empty array returns default (null for reference types)
        var result = _serializer.Deserialize<TestMessage>(Array.Empty<byte>());

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void Deserialize_WithNullData_ShouldReturnDefault()
    {
        // Act - Null data returns default (null for reference types)
        var result = _serializer.Deserialize<TestMessage>(null!);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void Deserialize_WithInvalidJson_ShouldThrow()
    {
        // Arrange
        var invalidJson = Encoding.UTF8.GetBytes("not valid json");

        // Act
        var act = () => _serializer.Deserialize<TestMessage>(invalidJson);

        // Assert
        act.Should().Throw<JsonException>();
    }

    #endregion

    #region Round-trip Tests

    [Fact]
    public void Serialize_ThenDeserialize_ShouldReturnOriginalObject()
    {
        // Arrange
        var original = new TestMessage
        {
            Id = 789,
            Name = "Round Trip Test",
            Timestamp = DateTime.UtcNow,
        };

        // Act
        var serialized = _serializer.Serialize(original);
        var deserialized = _serializer.Deserialize<TestMessage>(serialized);

        // Assert
        deserialized.Should().NotBeNull();
        deserialized!.Id.Should().Be(original.Id);
        deserialized.Name.Should().Be(original.Name);
    }

    [Fact]
    public void Serialize_ThenDeserialize_WithComplexObject_ShouldReturnOriginalObject()
    {
        // Arrange
        var original = new ComplexTestMessage
        {
            Id = Guid.NewGuid(),
            Items = new List<NestedItem>
            {
                new() { Key = "key1", Value = 100 },
                new() { Key = "key2", Value = 200 },
            },
            Metadata = new Dictionary<string, string>
            {
                { "meta1", "value1" },
                { "meta2", "value2" },
            },
        };

        // Act
        var serialized = _serializer.Serialize(original);
        var deserialized = _serializer.Deserialize<ComplexTestMessage>(serialized);

        // Assert
        deserialized.Should().NotBeNull();
        deserialized!.Id.Should().Be(original.Id);
        deserialized.Items.Should().HaveCount(2);
        deserialized.Metadata.Should().ContainKey("meta1");
    }

    #endregion

    #region Test Helpers

    private sealed class TestMessage
    {
        public int Id { get; init; }
        public string Name { get; init; } = string.Empty;
        public DateTime Timestamp { get; init; }
    }

    private sealed class ComplexTestMessage
    {
        public Guid Id { get; init; }
        public List<NestedItem> Items { get; init; } = [];
        public Dictionary<string, string> Metadata { get; init; } = new();
    }

    private sealed class NestedItem
    {
        public string Key { get; init; } = string.Empty;
        public int Value { get; init; }
    }

    #endregion
}

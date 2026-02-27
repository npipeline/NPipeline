using System.Text;
using NPipeline.Connectors.RabbitMQ.Serialization;

namespace NPipeline.Connectors.RabbitMQ.Tests.Serialization;

public sealed class RabbitMqJsonSerializerTests
{
    private readonly RabbitMqJsonSerializer _serializer = new();

    [Fact]
    public void ContentType_Is_ApplicationJson()
    {
        _serializer.ContentType.Should().Be("application/json");
    }

    [Fact]
    public void Serialize_And_Deserialize_Roundtrip()
    {
        var original = new TestPayload("Hello", 42);
        var serialized = _serializer.Serialize(original);
        var deserialized = _serializer.Deserialize<TestPayload>(serialized);

        deserialized.Name.Should().Be("Hello");
        deserialized.Value.Should().Be(42);
    }

    [Fact]
    public void Serialize_Returns_NonEmpty_Bytes()
    {
        var payload = new TestPayload("Test", 1);
        var bytes = _serializer.Serialize(payload);
        bytes.Length.Should().BeGreaterThan(0);
    }

    [Fact]
    public void Deserialize_Invalid_Json_Throws()
    {
        var badBytes = new ReadOnlyMemory<byte>([0xFF, 0xFE]);
        var act = () => _serializer.Deserialize<TestPayload>(badBytes);
        act.Should().Throw<Exception>();
    }

    [Fact]
    public void Serialize_Uses_CamelCase()
    {
        var payload = new TestPayload("Test", 1);
        var bytes = _serializer.Serialize(payload);
        var json = Encoding.UTF8.GetString(bytes.Span);
        json.Should().Contain("\"name\"");
        json.Should().Contain("\"value\"");
    }

    private sealed record TestPayload(string Name, int Value);
}

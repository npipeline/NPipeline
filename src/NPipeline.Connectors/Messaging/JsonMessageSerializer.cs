using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using NPipeline.Connectors.Serialization;

namespace NPipeline.Connectors.Messaging;

/// <summary>
///     Message bodies as JSON with System.Text.Json. By default it uses <see cref="ConnectorJson.Default" />, the JSON
///     connector's options: camelCase names, case-insensitive reads, enums as names and the <c>[Column]</c> attributes, so a
///     message written by one connector reads back in any other.
/// </summary>
/// <remarks>
///     For Native AOT and trimming, pass a source-generated <c>JsonSerializerContext</c>: bodies are then read and written
///     through its metadata, with the same web defaults.
/// </remarks>
public sealed class JsonMessageSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>A serializer with the connectors' defaults, or with <paramref name="options" /> plus the column attributes.</summary>
    public JsonMessageSerializer(JsonSerializerOptions? options = null)
    {
        _options = ConnectorJson.Resolve(options);
    }

    /// <summary>A serializer that uses <paramref name="resolver" />, typically a source-generated context, with web defaults.</summary>
    public JsonMessageSerializer(IJsonTypeInfoResolver resolver)
    {
        ArgumentNullException.ThrowIfNull(resolver);

        var options = new JsonSerializerOptions(ConnectorJson.Default) { TypeInfoResolver = resolver };
        options.MakeReadOnly();
        _options = options;
    }

    /// <summary>The serializer with the connectors' defaults.</summary>
    public static JsonMessageSerializer Default { get; } = new();

    /// <inheritdoc />
    public string ContentType => "application/json";

    /// <inheritdoc />
    public byte[] Serialize<T>(T value, MessageContext context) => JsonSerializer.SerializeToUtf8Bytes(value, ConnectorJson.TypeInfo<T>(_options));

    /// <inheritdoc />
    public T Deserialize<T>(ReadOnlySpan<byte> body, MessageContext context) =>
        JsonSerializer.Deserialize(body, ConnectorJson.TypeInfo<T>(_options))
        ?? throw new JsonException($"The body is JSON null, which is not a {typeof(T).Name}.");
}

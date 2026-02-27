using System.Text.Json;
using System.Text.Json.Serialization;
using NPipeline.Connectors.Serialization;

namespace NPipeline.Connectors.RabbitMQ.Serialization;

/// <summary>
///     RabbitMQ JSON serializer adapter implementing <see cref="IMessageSerializer" />.
/// </summary>
public sealed class RabbitMqJsonSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>
    ///     Initializes a new instance with default options (camelCase, ignore nulls).
    /// </summary>
    public RabbitMqJsonSerializer()
        : this(new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        })
    {
    }

    /// <summary>
    ///     Initializes a new instance with custom <see cref="JsonSerializerOptions" />.
    /// </summary>
    /// <param name="options">The JSON serializer options.</param>
    public RabbitMqJsonSerializer(JsonSerializerOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public string ContentType => "application/json";

    /// <inheritdoc />
    public ReadOnlyMemory<byte> Serialize<T>(T value)
    {
        return JsonSerializer.SerializeToUtf8Bytes(value, _options);
    }

    /// <inheritdoc />
    public T Deserialize<T>(ReadOnlyMemory<byte> data)
    {
        return JsonSerializer.Deserialize<T>(data.Span, _options)
               ?? throw new JsonException($"Deserialization of {typeof(T).Name} returned null.");
    }
}

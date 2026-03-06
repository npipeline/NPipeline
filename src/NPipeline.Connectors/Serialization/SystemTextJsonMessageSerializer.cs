using System.Text.Json;
using System.Text.Json.Serialization;

namespace NPipeline.Connectors.Serialization;

/// <summary>
///     Default <see cref="IMessageSerializer" /> implementation using System.Text.Json.
/// </summary>
public sealed class SystemTextJsonMessageSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>
    ///     Initializes a new instance of <see cref="SystemTextJsonMessageSerializer" /> with default options.
    /// </summary>
    public SystemTextJsonMessageSerializer()
        : this(new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        })
    {
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="SystemTextJsonMessageSerializer" /> with custom options.
    /// </summary>
    /// <param name="options">The JSON serializer options to use.</param>
    public SystemTextJsonMessageSerializer(JsonSerializerOptions options)
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

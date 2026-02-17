using System.Diagnostics;
using System.Text.Json;
using NPipeline.Connectors.Kafka.Metrics;

namespace NPipeline.Connectors.Kafka.Serialization;

/// <summary>
///     JSON serializer for Kafka messages using System.Text.Json.
/// </summary>
public sealed class JsonMessageSerializer : ISerializerProvider
{
    private readonly IKafkaMetrics _metrics;
    private readonly JsonSerializerOptions _options;

    /// <summary>
    ///     Initializes a new instance with default options.
    /// </summary>
    public JsonMessageSerializer() : this(NullKafkaMetrics.Instance)
    {
    }

    /// <summary>
    ///     Initializes a new instance with custom options.
    /// </summary>
    /// <param name="options">The JSON serializer options.</param>
    public JsonMessageSerializer(JsonSerializerOptions options) : this(options, NullKafkaMetrics.Instance)
    {
    }

    /// <summary>
    ///     Initializes a new instance with metrics support.
    /// </summary>
    /// <param name="metrics">The metrics recorder.</param>
    public JsonMessageSerializer(IKafkaMetrics metrics) : this(new JsonSerializerOptions(), metrics)
    {
    }

    /// <summary>
    ///     Initializes a new instance with custom options and metrics support.
    /// </summary>
    /// <param name="options">The JSON serializer options.</param>
    /// <param name="metrics">The metrics recorder.</param>
    public JsonMessageSerializer(JsonSerializerOptions options, IKafkaMetrics metrics)
    {
        _options = options ?? new JsonSerializerOptions();
        _metrics = metrics ?? NullKafkaMetrics.Instance;
    }

    /// <inheritdoc />
    public byte[] Serialize<T>(T value)
    {
        var sw = Stopwatch.StartNew();

        try
        {
            if (value is null)
                return [];

            return JsonSerializer.SerializeToUtf8Bytes(value, _options);
        }
        catch (Exception ex)
        {
            _metrics.RecordSerializeError(typeof(T), ex);
            throw;
        }
        finally
        {
            _metrics.RecordSerializeLatency(typeof(T), sw.Elapsed);
        }
    }

    /// <inheritdoc />
    public T Deserialize<T>(byte[] data)
    {
        var sw = Stopwatch.StartNew();

        try
        {
            if (data is null || data.Length == 0)
                return default!;

            return JsonSerializer.Deserialize<T>(data, _options)!;
        }
        catch (Exception ex)
        {
            _metrics.RecordDeserializeError(typeof(T), ex);
            throw;
        }
        finally
        {
            _metrics.RecordDeserializeLatency(typeof(T), sw.Elapsed);
        }
    }
}

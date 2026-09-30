using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Messaging;

namespace NPipeline.Connectors.Kafka.Serialization;

/// <summary>
///     Avro serializer for Kafka messages using Confluent Schema Registry.
///     Supports both ISpecificRecord (generated classes) and GenericRecord types.
/// </summary>
public sealed class AvroMessageSerializer : IMessageSerializer, IDisposable
{
    private readonly AvroDeserializerConfig? _deserializerConfig;
    private readonly ConcurrentDictionary<Type, object> _deserializers = new();
    private readonly CachedSchemaRegistryClient _schemaRegistryClient;
    private readonly AvroSerializerConfig? _serializerConfig;
    private readonly ConcurrentDictionary<Type, object> _serializers = new();
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance with Schema Registry configuration.
    /// </summary>
    /// <param name="schemaRegistryConfig">The Schema Registry configuration.</param>
    /// <param name="serializerConfig">Optional Avro serializer configuration.</param>
    /// <param name="deserializerConfig">Optional Avro deserializer configuration.</param>
    public AvroMessageSerializer(
        SchemaRegistryConfiguration schemaRegistryConfig,
        AvroSerializerConfig? serializerConfig = null,
        AvroDeserializerConfig? deserializerConfig = null)
    {
        ArgumentNullException.ThrowIfNull(schemaRegistryConfig);

        var schemaRegistryConfigDict = BuildSchemaRegistryConfig(schemaRegistryConfig);
        _schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfigDict);

        // The serializer, not the registry client, reads these settings; the client ignores them.
        _serializerConfig = serializerConfig ?? new AvroSerializerConfig
        {
            AutoRegisterSchemas = schemaRegistryConfig.AutoRegisterSchemas,
            SubjectNameStrategy = schemaRegistryConfig.SubjectNameStrategy,
        };

        _deserializerConfig = deserializerConfig;
    }

    /// <summary>
    ///     Initializes a new instance with an existing Schema Registry client.
    /// </summary>
    /// <param name="schemaRegistryClient">The Schema Registry client.</param>
    /// <param name="serializerConfig">Optional Avro serializer configuration.</param>
    /// <param name="deserializerConfig">Optional Avro deserializer configuration.</param>
    public AvroMessageSerializer(
        CachedSchemaRegistryClient schemaRegistryClient,
        AvroSerializerConfig? serializerConfig = null,
        AvroDeserializerConfig? deserializerConfig = null)
    {
        _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
        _serializerConfig = serializerConfig;
        _deserializerConfig = deserializerConfig;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Dispose cached serializers that implement IDisposable
        foreach (var serializer in _serializers.Values)
        {
            if (serializer is IDisposable disposableSerializer)
                disposableSerializer.Dispose();
        }

        foreach (var deserializer in _deserializers.Values)
        {
            if (deserializer is IDisposable disposableDeserializer)
                disposableDeserializer.Dispose();
        }

        _serializers.Clear();
        _deserializers.Clear();
        _schemaRegistryClient.Dispose();
    }

    /// <inheritdoc />
    /// <inheritdoc />
    public string ContentType => "application/vnd.apache.avro";

    /// <summary>Serializes <paramref name="value" />, registering or looking up its schema under the destination's subject.</summary>
    public byte[] Serialize<T>(T value, MessageContext context)
    {
        if (value is null)
            return [];

        // Confluent's serializers are asynchronous; the connector serializes synchronously, so this blocks.
        return GetOrCreateSerializer<T>().SerializeAsync(value, Context(context)).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public T Deserialize<T>(ReadOnlySpan<byte> body, MessageContext context)
    {
        if (body.IsEmpty)
            throw new InvalidDataException("The body is empty, which is not a serialized message.");

        return GetOrCreateDeserializer<T>().DeserializeAsync(body.ToArray(), false, Context(context)).GetAwaiter().GetResult();
    }

    private static SerializationContext Context(MessageContext context) =>
        new(context.IsKey ? MessageComponentType.Key : MessageComponentType.Value, context.Destination);

    private AvroSerializer<T> GetOrCreateSerializer<T>()
    {
        return (AvroSerializer<T>)_serializers.GetOrAdd(typeof(T), _ =>
        {
            // AvroSerializer supports both ISpecificRecord and GenericRecord
            return new AvroSerializer<T>(_schemaRegistryClient, _serializerConfig);
        });
    }

    private AvroDeserializer<T> GetOrCreateDeserializer<T>()
    {
        return (AvroDeserializer<T>)_deserializers.GetOrAdd(typeof(T), _ =>
        {
            // AvroDeserializer supports both ISpecificRecord and GenericRecord
            return new AvroDeserializer<T>(_schemaRegistryClient, _deserializerConfig);
        });
    }

    private static Dictionary<string, string> BuildSchemaRegistryConfig(SchemaRegistryConfiguration config)
    {
        var dict = new Dictionary<string, string>
        {
            { "schema.registry.url", config.Url },
        };

        if (!string.IsNullOrEmpty(config.BasicAuthUsername) && !string.IsNullOrEmpty(config.BasicAuthPassword))
        {
            dict["basic.auth.credentials.source"] = "USER_INFO";
            dict["basic.auth.user.info"] = $"{config.BasicAuthUsername}:{config.BasicAuthPassword}";
        }

        if (config.EnableSsl)
            dict["schema.registry.ssl.ca.location"] = ""; // Use system trust store

        if (config.RequestTimeoutMs > 0)
            dict["request.timeout.ms"] = config.RequestTimeoutMs.ToString();

        // Note: schema.registry.cache.capacity is not a valid CachedSchemaRegistryClient config
        // The cache capacity is managed internally by the client

        return dict;
    }
}

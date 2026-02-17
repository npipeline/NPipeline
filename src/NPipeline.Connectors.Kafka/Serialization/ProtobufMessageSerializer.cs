using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Google.Protobuf;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Metrics;

namespace NPipeline.Connectors.Kafka.Serialization;

/// <summary>
///     Protobuf serializer for Kafka messages using Confluent Schema Registry.
///     Requires message types to be generated Protobuf classes implementing IMessage.
/// </summary>
public sealed class ProtobufMessageSerializer : ISerializerProvider, IDisposable
{
    // Cached reflection methods for creating typed serializers/deserializers
    private static readonly MethodInfo CreateSerializerMethod = typeof(ProtobufMessageSerializer)
        .GetMethod(nameof(CreateProtobufSerializer), BindingFlags.NonPublic | BindingFlags.Instance)!;

    private static readonly MethodInfo CreateDeserializerMethod = typeof(ProtobufMessageSerializer)
        .GetMethod(nameof(CreateProtobufDeserializer), BindingFlags.NonPublic | BindingFlags.Instance)!;

    private readonly ProtobufDeserializerConfig? _deserializerConfig;
    private readonly ConcurrentDictionary<Type, object> _deserializers = new();
    private readonly IKafkaMetrics _metrics;
    private readonly CachedSchemaRegistryClient _schemaRegistryClient;
    private readonly ProtobufSerializerConfig? _serializerConfig;
    private readonly ConcurrentDictionary<Type, object> _serializers = new();
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance with Schema Registry configuration.
    /// </summary>
    /// <param name="schemaRegistryConfig">The Schema Registry configuration.</param>
    /// <param name="metrics">The metrics recorder.</param>
    /// <param name="serializerConfig">Optional Protobuf serializer configuration.</param>
    /// <param name="deserializerConfig">Optional Protobuf deserializer configuration.</param>
    public ProtobufMessageSerializer(
        SchemaRegistryConfiguration schemaRegistryConfig,
        IKafkaMetrics metrics,
        ProtobufSerializerConfig? serializerConfig = null,
        ProtobufDeserializerConfig? deserializerConfig = null)
    {
        ArgumentNullException.ThrowIfNull(schemaRegistryConfig);
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));

        var schemaRegistryConfigDict = BuildSchemaRegistryConfig(schemaRegistryConfig);
        _schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfigDict);
        _serializerConfig = serializerConfig;
        _deserializerConfig = deserializerConfig;
    }

    /// <summary>
    ///     Initializes a new instance with an existing Schema Registry client.
    /// </summary>
    /// <param name="schemaRegistryClient">The Schema Registry client.</param>
    /// <param name="metrics">The metrics recorder.</param>
    /// <param name="serializerConfig">Optional Protobuf serializer configuration.</param>
    /// <param name="deserializerConfig">Optional Protobuf deserializer configuration.</param>
    public ProtobufMessageSerializer(
        CachedSchemaRegistryClient schemaRegistryClient,
        IKafkaMetrics metrics,
        ProtobufSerializerConfig? serializerConfig = null,
        ProtobufDeserializerConfig? deserializerConfig = null)
    {
        _schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
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
    public byte[] Serialize<T>(T value)
    {
        if (value is null)
            return [];

        var sw = Stopwatch.StartNew();

        try
        {
            var serializer = GetOrCreateSerializer<T>();

            // Use a dummy SerializationContext - the topic is not used by the serializer for the actual serialization
            var context = new SerializationContext(MessageComponentType.Value, string.Empty);

            // Confluent serializers are async-only; Kafka expects sync serializers, so we block here.
            return serializer.SerializeAsync(value, context).GetAwaiter().GetResult();
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
        if (data is null || data.Length == 0)
            return default!;

        var sw = Stopwatch.StartNew();

        try
        {
            var deserializer = GetOrCreateDeserializer<T>();

            // Use a dummy SerializationContext - the topic is not used by the deserializer for the actual deserialization
            var context = new SerializationContext(MessageComponentType.Value, string.Empty);

            // Confluent deserializers are async-only; Kafka expects sync deserializers, so we block here.
            return deserializer.DeserializeAsync(data, data == null, context).GetAwaiter().GetResult();
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

    private IAsyncSerializer<T> GetOrCreateSerializer<T>()
    {
        return (IAsyncSerializer<T>)_serializers.GetOrAdd(typeof(T), type =>
        {
            // Verify that T implements IMessage
            if (!typeof(IMessage).IsAssignableFrom(type))
            {
                throw new InvalidOperationException(
                    $"Type {type.Name} does not implement Google.Protobuf.IMessage. " +
                    "Protobuf serialization requires generated Protobuf message types.");
            }

            // Use reflection to create the generic serializer
            var method = CreateSerializerMethod.MakeGenericMethod(type);
            return method.Invoke(this, null)!;
        });
    }

    private IAsyncDeserializer<T> GetOrCreateDeserializer<T>()
    {
        return (IAsyncDeserializer<T>)_deserializers.GetOrAdd(typeof(T), type =>
        {
            // Verify that T implements IMessage
            if (!typeof(IMessage).IsAssignableFrom(type))
            {
                throw new InvalidOperationException(
                    $"Type {type.Name} does not implement Google.Protobuf.IMessage. " +
                    "Protobuf deserialization requires generated Protobuf message types.");
            }

            // Use reflection to create the generic deserializer
            var method = CreateDeserializerMethod.MakeGenericMethod(type);
            return method.Invoke(this, null)!;
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

        if (config.AutoRegisterSchemas)
            dict["auto.register.schemas"] = "true";

        if (config.SubjectNameStrategy.HasValue)
            dict["subject.name.strategy"] = config.SubjectNameStrategy.Value.ToString().ToLowerInvariant();

        return dict;
    }

    // Called via reflection
#pragma warning disable CA1822 // Mark members as static - cannot be static due to reflection usage
    private ProtobufSerializer<T> CreateProtobufSerializer<T>()
        where T : IMessage<T>, new()
    {
        return new ProtobufSerializer<T>(_schemaRegistryClient, _serializerConfig);
    }

    private ProtobufDeserializer<T> CreateProtobufDeserializer<T>()
        where T : class, IMessage<T>, new()
    {
        // ProtobufDeserializer requires ISchemaRegistryClient interface, not the concrete type
        return new ProtobufDeserializer<T>(_schemaRegistryClient);
    }
#pragma warning restore CA1822
}

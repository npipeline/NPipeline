using System.Collections.Concurrent;
using System.Reflection;
using Confluent.Kafka;
using NPipeline.Connectors.Abstractions;

namespace NPipeline.Connectors.Kafka.Serialization;

/// <summary>
///     The sink's value serializer, over an <see cref="ISerializerProvider" />. An acknowledgable message is a wrapper,
///     so what goes on the wire is its <see cref="IAcknowledgableMessage.Body" />, serialized as the body's runtime type.
/// </summary>
/// <remarks>
///     The body is typed <see cref="object" />. Serializing it as <c>object</c> works for JSON, but Avro and Protobuf
///     choose the schema from the type argument and cannot serialize <c>object</c>, so the body's runtime type is used
///     (for example a generated Avro record, a <c>GenericRecord</c>, or a Protobuf message). Confluent's
///     <see cref="SerializationContext" /> is passed through, so a schema-registry serializer derives the subject
///     from the real topic.
/// </remarks>
/// <typeparam name="TValue">The sink's item type.</typeparam>
internal sealed class KafkaValueSerializer<TValue>(ISerializerProvider serializer) : ISerializer<TValue>
{
    private static readonly MethodInfo SerializeAsMethod = typeof(KafkaValueSerializer<TValue>)
        .GetMethod(nameof(SerializeAs), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly ConcurrentDictionary<Type, Func<ISerializerProvider, object, SerializationContext, byte[]>> BodySerializers = new();

    private readonly ISerializerProvider _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));

    public byte[] Serialize(TValue data, SerializationContext context)
    {
        return data switch
        {
            null => [],
            IAcknowledgableMessage { Body: null } => [],
            IAcknowledgableMessage acknowledgable => SerializeBody(acknowledgable.Body, context),
            _ => _serializer.Serialize(data, context),
        };
    }

    private byte[] SerializeBody(object body, SerializationContext context)
    {
        var serialize = BodySerializers.GetOrAdd(body.GetType(), static type =>
            SerializeAsMethod.MakeGenericMethod(type).CreateDelegate<Func<ISerializerProvider, object, SerializationContext, byte[]>>());

        return serialize(_serializer, body, context);
    }

    private static byte[] SerializeAs<TBody>(ISerializerProvider serializer, object body, SerializationContext context)
    {
        return serializer.Serialize((TBody)body, context);
    }
}

using Confluent.Kafka;

namespace NPipeline.Connectors.Kafka.Serialization;

/// <summary>
///     Provides serialization and deserialization for Kafka messages.
/// </summary>
public interface ISerializerProvider
{
    /// <summary>
    ///     Serializes a message to bytes.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="value">The value to serialize.</param>
    /// <returns>The serialized bytes.</returns>
    byte[] Serialize<T>(T value);

    /// <summary>
    ///     Deserializes a message from bytes.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="data">The bytes to deserialize.</param>
    /// <returns>The deserialized value.</returns>
    T Deserialize<T>(byte[] data);

    /// <summary>
    ///     Serializes a message for the topic and component (key or value) in <paramref name="context" />. The connector
    ///     calls this overload. A provider that uses a schema registry overrides it, so the subject is derived from the
    ///     real topic; the default ignores the context and calls <see cref="Serialize{T}(T)" />.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="value">The value to serialize.</param>
    /// <param name="context">The topic and component being written.</param>
    /// <returns>The serialized bytes.</returns>
    byte[] Serialize<T>(T value, SerializationContext context) => Serialize(value);

    /// <summary>
    ///     Deserializes a message read from the topic and component in <paramref name="context" />. The connector calls
    ///     this overload; the default ignores the context and calls <see cref="Deserialize{T}(byte[])" />.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="data">The bytes to deserialize.</param>
    /// <param name="context">The topic and component being read.</param>
    /// <returns>The deserialized value.</returns>
    T Deserialize<T>(byte[] data, SerializationContext context) => Deserialize<T>(data);
}

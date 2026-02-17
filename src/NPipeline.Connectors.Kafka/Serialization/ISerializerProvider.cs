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
}

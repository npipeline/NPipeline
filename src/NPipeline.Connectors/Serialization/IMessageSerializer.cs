namespace NPipeline.Connectors.Serialization;

/// <summary>
///     Shared serialization contract for connector message payloads.
///     Implementations can use System.Text.Json, MessagePack, Protobuf, etc.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    ///     Serializes a value to a byte buffer.
    /// </summary>
    /// <typeparam name="T">The type to serialize.</typeparam>
    /// <param name="value">The value to serialize.</param>
    /// <returns>A <see cref="ReadOnlyMemory{T}" /> containing the serialized bytes.</returns>
    ReadOnlyMemory<byte> Serialize<T>(T value);

    /// <summary>
    ///     Deserializes a byte buffer to a value.
    /// </summary>
    /// <typeparam name="T">The type to deserialize.</typeparam>
    /// <param name="data">The serialized byte buffer.</param>
    /// <returns>The deserialized value.</returns>
    T Deserialize<T>(ReadOnlyMemory<byte> data);

    /// <summary>
    ///     Gets the MIME content type produced by this serializer (e.g. "application/json", "application/x-msgpack").
    /// </summary>
    string ContentType { get; }
}

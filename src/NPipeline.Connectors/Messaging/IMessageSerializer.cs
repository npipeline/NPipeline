namespace NPipeline.Connectors.Messaging;

/// <summary>Where serialized bytes go or come from, for serializers that depend on it (a schema registry's subject).</summary>
/// <param name="Destination">The topic or queue.</param>
/// <param name="IsKey"><c>true</c> for a Kafka message key, <c>false</c> for a body.</param>
public readonly record struct MessageContext(string Destination, bool IsKey = false);

/// <summary>
///     Turns message bodies into bytes and back, for every message-queue connector. <see cref="JsonMessageSerializer" /> is
///     the default; Kafka adds Avro and Protobuf serializers backed by a schema registry.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>The MIME type written with each message, for example <c>application/json</c>.</summary>
    string ContentType { get; }

    /// <summary>Serializes <paramref name="value" />.</summary>
    byte[] Serialize<T>(T value, MessageContext context);

    /// <summary>Deserializes a body; throws when it is not a valid <typeparamref name="T" />.</summary>
    T Deserialize<T>(ReadOnlySpan<byte> body, MessageContext context);
}

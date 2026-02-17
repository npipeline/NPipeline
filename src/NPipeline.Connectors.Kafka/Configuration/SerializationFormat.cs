namespace NPipeline.Connectors.Kafka.Configuration;

/// <summary>
///     Supported serialization formats for Kafka messages.
/// </summary>
public enum SerializationFormat
{
    /// <summary>
    ///     JSON serialization (default, no schema registry required).
    /// </summary>
    Json,

    /// <summary>
    ///     Apache Avro serialization with Schema Registry integration.
    /// </summary>
    Avro,

    /// <summary>
    ///     Protocol Buffers serialization with Schema Registry integration.
    /// </summary>
    Protobuf,
}

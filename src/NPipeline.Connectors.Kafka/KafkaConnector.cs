using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Nodes;

namespace NPipeline.Connectors.Kafka;

/// <summary>Creates Kafka sources and sinks.</summary>
/// <example>
///     <code>
/// var orders = KafkaConnector.Source&lt;Order&gt;("kafka:9092", "orders", groupId: "billing", o => o with { AutoOffsetReset = AutoOffsetReset.Earliest });
/// var invoices = KafkaConnector.Sink&lt;Invoice&gt;("kafka:9092", "invoices");
///     </code>
/// </example>
public static class KafkaConnector
{
    /// <summary>A source that consumes <paramref name="topic" /> as a member of <paramref name="groupId" />.</summary>
    public static KafkaSourceNode<T> Source<T>(string bootstrapServers, string topic, string groupId, Func<KafkaReadOptions, KafkaReadOptions>? configure = null)
    {
        var options = new KafkaReadOptions { BootstrapServers = bootstrapServers, Topic = topic, GroupId = groupId };
        return new KafkaSourceNode<T>(configure?.Invoke(options) ?? options);
    }

    /// <summary>A sink that produces to <paramref name="topic" />.</summary>
    public static KafkaSinkNode<T> Sink<T>(string bootstrapServers, string topic, Func<KafkaWriteOptions<T>, KafkaWriteOptions<T>>? configure = null)
    {
        var options = new KafkaWriteOptions<T> { BootstrapServers = bootstrapServers, Topic = topic };
        return new KafkaSinkNode<T>(configure?.Invoke(options) ?? options);
    }
}

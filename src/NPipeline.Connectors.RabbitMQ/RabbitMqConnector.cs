using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Nodes;

namespace NPipeline.Connectors.RabbitMQ;

/// <summary>Creates RabbitMQ sources and sinks over a shared connection.</summary>
/// <example>
///     <code>
/// await using var connection = RabbitMqConnector.Connect(new RabbitMqConnectionOptions { HostName = "rabbit" });
/// var orders = RabbitMqConnector.Source&lt;Order&gt;(connection, "orders");
/// var billed = RabbitMqConnector.Sink&lt;Invoice&gt;(connection, exchange: "", routingKey: "invoices");
///     </code>
/// </example>
public static class RabbitMqConnector
{
    /// <summary>A connection for sources and sinks to share. It connects on first use; dispose it when the application stops.</summary>
    public static IRabbitMqConnectionManager Connect(RabbitMqConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return new RabbitMqConnectionManager(options);
    }

    /// <summary>A source that consumes <paramref name="queue" />.</summary>
    public static RabbitMqSourceNode<T> Source<T>(IRabbitMqConnectionManager connection, string queue,
        Func<RabbitMqReadOptions, RabbitMqReadOptions>? configure = null)
    {
        var options = new RabbitMqReadOptions { Connection = connection, Queue = queue };
        return new RabbitMqSourceNode<T>(configure?.Invoke(options) ?? options);
    }

    /// <summary>A sink that publishes to <paramref name="exchange" />; <c>""</c> with a queue's name as <paramref name="routingKey" /> publishes to that queue.</summary>
    public static RabbitMqSinkNode<T> Sink<T>(IRabbitMqConnectionManager connection, string exchange, string routingKey = "",
        Func<RabbitMqWriteOptions<T>, RabbitMqWriteOptions<T>>? configure = null)
    {
        var options = new RabbitMqWriteOptions<T> { Connection = connection, Exchange = exchange, RoutingKey = routingKey };
        return new RabbitMqSinkNode<T>(configure?.Invoke(options) ?? options);
    }
}

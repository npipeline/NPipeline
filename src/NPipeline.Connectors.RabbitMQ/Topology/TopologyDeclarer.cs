using Microsoft.Extensions.Logging;
using NPipeline.Connectors.RabbitMQ.Configuration;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Topology;

/// <summary>
///     Declares exchanges, queues, and bindings on a RabbitMQ channel.
/// </summary>
internal static class TopologyDeclarer
{
    /// <summary>
    ///     Declares source topology (queue, optional exchange, bindings).
    /// </summary>
    public static async Task DeclareSourceTopologyAsync(
        IChannel channel,
        RabbitMqSourceOptions options,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var topology = options.Topology;

        if (topology is null || !topology.AutoDeclare)
            return;

        // Declare exchange if exchange type is specified
        if (topology.ExchangeType is not null && topology.Bindings is { Count: > 0 })
        {
            foreach (var binding in topology.Bindings)
            {
                await DeclareExchangeAsync(channel, binding.Exchange, topology, logger, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        // Declare queue
        await DeclareQueueAsync(channel, options.QueueName, topology, logger, cancellationToken)
            .ConfigureAwait(false);

        // Declare bindings
        if (topology.Bindings is { Count: > 0 })
        {
            foreach (var binding in topology.Bindings)
            {
                await channel.QueueBindAsync(
                    options.QueueName,
                    binding.Exchange,
                    binding.RoutingKey,
                    binding.Arguments,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    ///     Declares sink topology (exchange, optional queue, bindings).
    /// </summary>
    public static async Task DeclareSinkTopologyAsync(
        IChannel channel,
        RabbitMqSinkOptions options,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var topology = options.Topology;

        if (topology is null || !topology.AutoDeclare)
            return;

        // Declare exchange if exchange type is specified and exchange name is not default
        if (topology.ExchangeType is not null && !string.IsNullOrEmpty(options.ExchangeName))
        {
            await DeclareExchangeAsync(channel, options.ExchangeName, topology, logger, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static async Task DeclareExchangeAsync(
        IChannel channel,
        string exchangeName,
        RabbitMqTopologyOptions topology,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var exchangeType = topology.ExchangeType ?? "direct";

        if (topology.PassiveDeclare)
        {
            await channel.ExchangeDeclarePassiveAsync(exchangeName, cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            await channel.ExchangeDeclareAsync(
                exchangeName,
                exchangeType,
                topology.Durable,
                topology.AutoDelete,
                new Dictionary<string, object?>(),
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        LogMessages.ExchangeDeclared(logger, exchangeName, exchangeType, topology.Durable);
    }

    private static async Task DeclareQueueAsync(
        IChannel channel,
        string queueName,
        RabbitMqTopologyOptions topology,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var arguments = BuildQueueArguments(topology);

        if (topology.PassiveDeclare)
        {
            await channel.QueueDeclarePassiveAsync(queueName, cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            await channel.QueueDeclareAsync(
                queueName,
                topology.Durable,
                topology.Exclusive,
                topology.AutoDelete,
                arguments,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        var queueType = topology.QueueType.ToString().ToLowerInvariant();
        LogMessages.QueueDeclared(logger, queueName, queueType, topology.Durable);
    }

    private static Dictionary<string, object?> BuildQueueArguments(RabbitMqTopologyOptions topology)
    {
        var arguments = new Dictionary<string, object?>();

        // Set queue type
        arguments["x-queue-type"] = topology.QueueType switch
        {
            QueueType.Classic => "classic",
            QueueType.Quorum => "quorum",
            QueueType.Stream => "stream",
            _ => "quorum",
        };

        // Dead-letter exchange
        if (topology.DeadLetterExchange is not null)
            arguments["x-dead-letter-exchange"] = topology.DeadLetterExchange;

        if (topology.DeadLetterRoutingKey is not null)
            arguments["x-dead-letter-routing-key"] = topology.DeadLetterRoutingKey;

        // TTL
        if (topology.MessageTtlMs.HasValue)
            arguments["x-message-ttl"] = topology.MessageTtlMs.Value;

        // Max length
        if (topology.MaxLength.HasValue)
            arguments["x-max-length"] = topology.MaxLength.Value;

        if (topology.MaxLengthBytes.HasValue)
            arguments["x-max-length-bytes"] = topology.MaxLengthBytes.Value;

        // Merge extra arguments
        if (topology.ExtraArguments is not null)
        {
            foreach (var kvp in topology.ExtraArguments)
            {
                arguments[kvp.Key] = kvp.Value;
            }
        }

        return arguments;
    }
}

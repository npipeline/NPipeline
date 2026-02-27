namespace Sample_RabbitMqConnector;

/// <summary>
///     Sample order event consumed from a RabbitMQ queue.
/// </summary>
public sealed record OrderEvent(string OrderId, string CustomerId, decimal Amount, DateTime CreatedAt);

/// <summary>
///     Enriched order event after pipeline processing.
/// </summary>
public sealed record EnrichedOrder(string OrderId, string CustomerId, decimal Amount, DateTime CreatedAt, DateTime ProcessedAt, string Region);

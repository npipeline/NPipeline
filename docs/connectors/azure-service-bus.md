---
title: "Azure Service Bus Connector"
description: "Consume from and send to Azure Service Bus queues and topics with sessions, dead-letter, and batch sending."
order: 17
---

# Azure Service Bus Connector

The `NPipeline.Connectors.Azure.ServiceBus` package provides source and sink nodes for [Azure Service Bus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/). Supports queues, topics/subscriptions, session-based message grouping, dead-letter queues, message lock auto-renewal, batch sending, and transactional sends.

## Installation

```bash
dotnet add package NPipeline.Connectors.Azure.ServiceBus
```

**Dependencies:** [Azure.Messaging.ServiceBus](https://www.nuget.org/packages/Azure.Messaging.ServiceBus) 7.x, [Azure.Identity](https://www.nuget.org/packages/Azure.Identity) 1.x

## Node Types

| Node | Description |
|------|-------------|
| `ServiceBusQueueSourceNode<T>` | Consume from a queue |
| `ServiceBusSubscriptionSourceNode<T>` | Consume from a topic subscription |
| `ServiceBusSessionSourceNode<T>` | Consume session-enabled queues |
| `ServiceBusQueueSinkNode<T>` | Send to a queue |
| `ServiceBusTopicSinkNode<T>` | Publish to a topic |

## Source Nodes

### Queue Source

```csharp
// Connection string
public ServiceBusQueueSourceNode(
    ServiceBusConfiguration configuration,
    ILogger? logger = null)

// Pre-configured ServiceBusClient
public ServiceBusQueueSourceNode(
    ServiceBusClient client,
    ServiceBusConfiguration configuration,
    ILogger? logger = null)
```

### Example

```csharp
var config = new ServiceBusConfiguration
{
    ConnectionString = "Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...",
    QueueName = "orders",
    PrefetchCount = 50,
    MaxConcurrentCalls = 4,
    AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess
};

var source = new ServiceBusQueueSourceNode<Order>(config);
```

### Session Source

For session-enabled queues (ordered processing within a session):

```csharp
var config = new ServiceBusConfiguration
{
    ConnectionString = "...",
    QueueName = "orders-sessions",
    EnableSessions = true,
    MaxConcurrentSessions = 8,
    SessionIdleTimeout = TimeSpan.FromMinutes(1)
};

var source = new ServiceBusSessionSourceNode<Order>(config);
```

## Sink Nodes

### Queue Sink

```csharp
public ServiceBusQueueSinkNode(
    ServiceBusConfiguration configuration,
    ILogger? logger = null)
```

### Topic Sink

```csharp
public ServiceBusTopicSinkNode(
    ServiceBusConfiguration configuration,
    ILogger? logger = null)
```

### Example: Batch Sending

```csharp
var config = new ServiceBusConfiguration
{
    ConnectionString = "...",
    QueueName = "processed-orders",
    EnableBatchSending = true,
    BatchSize = 50
};

var sink = new ServiceBusQueueSinkNode<ProcessedOrder>(config);
```

## Authentication

| Mode | Description |
|------|-------------|
| `ConnectionString` (default) | Standard Service Bus connection string |
| `AzureAdCredential` | Azure AD / Managed Identity (recommended for production) |
| `EndpointWithKey` | Explicit endpoint + shared access key |

```csharp
// Azure AD authentication
var config = new ServiceBusConfiguration
{
    AuthenticationMode = AzureAuthenticationMode.AzureAdCredential,
    FullyQualifiedNamespace = "mynamespace.servicebus.windows.net",
    QueueName = "orders"
};
```

## Configuration

### Connection

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ConnectionString` | `string?` | `null` | Service Bus connection string |
| `FullyQualifiedNamespace` | `string?` | `null` | Namespace (for Azure AD auth) |
| `AuthenticationMode` | `AzureAuthenticationMode` | `ConnectionString` | Auth mode |
| `QueueName` | `string?` | `null` | Queue name |
| `TopicName` | `string?` | `null` | Topic name |
| `SubscriptionName` | `string?` | `null` | Subscription name |

### Source

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `PrefetchCount` | `int` | `0` | Prefetch count |
| `MaxConcurrentCalls` | `int` | `1` | Concurrent message handlers |
| `MaxAutoLockRenewalDuration` | `TimeSpan` | `5 min` | Auto-renew message lock |
| `SubQueue` | `SubQueue` | `None` | `None`, `DeadLetter`, or `TransferDeadLetter` |
| `AcknowledgmentStrategy` | `AcknowledgmentStrategy` | `AutoOnSinkSuccess` | When to complete messages |

### Sink

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `EnableBatchSending` | `bool` | `true` | Batch messages |
| `BatchSize` | `int` | `100` | Max messages per batch (Service Bus limit: 100) |
| `EnableTransactionalSends` | `bool` | `false` | Transactional sending |

### Sessions

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `EnableSessions` | `bool` | `false` | Enable session-based processing |
| `MaxConcurrentSessions` | `int` | `8` | Concurrent sessions |
| `SessionIdleTimeout` | `TimeSpan` | `1 min` | Session idle timeout |

### Error Handling

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ContinueOnError` | `bool` | `true` | Continue on errors |
| `ContinueOnDeserializationError` | `bool` | `false` | Skip deserialization failures |
| `DeadLetterOnDeserializationError` | `bool` | `true` | Dead-letter bad messages |

## Dependency Injection

```csharp
services.AddServiceBusConnector(options =>
{
    options.ConnectionString = "Endpoint=sb://mynamespace.servicebus.windows.net/;...";
});

services.AddServiceBusQueueSource<Order>("orders", config =>
{
    config.PrefetchCount = 50;
    config.MaxConcurrentCalls = 4;
});

services.AddServiceBusQueueSink<ProcessedOrder>("processed-orders");
services.AddServiceBusTopicSink<AuditEvent>("audit-events");
```

## Next Steps

- [Kafka Connector](kafka.md) — self-managed distributed streaming
- [RabbitMQ Connector](rabbitmq.md) — self-managed message broker
- [AWS SQS Connector](aws-sqs.md) — AWS managed queuing

## Source Node Variants

| Class | Description |
|-------|-------------|
| `ServiceBusQueueSourceNode<T>` | Receive from a queue |
| `ServiceBusSubscriptionSourceNode<T>` | Receive from a topic subscription |
| `ServiceBusSessionSourceNode<T>` | Session-based processing (ordered within session) |

## Message Settlement

| Method | Description |
|--------|-------------|
| `CompleteAsync()` | Mark message as processed — removes from queue |
| `AbandonAsync()` | Release lock — message becomes available again |
| `DeadLetterAsync(reason)` | Move to dead-letter sub-queue with reason |
| `DeferAsync()` | Defer for later processing by sequence number |

```csharp
pipeline.AddTransform<ServiceBusMessage<Order>, ProcessedOrder>(async (msg, ct) =>
{
    try
    {
        var result = Process(msg.Body);
        await msg.CompleteAsync(ct);
        return result;
    }
    catch (ValidationException ex)
    {
        await msg.DeadLetterAsync(ex.Message, ct);
        throw;
    }
});
```

## Dead-Letter Processing

Read from the dead-letter sub-queue:

```csharp
var dlqConfig = new ServiceBusConfiguration
{
    ConnectionString = "...",
    QueueName = "orders",
    SubQueue = SubQueue.DeadLetter
};

var dlqSource = new ServiceBusQueueSourceNode<Order>(dlqConfig);
```

## Session-Based Processing

Sessions guarantee FIFO ordering within a session and enable stateful processing:

```csharp
var config = new ServiceBusConfiguration
{
    ConnectionString = "...",
    QueueName = "orders",
    EnableSessions = true,
    MaxConcurrentSessions = 8,
    SessionIdleTimeout = TimeSpan.FromMinutes(1)
};
```

Messages with the same `SessionId` are processed in order. Different sessions process in parallel.

## Lock Renewal

For long-running processing, configure auto-lock renewal:

```csharp
var config = new ServiceBusConfiguration
{
    MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(30),
    PrefetchCount = 0  // disable prefetch for long processing
};
```

## Best Practices

1. **Use Azure AD auth** in production — avoid connection strings
2. **Set `PrefetchCount`** based on processing speed (0 for slow consumers)
3. **Use sessions** when ordering matters within a logical group
4. **Configure `MaxAutoLockRenewalDuration`** to exceed worst-case processing time
5. **Dead-letter with reason** — include diagnostic information
6. **Use topics + subscriptions** for pub/sub fan-out
7. **Monitor dead-letter queue depth** — alerts on rising DLQ count

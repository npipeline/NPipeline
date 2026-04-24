---
title: Azure Service Bus Connector
description: Consume from and publish to Azure Service Bus with NPipeline using the Azure Service Bus connector with support for queues, topics, sessions, explicit settlement, and Azure AD authentication.
order: 3
---

## Azure Service Bus Connector

The `NPipeline.Connectors.Azure.ServiceBus` package provides source and sink nodes for Azure Service Bus,
Microsoft's fully managed enterprise message broker with message queuing and publish-subscribe capabilities.

This connector uses the official [Azure.Messaging.ServiceBus](https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/servicebus/Azure.Messaging.ServiceBus/README.md)
SDK, providing asynchronous message processing with `ServiceBusProcessor`, a channel-based push-to-pull bridge for
backpressure-aware consumption, explicit message settlement, session support, and both connection string and Azure AD
(Managed Identity / `DefaultAzureCredential`) authentication.

## Installation

```bash
dotnet add package NPipeline.Connectors.Azure.ServiceBus
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Quick Start

Here's a minimal pipeline that consumes `Order` messages from an Azure Service Bus queue, processes them, and
publishes the results to another queue:

```csharp
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.DependencyInjection;
using NPipeline.Connectors.Azure.ServiceBus.Nodes;
using NPipeline.Extensions.DependencyInjection;

services.AddNPipeline(Assembly.GetExecutingAssembly());

// Register connector (shared connection pool)
services.AddServiceBusConnector(options =>
{
    options.ConnectionString = configuration["ServiceBus:ConnectionString"];
});

// Register source and sink
services.AddServiceBusQueueSource<Order>("orders", config =>
{
    config.MaxConcurrentCalls = 5;
    config.AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess;
});

services.AddServiceBusQueueSink<ProcessedOrder>("processed-orders");
```

Alternatively, use the nodes directly without dependency injection:

```csharp
var sourceConfig = new ServiceBusConfiguration
{
    ConnectionString = "Endpoint=sb://my-namespace.servicebus.windows.net/;...",
    QueueName = "orders",
    AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess,
};

var sinkConfig = new ServiceBusConfiguration
{
    ConnectionString = "Endpoint=sb://my-namespace.servicebus.windows.net/;...",
    QueueName = "processed-orders",
};

var source = new ServiceBusQueueSourceNode<Order>(sourceConfig);
var sink   = new ServiceBusQueueSinkNode<ProcessedOrder>(sinkConfig);
```

## Configuration

All source and sink nodes share the same `ServiceBusConfiguration` class. Each property is described below.

### Connection

| Property | Type | Default | Description |
|---|---|---|---|
| `ConnectionString` | `string?` | `null` | Azure Service Bus connection string |
| `FullyQualifiedNamespace` | `string?` | `null` | Namespace hostname for Azure AD auth (e.g. `my-ns.servicebus.windows.net`) |
| `AuthenticationMode` | `AzureAuthenticationMode` | `ConnectionString` | `ConnectionString`, `AzureAdCredential`, or `EndpointWithKey` |
| `NamedConnectionName` | `string?` | `null` | Name of a registered named connection |
| `NamedConnection` | `string?` | `null` | Name of a registered named connection (equivalent to `NamedConnectionName`) |
| `Credential` | `TokenCredential?` | `null` | Azure AD credential (defaults to `DefaultAzureCredential` when not set) |
| `SharedAccessKeyName` | `string?` | `null` | Shared access key name for `EndpointWithKey` mode |
| `SharedAccessKey` | `string?` | `null` | Shared access key value for `EndpointWithKey` mode |

### Source-side

| Property | Type | Default | Description |
|---|---|---|---|
| `QueueName` | `string?` | `null` | Queue to consume from (mutually exclusive with `SubscriptionName`) |
| `TopicName` | `string?` | `null` | Topic name (required for subscription source) |
| `SubscriptionName` | `string?` | `null` | Subscription name for topic consumers |
| `MaxConcurrentCalls` | `int` | `1` | Parallel message handlers per processor |
| `PrefetchCount` | `int` | `0` | Number of messages to pre-fetch |
| `MaxAutoLockRenewalDuration` | `TimeSpan` | 5 minutes | Maximum duration for lock auto-renewal |
| `SubQueue` | `SubQueue` | `SubQueue.None` | Read from dead-letter queue or transfer DLQ |
| `EnableAutoComplete` | `bool` | `false` | Let the SDK complete messages automatically |
| `InternalBufferCapacity` | `int` | `0` | Size of the internal channel buffer (`0` = auto, defaults to `max(MaxConcurrentCalls * 2, 8)`) |

### Sink-side

| Property | Type | Default | Description |
|---|---|---|---|
| `QueueName` | `string?` | `null` | Queue to publish to (for `ServiceBusQueueSinkNode`) |
| `TopicName` | `string?` | `null` | Topic to publish to (for `ServiceBusTopicSinkNode`) |
| `EnableBatchSending` | `bool` | `true` | Use `ServiceBusMessageBatch` for efficient multi-message sends |
| `BatchSize` | `int` | `100` | Maximum messages per batch |

### Session support

| Property | Type | Default | Description |
|---|---|---|---|
| `EnableSessions` | `bool` | `false` | Enable for session-enabled entities |
| `MaxConcurrentSessions` | `int` | `8` | Max parallel sessions |
| `SessionMaxConcurrentCallsPerSession` | `int` | `1` | Max calls per session |
| `SessionIdleTimeout` | `TimeSpan` | 1 minute | How long to wait on an idle session before closing it |

### Acknowledgment

| Property | Type | Default | Description |
|---|---|---|---|
| `AcknowledgmentStrategy` | `AcknowledgmentStrategy` | `AutoOnSinkSuccess` | `AutoOnSinkSuccess`, `Manual`, or `None` |

### Error handling

| Property | Type | Default | Description |
|---|---|---|---|
| `ContinueOnDeserializationError` | `bool` | `false` | Skip deserialization failures without stopping the pipeline |
| `DeadLetterOnDeserializationError` | `bool` | `true` | Move undeserializable messages to the DLQ |
| `ContinueOnError` | `bool` | `true` | Continue processing after a sink error |

### Retry

Configure the SDK-level retry policy via `ServiceBusRetryConfiguration`:

```csharp
var config = new ServiceBusConfiguration
{
    // ...
    Retry = new ServiceBusRetryConfiguration
    {
        Mode       = ServiceBusRetryMode.Exponential, // or Fixed
        MaxRetries = 3,
        Delay      = TimeSpan.FromSeconds(1),
        MaxDelay   = TimeSpan.FromSeconds(30),
        TryTimeout = TimeSpan.FromMinutes(1),
    },
};
```

## Authentication

### Connection String

```csharp
var config = new ServiceBusConfiguration
{
    ConnectionString = "Endpoint=sb://my-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=...",
    QueueName = "orders",
};
```

### Azure AD / Managed Identity

Use this mode to authenticate without shared access keys using Azure Managed Identity or any
`TokenCredential` from `Azure.Identity`:

```csharp
var config = new ServiceBusConfiguration
{
    AuthenticationMode = AzureAuthenticationMode.AzureAdCredential,
    FullyQualifiedNamespace = "my-namespace.servicebus.windows.net",
    QueueName = "orders",
    // Credential defaults to new DefaultAzureCredential() when not set
    // Credential = new ManagedIdentityCredential(),
};
```

### Named Connection

Register a named connection once via `AzureConnectionOptions`, then reference it from
`ServiceBusConfiguration.NamedConnection` (or `NamedConnectionName`):

```csharp
var azureConnections = new AzureConnectionOptions();
azureConnections.AddOrUpdateConnection(
    "primary",
    "Endpoint=sb://my-namespace.servicebus.windows.net/;...");

services.AddSingleton(azureConnections);

services.AddServiceBusConnector();

var config = new ServiceBusConfiguration
{
    AuthenticationMode = AzureAuthenticationMode.ConnectionString,
    NamedConnectionName = "primary",
    QueueName = "orders",
};
```

## Source Nodes

### `ServiceBusQueueSourceNode<T>`

Consumes messages from a Service Bus **queue**.

```csharp
var node = new ServiceBusQueueSourceNode<Order>(new ServiceBusConfiguration
{
    ConnectionString = "...",
    QueueName = "orders",
    MaxConcurrentCalls = 10,
    PrefetchCount = 50,
});
```

### `ServiceBusSubscriptionSourceNode<T>`

Consumes messages from a **topic subscription**.

```csharp
var node = new ServiceBusSubscriptionSourceNode<OrderEvent>(new ServiceBusConfiguration
{
    ConnectionString = "...",
    TopicName = "order-events",
    SubscriptionName = "inventory-service",
    MaxConcurrentCalls = 5,
});
```

### `ServiceBusSessionSourceNode<T>`

Consumes messages from a **session-enabled** queue or subscription. Sessions guarantee ordered processing
of messages that share the same session ID.

```csharp
var node = new ServiceBusSessionSourceNode<Order>(new ServiceBusConfiguration
{
    ConnectionString = "...",
    QueueName = "session-orders",
    EnableSessions = true,
    MaxConcurrentSessions = 8,
    SessionMaxConcurrentCallsPerSession = 1,
    SessionIdleTimeout = TimeSpan.FromMinutes(2),
});
```

## Sink Nodes

### `ServiceBusQueueSinkNode<T>`

Publishes messages to a Service Bus **queue**.

```csharp
var node = new ServiceBusQueueSinkNode<ProcessedOrder>(new ServiceBusConfiguration
{
    ConnectionString = "...",
    QueueName = "processed-orders",
    BatchSize = 50,
});
```

### `ServiceBusTopicSinkNode<T>`

Publishes messages to a Service Bus **topic**.

```csharp
var node = new ServiceBusTopicSinkNode<OrderEvent>(new ServiceBusConfiguration
{
    ConnectionString = "...",
    TopicName = "order-events",
    EnableBatchSending = true,
});
```

## Message Model

All source nodes emit `ServiceBusMessage<T>`, which wraps the deserialized payload and exposes the
original `ServiceBusReceivedMessage` metadata along with explicit settlement methods.

```csharp
public class MyTransform : ITransformNode<ServiceBusMessage<Order>, ServiceBusMessage<ProcessedOrder>>
{
    public async Task<ServiceBusMessage<ProcessedOrder>?> TransformAsync(
        ServiceBusMessage<Order> message,
        CancellationToken cancellationToken)
    {
        var order = message.Body;
        
        // Access Service Bus metadata
        var messageId  = message.MessageId;
        var enqueuedAt = message.EnqueuedTime;
        var deliveries = message.DeliveryCount;
        var subject = message.Subject;

        // Process...
        var processed = new ProcessedOrder { OrderId = order.Id, Total = order.Amount * 1.1m };

        // Return new message preserving settlement callbacks
        return message.WithBody(processed);
    }
}
```

### Settlement

`ServiceBusMessage<T>` exposes explicit settlement methods:

```csharp
await message.CompleteAsync();           // Remove message from queue
await message.AbandonAsync();            // Return to queue; delivery count incremented
await message.DeadLetterAsync(          // Move to dead-letter sub-queue
    deadLetterReason: "ValidationFailed",
    deadLetterErrorDescription: "Amount must be positive");
await message.DeferAsync();              // Defer; retrieve later by SequenceNumber
```

Settlement is **idempotent** — once any settlement method has been called, subsequent calls on the same
message are no-ops. The `IsSettled` property reflects whether settlement has been performed.

### `IAcknowledgableMessage` interface

`ServiceBusMessage<T>` implements `IAcknowledgableMessage`, enabling generic pipeline components to settle
messages without depending on the Service Bus SDK:

```csharp
// Acknowledge (→ CompleteAsync)
await message.AcknowledgeAsync();

// Negative-acknowledge with requeue (→ AbandonAsync)
await message.NegativeAcknowledgeAsync(requeue: true);

// Negative-acknowledge without requeue (→ DeadLetterAsync)
await message.NegativeAcknowledgeAsync(requeue: false);
```

### `WithBody<TNew>`

When chaining transforms, use `WithBody<TNew>` to produce a new `ServiceBusMessage<TNew>` from the same
underlying received message. This preserves the settlement callbacks so that downstream settlement
completes the correct Azure message:

```csharp
return message.WithBody(new ProcessedOrder { ... });
```

## Dead-Letter Handling

### Reading from the dead-letter queue

Set `SubQueue = SubQueue.DeadLetter` on a source configuration to consume from the entity's dead-letter
sub-queue:

```csharp
var dlqConfig = new ServiceBusConfiguration
{
    ConnectionString = "...",
    QueueName = "orders",
    SubQueue = SubQueue.DeadLetter,
};

var dlqSource = new ServiceBusQueueSourceNode<DeadLetteredOrder>(dlqConfig);
```

### Automatic DLQ on deserialization failure

When `DeadLetterOnDeserializationError = true` (default), messages that cannot be deserialized are
automatically moved to the dead-letter sub-queue with the reason `DeserializationError` before the
pipeline receives them.

```csharp
var config = new ServiceBusConfiguration
{
    ConnectionString = "...",
    QueueName = "orders",
    DeadLetterOnDeserializationError = true,   // default
    ContinueOnDeserializationError = false,    // stop pipeline on error
};
```

## Acknowledgment Strategies

| Strategy | Behaviour |
|---|---|
| `Manual` | Your code (transforms/sinks) must explicitly call `CompleteAsync()`, `AbandonAsync()`, etc. |
| `AutoOnSinkSuccess` | The pipeline automatically calls `CompleteAsync()` after the sink node processes the message; calls `AbandonAsync()` on sink failure |
| `None` | No acknowledgment is performed by the pipeline; the SDK settings control lock behaviour |

## Dependency Injection

### Full registration

```csharp
services.AddNPipeline(Assembly.GetExecutingAssembly());

services.AddServiceBusConnector(options =>
{
    options.ConnectionString = configuration["ServiceBus:ConnectionString"];
});

services.AddServiceBusQueueSource<Order>("orders", config =>
{
    config.MaxConcurrentCalls = 10;
    config.AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess;
});

services.AddServiceBusSubscriptionSource<OrderEvent>("order-events", "notifications", config =>
{
    config.MaxConcurrentCalls = 5;
});

services.AddServiceBusQueueSink<ProcessedOrder>("processed-orders");
services.AddServiceBusTopicSink<OrderEvent>("order-events-out");
```

### `IPipelineDefinition`

Alternatively, encapsulate the pipeline topology in a class implementing `IPipelineDefinition`:

```csharp
public class OrderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            new ServiceBusQueueSourceNode<Order>(CreateSourceConfig()), "sb-source");
        var transform = builder.AddTransform(new OrderTransform(), "order-transform");
        var sink = builder.AddSink(
            new ServiceBusQueueSinkNode<ProcessedOrder>(CreateSinkConfig()), "sb-sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
    
    // ...
}
```

## Architecture

### Push-to-Pull Bridge

Azure Service Bus uses `ServiceBusProcessor`, which is push-based (messages are delivered to handler
callbacks). NPipeline pipelines are pull-based (`IDataStream<T>` / `ReadAsync`). The connector bridges
these models using a bounded `System.Threading.Channels.Channel<T>`:

```
ServiceBusProcessor
  (push callbacks)
        │
        ▼
  Channel<TMessage>   ← bounded, provides backpressure
        │
        ▼
  IDataStream<T>.ReadAsync
  (pipeline pull)
```

### Lock Renewal

Each message's settlement lock is maintained for the lifetime of its processing. A per-message
`TaskCompletionSource<bool>` is passed to the SDK handler — the handler blocks until the message is
settled, which keeps the lock alive and enables `MaxAutoLockRenewalDuration` to take effect.

### Connection Pool

`ServiceBusConnectionPool` maintains one `ServiceBusClient` per unique connection string / namespace,
ensuring efficient connection reuse across multiple nodes in the same process.

## Metrics

The connector emits standard NPipeline metrics through the `INodeMetrics` abstraction. Attach the
`NPipeline.Extensions.Metrics` package to expose these as OpenTelemetry counters / histograms.

| Metric | Description |
|---|---|
| `npipeline.messages.received` | Messages read from Service Bus |
| `npipeline.messages.processed` | Messages successfully processed |
| `npipeline.messages.failed` | Messages that failed processing |
| `npipeline.messages.sent` | Messages published to Service Bus |

## Serialization

Messages are serialized and deserialized as **JSON** using `System.Text.Json`. The message body on the
wire is a UTF-8 encoded JSON string set as the `ServiceBusMessage.Body`.

To customize serialization, set `JsonSerializerOptions` on your `ServiceBusConfiguration`:

```csharp
var config = new ServiceBusConfiguration
{
    ConnectionString = "...",
    QueueName = "orders",
    JsonSerializerOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter() },
    },
};
```

## Sample

A complete working sample is available in the repository under
`samples/Sample_AzureServiceBusConnector`.

The sample demonstrates:

- Connecting to a Service Bus queue with a connection string
- Consuming `Order` messages and transforming them to `ProcessedOrder`
- The `OrderProcessor` transform validating and enriching messages
- Automatic settlement via `AcknowledgmentStrategy.AutoOnSinkSuccess`

## Troubleshooting

### Messages are not being received

- Verify the connection string or Azure AD credentials grant `Azure Service Bus Data Receiver` role.
- Confirm the queue or subscription name is correct and the entity exists.
- Check that `MaxConcurrentCalls` > 0 and the internal channel buffer is not saturated.

### Messages are repeatedly delivered (high delivery count)

- If using `AcknowledgmentStrategy.Manual`, ensure transforms and sinks call `CompleteAsync()` on success.
- If using `AutoOnSinkSuccess`, verify the sink node completes without throwing.
- Check `MaxAutoLockRenewalDuration` is long enough for your processing time.

### `ServiceBusException: MessagingEntityNotFound`

- Confirm the queue, topic, or subscription exists in the Service Bus namespace.
- Match the casing exactly — entity names are case-sensitive.

### Azure AD: `AuthorizationFailedException`

- Assign the `Azure Service Bus Data Owner`, `Azure Service Bus Data Sender`, or `Azure Service Bus Data Receiver`
  RBAC role on the namespace or entity to the Managed Identity / service principal.
- When developing locally, ensure `az login` or `AZURE_*` environment variables are configured for
  `DefaultAzureCredential`.

### Deserialization errors flooding the DLQ

- Set `ContinueOnDeserializationError = true` to skip bad messages without stopping.
- Set `DeadLetterOnDeserializationError = false` to abandon instead of dead-letter.
- Use the DLQ source (`SubQueue = SubQueue.DeadLetter`) to inspect and reprocess failed messages.

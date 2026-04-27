# NPipeline Azure Service Bus Connector

Azure Service Bus connector for NPipeline — integrate with Microsoft Azure Service Bus for
enterprise-grade message queuing and pub/sub messaging.

## Features

- **Queue & Topic/Subscription Source Nodes**: Consume messages from queues or topic subscriptions with type-safe JSON deserialization
- **Queue & Topic Sink Nodes**: Publish messages to queues or topics with batched sending
- **Session Support**: First-class support for session-enabled queues and subscriptions via `ServiceBusSessionSourceNode`
- **Explicit Settlement**: Full access to Complete, Abandon, Dead-Letter, and Defer operations via `ServiceBusMessage<T>`
- **Message Lock Renewal**: Automatic lock renewal during long-running processing
- **Multiple Auth Modes**: Connection string, Azure AD (Managed Identity / DefaultAzureCredential), and named connections
- **Acknowledgment Strategies**: `AutoOnSinkSuccess`, `Manual`, and `None` — with idempotent settlement
- **Dead-Letter Routing**: Automatic dead-lettering of deserialization failures
- **Retry Configuration**: Exponential and fixed-mode retry with configurable delay/timeout
- **Channel Bridge**: Push-to-pull bridge using `System.Threading.Channels` for backpressure-aware processing

## Installation

```bash
dotnet add package NPipeline.Connectors.Azure.ServiceBus
```

## Quick Start

```csharp
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Nodes;

// Source: consume from a queue
var config = new ServiceBusConfiguration
{
    ConnectionString = "Endpoint=sb://...",
    QueueName = "orders",
    AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess,
};

var source = builder.AddSource(new ServiceBusQueueSourceNode<Order>(config), "sb-source");
var sink = builder.AddSink(new ServiceBusQueueSinkNode<ProcessedOrder>(sinkConfig), "sb-sink");
```

## Configuration

```csharp
var config = new ServiceBusConfiguration
{
    // ── Connection ──────────────────────────────────────────────────────────────
    ConnectionString = "<connection-string>",
    // Or for Azure AD authentication:
    // AuthenticationMode = AzureAuthenticationMode.AzureAdCredential,
    // FullyQualifiedNamespace = "my-namespace.servicebus.windows.net",
    // Credential = new DefaultAzureCredential(),

    // ── Source Options ──────────────────────────────────────────────────────────
    QueueName = "my-queue",               // Queue source/sink
    // TopicName = "my-topic",            // For topic sink
    // SubscriptionName = "my-sub",       // For subscription source
    MaxConcurrentCalls = 5,               // Parallel message handlers (default: 1)
    PrefetchCount = 20,                   // Pre-fetch buffer (default: 0)
    MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(10),

    // ── Sink Options ──────────────────────────────────────────────────────────
    EnableBatchSending = true,            // Use ServiceBusMessageBatch (default: true)
    BatchSize = 100,                      // Max messages per batch (default: 100)

    // ── Acknowledgment ────────────────────────────────────────────────────────
    AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess,

    // ── Session Options (session-enabled entities only) ───────────────────────
    EnableSessions = false,               // Set to true for ServiceBusSessionSourceNode
    MaxConcurrentSessions = 8,
    SessionMaxConcurrentCallsPerSession = 1,

    // ── Error Handling ────────────────────────────────────────────────────────
    ContinueOnDeserializationError = false,
    DeadLetterOnDeserializationError = true,
    ContinueOnError = true,               // Sink errors (default: true)

    // ── Retry ─────────────────────────────────────────────────────────────────
    Retry = new ServiceBusRetryConfiguration
    {
        Mode = ServiceBusRetryMode.Exponential,
        MaxRetries = 3,
        Delay = TimeSpan.FromSeconds(1),
        MaxDelay = TimeSpan.FromSeconds(30),
    },
};
```

## Settlement

Each message received by a source node is wrapped in a `ServiceBusMessage<T>` that exposes explicit settlement:

```csharp
// In a transform node:
await message.CompleteAsync();    // Remove from queue
await message.AbandonAsync();     // Return to queue for redelivery
await message.DeadLetterAsync("Reason", "Description"); // Move to DLQ
await message.DeferAsync();       // Defer (receive later by sequence number)

// Via the IAcknowledgableMessage interface:
await message.AcknowledgeAsync();             // → CompleteAsync()
await message.NegativeAcknowledgeAsync();     // → AbandonAsync() (requeue=true)
await message.NegativeAcknowledgeAsync(false); // → DeadLetterAsync()
```

Settlement is **idempotent** — calling any settlement method multiple times is safe, only the first call takes effect.

## Dependency Injection

```csharp
services.AddServiceBusConnector(options =>
{
    options.ConnectionString = configuration["ServiceBus:ConnectionString"];
});

// Register individual nodes
services.AddServiceBusQueueSource<Order>("orders", config =>
{
    config.MaxConcurrentCalls = 10;
});

services.AddServiceBusQueueSink<ProcessedOrder>("processed-orders");
```

## Session-Aware Processing

```csharp
var config = new ServiceBusConfiguration
{
    ConnectionString = "...",
    QueueName = "session-queue",
    EnableSessions = true,
    MaxConcurrentSessions = 4,
    SessionMaxConcurrentCallsPerSession = 1,
    SessionIdleTimeout = TimeSpan.FromMinutes(2),
};

var source = new ServiceBusSessionSourceNode<Order>(config);
```

## Dead-Letter Queue Reading

```csharp
var config = new ServiceBusConfiguration
{
    ConnectionString = "...",
    QueueName = "my-queue",
    SubQueue = SubQueue.DeadLetter,  // Read from DLQ
};
```

## Documentation

For comprehensive documentation, see [Azure Service Bus Connector Documentation](https://docs.npipeline.net/connectors/azure-service-bus).

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Connectors.Azure](https://www.nuget.org/packages/NPipeline.Connectors.Azure)** - Shared Azure authentication and utilities
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## Requirements

- .NET 8.0, 9.0, or 10.0
- Azure.Messaging.ServiceBus 7.20.1+ (automatically included)
- Azure.Identity 1.18.0+ (automatically included)
- NPipeline.Connectors.Azure (automatically included)

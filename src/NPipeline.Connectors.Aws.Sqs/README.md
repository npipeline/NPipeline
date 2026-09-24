# NPipeline AWS SQS Connector

AWS SQS connector for NPipeline - integrate with Amazon Simple Queue Service for reliable message queuing.

## Features

- **Source & Sink Nodes**: Read from and write to SQS queues with type-safe JSON serialization
- **Automatic Acknowledgment**: Multiple strategies (AutoOnSinkSuccess, Manual, Delayed, None) with batch optimization
- **Long Polling**: Cost-efficient message retrieval with configurable wait times
- **Parallel Processing**: Optional parallel message processing for high-throughput scenarios
- **Error Handling**: Retries through the AWS SDK's standard retry mode (configurable with `RetryMode` and `MaxErrorRetry`)
- **Multiple Credential Methods**: Support for access keys, AWS profiles, and default credential chains

## Installation

```bash
dotnet add package NPipeline.Connectors.Aws.Sqs
```

## Quick Start

```csharp
using NPipeline.Connectors.AwsSqs.Configuration;
using NPipeline.Connectors.AwsSqs.Nodes;

var config = new SqsConfiguration
{
    Region = "us-east-1",
    SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/input-queue",
    SinkQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/output-queue"
};

var source = builder.AddSource(new SqsSourceNode<OrderMessage>(config), "sqs-source");
var sink = builder.AddSink(new SqsSinkNode<ProcessedOrder>(config), "sqs-sink");
```

## Retries

The connector leaves retries to the AWS SDK. The SDK's retry knows which SQS errors are throttling and which are
transient, backs off with jitter, and spends from a retry quota so a failing endpoint isn't flooded. The nodes don't
retry on top of it: an exception that reaches a node has already used up the SDK's retries, and it fails the node.

Two settings configure the SDK client that the nodes create:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `RetryMode` | `RequestRetryMode?` | `Standard` | `Standard` retries throttling, 5xx, and network errors with jittered exponential backoff. `Adaptive` also slows the client down when SQS throttles it. `null` lets the SDK decide (`AWS_RETRY_MODE` or the shared config file) |
| `MaxErrorRetry` | `int?` | `3` | Retries per call, not total attempts. The default makes up to four attempts, the same count as the removed `MaxRetries = 3`. `0` turns retries off. `null` lets the SDK decide (`AWS_MAX_ATTEMPTS`, the shared config file, or the SDK default of 2) |

If you pass your own `IAmazonSQS` to a node's constructor, the connector can't reconfigure it, and these two settings
are ignored. Set the retry behavior on the client's own configuration:

```csharp
var client = new AmazonSQSClient(new AmazonSQSConfig
{
    RegionEndpoint = RegionEndpoint.USEast1,
    RetryMode = RequestRetryMode.Standard,
    MaxErrorRetry = 3,
});

var source = new SqsSourceNode<Order>(client, configuration);
```

`AcknowledgmentDelayMs` and `PollingIntervalMs` are pacing, not retry settings, and are unaffected.

SQS standard queues deliver at least once, and a retried `SendMessage` whose first attempt reached SQS can enqueue the
message twice. Use a FIFO queue with a deduplication ID, or make consumers idempotent, if duplicates matter.

## Documentation

For comprehensive documentation, see [AWS SQS Connector Documentation](https://docs.npipeline.net/connectors/aws-sqs).

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## Requirements

- .NET 8.0, 9.0, or 10.0
- AWSSDK.SQS 4.0.2.14+ (automatically included)
- AWSSDK.Extensions.NETCore.Setup 4.0.3.22+ (automatically included)
- NPipeline.Connectors (automatically included)

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.

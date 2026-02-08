# NPipeline AWS SQS Connector

AWS SQS connector for NPipeline - integrate with Amazon Simple Queue Service for reliable message queuing.

## Features

- **Source & Sink Nodes**: Read from and write to SQS queues with type-safe JSON serialization
- **Automatic Acknowledgment**: Multiple strategies (AutoOnSinkSuccess, Manual, Delayed, None) with batch optimization
- **Long Polling**: Cost-efficient message retrieval with configurable wait times
- **Parallel Processing**: Optional parallel message processing for high-throughput scenarios
- **Error Handling**: Built-in retry logic with exponential backoff for transient errors
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

## Documentation

For comprehensive documentation, see [AWS SQS Connector Documentation](https://www.npipeline.dev/docs/connectors/aws-sqs).

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## Requirements

- .NET 8.0, 9.0, or 10.0
- AWSSDK.SQS 4.0.2.14+ (automatically included)
- AWSSDK.Extensions.NETCore.Setup 4.0.3.22+ (automatically included)
- NPipeline.Connectors (automatically included)

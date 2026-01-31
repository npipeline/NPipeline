# NPipeline.Extensions.Lineage

Comprehensive data lineage tracking and provenance capabilities for NPipeline pipelines.

## Overview

The `NPipeline.Extensions.Lineage` extension provides production-ready lineage tracking for data flowing through NPipeline pipelines. Track the complete journey
of each data item from source to destination, enabling data governance, debugging, audit trails, and data discovery.

## Key Features

- **Item-level lineage tracking**: Trace individual data items as they flow through each node in the pipeline
- **Pipeline-level reports**: Generate high-level reports showing pipeline structure, nodes, edges, and data flow patterns
- **Configurable sampling**: Reduce overhead with deterministic or random sampling strategies
- **Thread-safe collection**: Lineage data is collected safely across parallel and concurrent pipeline executions
- **Flexible sinks**: Built-in logging sinks with support for custom lineage sinks (e.g., databases, file systems, external services)
- **Dependency injection integration**: Seamlessly integrates with Microsoft.Extensions.DependencyInjection
- **Data redaction**: Optionally exclude actual data from lineage records to reduce memory usage and improve security
- **Comprehensive metadata**: Capture hop timestamps, decision flags, observed cardinality, and ancestry information

## Installation

```bash
dotnet add package NPipeline.Extensions.Lineage
```

## Requirements

- **.NET 8.0, 9.0, or 10.0**
- **Microsoft.Extensions.DependencyInjection.Abstractions** 10.0.0 or later
- **Microsoft.Extensions.Logging.Abstractions** 10.0.0 or later
- **NPipeline** core package

## Quick Start

Enable lineage tracking with dependency injection:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Lineage.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

var services = new ServiceCollection();

// Add NPipeline core services
services.AddNPipeline(Assembly.GetExecutingAssembly());

// Add lineage services with default logging sink
services.AddNPipelineLineage();

var serviceProvider = services.BuildServiceProvider();
```

Or configure directly on your pipeline builder:

```csharp
using NPipeline.Pipeline;
using NPipeline.Lineage;

var builder = new PipelineBuilder("MyPipeline");

// Enable item-level lineage tracking
builder.EnableItemLevelLineage();

// Add a logging sink for pipeline-level lineage reports
builder.UseLoggingPipelineLineageSink();

// Build and execute the pipeline
var pipeline = builder.Build();
await pipeline.ExecuteAsync(serviceProvider);
```

## Configuration

Lineage tracking behavior is configurable through [`LineageOptions`](LineageOptions.cs):

- **Sampling**: Control overhead with deterministic or random sampling (e.g., sample 1 in 10 items)
- **Data redaction**: Exclude actual data from lineage records to reduce memory usage
- **Metadata capture**: Configure which metadata to capture (timestamps, decisions, cardinality)
- **Memory management**: Set materialization caps and overflow policies

For detailed configuration options, see
the [Configuration documentation](https://github.com/npipeline/NPipeline/tree/main/docs/extensions/lineage/configuration.md).

## Dependency Injection

Basic registration with default logging sink:

```csharp
services.AddNPipelineLineage();
```

Custom sink types and factory delegates are supported. See the main documentation for advanced registration scenarios.

## Common Use Cases

- **Data Governance**: Track data provenance and transformation history for compliance requirements (GDPR, HIPAA, SOX)
- **Debugging**: Quickly identify which node introduced data quality issues or transformation errors
- **Audit Trails**: Prove data integrity and maintain complete transformation history
- **Data Discovery**: Find what data depends on a particular source or transformation
- **Root Cause Analysis**: Trace issues back to their origin in complex pipelines
- **Performance Monitoring**: Understand data flow patterns and identify bottlenecks
- **Testing**: Verify that data flows through the expected path in complex pipelines

## Examples

See the [Sample_LineageExtension](https://github.com/npipeline/NPipeline/tree/main/samples/Sample_LineageExtension/) project for complete working examples
demonstrating:

- Basic lineage tracking
- Deterministic and random sampling
- Complex join pipelines
- Branching with lineage
- Error handling with lineage
- Custom lineage sinks

## Documentation

For comprehensive documentation including detailed examples, configuration options, API reference, and troubleshooting:

- **[Extension Overview](https://github.com/npipeline/NPipeline/tree/main/docs/extensions/lineage/index.md)** - High-level overview and use cases
- **[Configuration Guide](https://github.com/npipeline/NPipeline/tree/main/docs/extensions/lineage/configuration.md)** - Detailed configuration options
- **[Architecture](https://github.com/npipeline/NPipeline/tree/main/docs/extensions/lineage/architecture.md)** - System architecture and design
- **[Performance](https://github.com/npipeline/NPipeline/tree/main/docs/extensions/lineage/performance.md)** - Performance considerations and optimization
- **[Extension Samples](https://github.com/npipeline/NPipeline/tree/main/docs/samples/extensions.md)** - Sample applications for all extensions
- **[NPipeline Core Concepts](https://github.com/npipeline/NPipeline/tree/main/docs/core-concepts/index.md)** - Core pipeline concepts
- **[NPipeline Extensions](https://github.com/npipeline/NPipeline/tree/main/docs/extensions/index.md)** - Other available extensions

## License

MIT License - see LICENSE file for details.

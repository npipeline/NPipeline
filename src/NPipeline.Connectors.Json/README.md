# NPipeline JSON Connector

Source and sink nodes for reading and writing JSON files in NPipeline pipelines, built on System.Text.Json. Records
are deserialized straight from UTF-8, one at a time, so files of any size stream with constant memory, and a record
that fails to convert can be skipped without stopping the read.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.Json
```

Targets .NET 8.0, 9.0 and 10.0.

## Features

- **Every common shape**: root arrays, NDJSON and JSON Lines (records may span lines), single objects, and arrays nested
  in a wrapper object (`ItemsPath = "data.items"`), detected per file.
- **Full System.Text.Json support**: nested objects, lists, enums, records, custom converters, your own
  `JsonSerializerOptions`, and source-generated `JsonTypeInfo<T>` for trimming and Native AOT.
- **Shared attributes**: `[Column]` and `[IgnoreColumn]` work as in the CSV and Excel connectors.
- **Row errors**: a record that does not convert names its position and JSON path, and can fail the read, be skipped
  or go to the pipeline's dead-letter sink. A malformed NDJSON line is skipped the same way.
- **Streaming writes**: arrays and NDJSON (chosen by file name) are flushed every 64 KB.
- **Many files and compression**: directories, globs, and `.gz`, `.br` or `.zz` files, through any storage provider.
- **Metrics and traces**: rows, bytes, files and row errors through `System.Diagnostics.Metrics` (`NPipeline.Connectors`).

## Usage

```csharp
using NPipeline.Connectors.Json;
using NPipeline.StorageProviders.Models;

public sealed record Order(int Id, string Customer, decimal Total, List<string> Tags);

public sealed class OrdersPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(JsonConnector.Source<Order>(StorageUri.FromFilePath("orders.json")), "orders");
        var sink = builder.AddSink(JsonConnector.Sink<Order>(StorageUri.FromFilePath("orders.ndjson.gz")), "copy");

        builder.Connect(source, sink);
    }
}
```

Read an array inside an API export, skipping records that do not convert:

```csharp
var source = JsonConnector.Source<Order>(uri, o => o with
{
    ItemsPath = "data.orders",
    RowErrorHandler = _ => RowErrorAction.Skip,
});
```

See the [JSON connector documentation](https://docs.npipeline.net/connectors/json) for every option, and
[File Connectors: Shared Behaviour](https://docs.npipeline.net/connectors/file-connectors) for globs, compression,
commit behavior, row errors and metrics.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.

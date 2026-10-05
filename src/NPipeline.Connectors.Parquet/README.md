# NPipeline Parquet Connector

Source and sink nodes for reading and writing Apache Parquet files in NPipeline pipelines. File I/O uses
[Parquet.Net](https://github.com/aloneguid/parquet-dotnet); mapping uses NPipeline's shared engine, compiled once per
file layout, so records are built from typed column arrays without boxing or name lookups.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.Parquet
```

Targets .NET 8.0, 9.0 and 10.0.

## Features

- **Typed columnar mapping**: each row group's mapped columns are read into typed arrays, and columns a record does not
  map are never read.
- **Any record shape**: setters, `init` accessors, `required` members, positional records and primary constructors;
  `[Column]`, `[ParquetColumn]`, `[IgnoreColumn]` and `[ParquetDecimal]` control the mapping.
- **Full type coverage**: every .NET scalar, including unsigned integers, `Guid` (`UUID`), `DateOnly`, `TimeOnly`,
  `DateTimeOffset` and enums, plus `LIST` columns from arrays and lists, with null lists, empty lists and null
  elements kept apart.
- **Pruning**: a `RowGroupFilter` skips row groups by their column statistics before reading them, and a `RowFilter`
  skips rows before mapping.
- **Partitions**: files under `key=value` directories fill members named after the keys.
- **Bounded memory**: the sink flushes row groups by row count or by buffered bytes, whichever comes first.
- **Many files**: read a directory or a glob through any storage provider, several files at once with
  `FileReadParallelism`, in path order.
- **Strict conversion**: a value that does not fit its member is a row error, which can fail the read, be skipped, or go
  to the pipeline's dead-letter sink.
- **Metrics and traces**: rows, bytes, files and row errors through `System.Diagnostics.Metrics` (`NPipeline.Connectors`).

## Usage

```csharp
using NPipeline.Connectors.Parquet;
using NPipeline.StorageProviders.Models;

public sealed record Order(int Id, string Customer, decimal Total, DateTimeOffset PlacedAt);

public sealed class OrdersPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(ParquetConnector.Source<Order>(StorageUri.Parse("s3://bucket/orders/")), "orders");
        var sink = builder.AddSink(ParquetConnector.Sink<Order>(StorageUri.FromFilePath("orders.parquet")), "copy");

        builder.Connect(source, sink);
    }
}
```

Options are immutable records, adjusted with `with`:

```csharp
var sink = ParquetConnector.Sink<Order>(uri, o => o with { Codec = CompressionMethod.Zstd, RowGroupSize = 250_000 });

var source = ParquetConnector.Source<Order>(uri, o => o with
{
    FileReadParallelism = 4,
    RowGroupFilter = group => !group.TryGetRange<DateTimeOffset>("PlacedAt", out _, out var max) || max >= since,
    RowErrorHandler = _ => RowErrorAction.Skip,
});
```

Map rows by hand when you need to:

```csharp
var source = ParquetConnector.Source(uri, row => new Order(
    row.Get<int>("id"),
    row.Get<string>("customer"),
    row.GetOrDefault("total", 0m),
    row.Get<DateTimeOffset>("placed_at")));
```

See the [Parquet connector documentation](https://docs.npipeline.net/connectors/parquet) for the type mapping and every
option, and [File Connectors: Shared Behaviour](https://docs.npipeline.net/connectors/file-connectors) for globs,
commit behavior, row errors and metrics.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Connectors.DataLake](https://www.nuget.org/packages/NPipeline.Connectors.DataLake)** - Partitioned Parquet tables with snapshots and time travel
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.

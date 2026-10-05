# NPipeline CSV Connector

Source and sink nodes for reading and writing CSV and TSV files in NPipeline pipelines. Parsing uses
[CsvHelper](https://joshclose.github.io/CsvHelper/); mapping uses NPipeline's shared engine, which binds columns to
members once per file and converts values strictly and culture-invariantly.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.Csv
```

Targets .NET 8.0, 9.0 and 10.0.

## Features

- **Name-based binding**: columns bind to members case-insensitively, once per file; `[Column]` and `[IgnoreColumn]`
  control the mapping, and naming policies (snake_case, camelCase…) convert member names.
- **Any record shape**: setters, `init` accessors, `required` members, positional records and primary constructors.
- **Strict conversion**: a value that does not fit its member is a row error, never a silent default. Row errors can
  fail the read, be skipped, or go to the pipeline's dead-letter sink.
- **Round trips**: the sink writes ISO 8601 dates and invariant numbers, which the source reads back unchanged on any
  machine.
- **Many files**: read a directory or a glob (`s3://bucket/2026/*.csv`) through any storage provider.
- **Compression**: `.gz`, `.br` and `.zz` files are decompressed and compressed transparently.
- **Safe writes**: the output file appears only when the write is complete, and a failed or cancelled write leaves the target as it was.
- **Metrics and traces**: rows, bytes, files and row errors through `System.Diagnostics.Metrics` (`NPipeline.Connectors`).

## Usage

```csharp
using NPipeline.Connectors.Csv;
using NPipeline.StorageProviders.Models;

public sealed record Order(int Id, string Customer, decimal Total, DateTimeOffset PlacedAt);

public sealed class OrdersPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(CsvConnector.Source<Order>(StorageUri.FromFilePath("orders.csv")), "orders");
        var sink = builder.AddSink(CsvConnector.Sink<Order>(StorageUri.FromFilePath("orders-copy.csv.gz")), "copy");

        builder.Connect(source, sink);
    }
}
```

Options are immutable records, adjusted with `with`:

```csharp
var source = CsvConnector.Source<Order>(uri, o => o with
{
    Delimiter = ";",
    Culture = CultureInfo.GetCultureInfo("de-DE"),
    RowErrorHandler = _ => RowErrorAction.Skip,
});
```

Map rows by hand when you need to:

```csharp
var source = CsvConnector.Source(uri, row => new Order(
    row.Get<int>("id"),
    row.Get<string>("customer"),
    row.Get<decimal>("total"),
    row.Get<DateTimeOffset>("placed_at")));
```

See the [CSV connector documentation](https://docs.npipeline.net/connectors/csv) for every option, and
[File Connectors: Shared Behaviour](https://docs.npipeline.net/connectors/file-connectors) for globs, compression,
commit behavior, row errors and metrics.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.

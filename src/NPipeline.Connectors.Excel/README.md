# NPipeline Excel Connector

Source and sink nodes for Excel workbooks in NPipeline pipelines. The source reads a sheet of an XLSX, XLSM or XLS
file with ExcelDataReader; the sink streams XLSX workbooks with typed, formatted cells and constant memory.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.Excel
```

Targets .NET 8.0, 9.0 and 10.0.

## Features

- **Header binding**: columns bind to members by header, case-insensitively, once per sheet; `[Column]` and
  `[IgnoreColumn]` control the mapping. Positional records, `init` accessors and `required` members work.
- **Strict conversion**: `1.7` is not an `int`, text is parsed culture-invariantly, and date cells read as `DateTime`,
  `DateOnly`, `DateTimeOffset` or `TimeOnly`. Bad cells are row errors: fail, skip or dead-letter.
- **Sheet selection**: by name or position, with title rows skipped and empty rows ignored.
- **Streaming writes**: rows go to storage as they are written, to any provider, including S3 and Azure.
- **Typed cells**: numbers, booleans and dates are real cells, and dates are formatted as dates. Leading and trailing
  spaces, line breaks and control characters round-trip.
- **Presentation**: a bold header, optionally frozen and with filter buttons.
- **Limits enforced**: Excel's row, column and cell-length limits fail with a clear message instead of a corrupt file.

## Usage

```csharp
using NPipeline.Connectors.Excel;
using NPipeline.StorageProviders.Models;

public sealed record Order(int Id, string Customer, decimal Total, DateOnly Placed);

public sealed class OrdersPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            ExcelConnector.Source<Order>(StorageUri.FromFilePath("orders.xlsx"), o => o with { SheetName = "Orders" }),
            "orders");

        var sink = builder.AddSink(
            ExcelConnector.Sink<Order>(StorageUri.FromFilePath("report.xlsx"), o => o with { FreezeHeader = true, AutoFilter = true }),
            "report");

        builder.Connect(source, sink);
    }
}
```

See the [Excel connector documentation](https://docs.npipeline.net/connectors/excel) for every option, and
[File Connectors: Shared Behaviour](https://docs.npipeline.net/connectors/file-connectors) for globs, commit behavior,
row errors and metrics.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.

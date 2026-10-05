---
title: "Excel Connector"
description: "Read XLS and XLSX sheets and stream XLSX workbooks with typed, formatted cells."
order: 5
---

# Excel Connector

The `NPipeline.Connectors.Excel` package reads a sheet of an XLSX, XLSM or legacy XLS workbook with
[ExcelDataReader](https://github.com/ExcelDataReader/ExcelDataReader), and writes XLSX workbooks with a streaming
writer. Columns bind to members by header, cells convert strictly, and written numbers, booleans and dates are typed
cells that Excel shows as numbers, checkboxes and dates.

Globs, commit behavior, row errors and metrics work the same in every file connector; see
[File Connectors: Shared Behaviour](file-connectors.md).

## Installation

```bash
dotnet add package NPipeline.Connectors.Excel
```

**Dependencies:** [ExcelDataReader](https://www.nuget.org/packages/ExcelDataReader) 3.x, `NPipeline.Connectors`,
`NPipeline.StorageProviders`

## Reading

```csharp
public sealed record Order(int Id, string Customer, decimal Total, DateOnly Placed);

var source = ExcelConnector.Source<Order>(StorageUri.FromFilePath("orders.xlsx"), o => o with { SheetName = "Orders" });
builder.AddSource(source, "orders");
```

The header row binds to `Order`'s members case-insensitively, ignoring spaces around header text. Header cells may be
numbers or dates (a `2024` column). Workbooks are zip archives, so a stream that cannot seek (S3, SFTP) is first copied
to a temporary file.

### Cell conversion

| Cell | Converts to |
| --- | --- |
| Number | Numeric members when no information is lost (`3` to `int`, but `1.7` is an error); `DateTime` and the other date types as an Excel date |
| Text | Any member type, parsed culture-invariantly (`"42"` to `int`, `"2026-01-02"` to `DateOnly`) |
| Boolean | `bool` |
| Date | `DateTime` (UTC), `DateTimeOffset`, `DateOnly` (at midnight), `TimeOnly` |
| Empty | `null` for nullable members; an error for other value types |

A cell that does not convert is a row error. Its `RowError` names the column, and the raw excerpt names the sheet and
row and lists the row's cells (`Orders row 17: 16 | Ada | twelve`).

### Manual mapping

```csharp
var source = ExcelConnector.Source(uri, row => new Order(
    row.Get<int>("Order No"),
    row.Get<string>("Customer"),
    row.TryGet<decimal>("Total", out var total) ? total : 0m,
    row.Get<DateOnly>(3)));
```

| `ExcelRow` member | Description |
| --- | --- |
| `Get<T>(string name)`, `Get<T>(int index)` | Reads and converts a cell; throws `FieldMappingException` naming the column |
| `TryGet<T>(string name, out T value)`, `TryGet<T>(int index, out T value)` | The lenient form |
| `this[string name]`, `this[int index]` | The cell's raw value: `double`, `string`, `bool`, `DateTime` or `null` |
| `HasColumn(string name)` | Whether the header has the column |
| `Headers`, `SheetName`, `RowNumber`, `RecordNumber`, `FieldCount` | The header, the sheet, the row as Excel numbers it, its position among data rows, and its cell count |

### Read options

| Option | Default | Description |
| --- | --- | --- |
| `SheetName` | `null` | The sheet to read, matched case-insensitively |
| `SheetIndex` | `0` | The sheet to read by position when `SheetName` is `null` |
| `HasHeader` | `null` | `null`: a header is expected, except for a single-value type read without a mapper |
| `SkipRows` | `0` | Rows above the header to skip, such as a title |
| `SkipEmptyRows` | `true` | Skip rows whose cells are all empty |
| `Password` | `null` | The password of an encrypted workbook |
| `Naming` | `AsIs` | How member names become column names |
| `MissingColumns` | `ThrowForRequired` | What a member without a column means; see [column binding](file-connectors.md#column-binding) |

Plus the [shared source options](file-connectors.md#options-every-file-source-and-sink-has). A directory reads its
`.xlsx`, `.xlsm` and `.xls` files. Stream compression does not apply, since workbooks are already compressed.

## Writing

```csharp
var sink = ExcelConnector.Sink<Order>(StorageUri.FromFilePath("orders.xlsx"), o => o with
{
    SheetName = "Orders",
    FreezeHeader = true,
    AutoFilter = true,
});
```

The sink writes a bold header of member names, then one row per item. The workbook is streamed: rows go to storage as
they are written, so memory usage remains constant regardless of the number of rows, and object stores need no buffer.

| Type | Cell |
| --- | --- |
| Integers, `double`, `float` | Number |
| `decimal` | Number, or text when a workbook's double cannot hold it exactly |
| `long`, `ulong` beyond ±2^53 | Text, so no digit is lost |
| `bool` | Boolean |
| `DateTime`, `DateTimeOffset` | Date and time (`yyyy-mm-dd hh:mm:ss`), in UTC |
| `DateOnly` | Date (`yyyy-mm-dd`) |
| `TimeOnly` | Time (`h:mm:ss`) |
| `string`, `char` | Text; leading and trailing spaces and line breaks are kept |
| `TimeSpan`, `Guid`, enums, `byte[]` | Text, as the other connectors write them |
| `null` | An empty cell |

Excel stores dates to the millisecond, so finer precision is lost. Characters XML cannot hold (control characters)
are written with Excel's `_xHHHH_` escapes and read back unchanged.

A sheet holds at most 1,048,576 rows and 16,384 columns, and a cell at most 32,767 characters: the write fails past
those limits, naming the cell. A `null` item fails the write by default; set `NullItems = NullItemHandling.Skip` to
drop it.

### Manual writing

```csharp
var sink = ExcelConnector.Sink<Order>(
    StorageUri.FromFilePath("summary.xlsx"),
    ["Customer", "Total"],
    (row, order) =>
    {
        row.Write(order.Customer);
        row.Write(order.Total);
    });
```

### Write options

| Option | Default | Description |
| --- | --- | --- |
| `SheetName` | `Sheet1` | 1 to 31 characters, without `[ ] : * ? / \` |
| `HasHeader` | `null` | `null`: records get a header and single-value types do not |
| `BoldHeader` | `true` | Make the header row bold |
| `FreezeHeader` | `false` | Keep the header visible while scrolling |
| `AutoFilter` | `false` | Add filter buttons to the header |
| `Naming` | `AsIs` | How member names become column names, such as `ColumnNamingPolicy.Custom(name => ...)` |

Plus the [shared sink options](file-connectors.md#options-every-file-source-and-sink-has): `Provider`, `Resolver`,
`BufferSize` and `NullItems`.

## Example: Excel to Parquet

```csharp
public sealed class ExcelToParquetPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            ExcelConnector.Source<Order>(StorageUri.FromFilePath("orders.xlsx"), o => o with { SheetName = "Orders", SkipRows = 2 }),
            "excel-source");

        var sink = builder.AddSink(ParquetConnector.Sink<Order>(StorageUri.FromFilePath("orders.parquet")), "parquet-sink");

        builder.Connect(source, sink);
    }
}
```

## Limitations

- One sheet per source and per sink.
- Formulas are not evaluated; the source reads the values Excel cached when it saved the file.
- The sink writes values, not formulas, and no per-column widths or formats beyond the date formats above.

## Next Steps

- [File Connectors: Shared Behaviour](file-connectors.md): globs, commit behavior, row errors, metrics
- [CSV Connector](csv.md): streaming alternative for tabular data
- [Parquet Connector](parquet.md): columnar format for large datasets
- [Storage Providers](../storage-providers/index.md): read workbooks from cloud storage

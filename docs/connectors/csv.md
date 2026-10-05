---
title: "CSV Connector"
description: "Read and write CSV and TSV files with strict, culture-invariant mapping."
order: 2
---

# CSV Connector

The `NPipeline.Connectors.Csv` package reads and writes CSV and TSV files. It parses with
[CsvHelper](https://joshclose.github.io/CsvHelper/) and maps rows with NPipeline's shared mapping engine: columns bind
to members by name once per file, and values convert strictly and culture-invariantly, so what the sink writes the
source reads back unchanged.

Globs, compression, commit behavior, row errors and metrics work the same in every file connector; see
[File Connectors: Shared Behaviour](file-connectors.md).

## Installation

```bash
dotnet add package NPipeline.Connectors.Csv
```

**Dependencies:** [CsvHelper](https://www.nuget.org/packages/CsvHelper) 33.x, `NPipeline.Connectors`,
`NPipeline.StorageProviders`

## Reading

```csharp
public sealed record Order(int Id, string Customer, decimal Total, DateTimeOffset PlacedAt);

var source = CsvConnector.Source<Order>(StorageUri.FromFilePath("orders.csv"));
builder.AddSource(source, "orders");
```

The header binds to `Order`'s members case-insensitively, so `Id`, `id` and `ID` all map to `Id`. Columns the type does
not have are ignored. A value that does not convert fails the read with a `RecordMappingException` naming the row and
column, unless a `RowErrorHandler` skips or dead-letters it.

To read several files, give a directory or a glob: `StorageUri.Parse("s3://bucket/2026/*.csv")`. Files ending in `.gz`,
`.br` or `.zz` are decompressed.

### Mapping

| Type shape | How it maps |
| --- | --- |
| Class with setters or `init` accessors | Members set by column name |
| Positional record or primary constructor | Constructor parameters matched to columns by name |
| `required` members | Must have a column, or the file fails to bind |
| `[Column("order_id")]` | Sets the column name |
| `[IgnoreColumn]` | Leaves the member out |
| `string`, `int`, `DateTime` and other single values | One value per line, no header |

Members must be single values: numbers, `string`, `bool`, `char`, `decimal`, `DateTime`, `DateTimeOffset`, `DateOnly`,
`TimeOnly`, `TimeSpan`, `Guid`, enums, `byte[]` (base64) and their nullable forms. A member of another type, such as
`List<string>`, fails when the node is created; mark it `[IgnoreColumn]`.

Conversion rules:

- Numbers use `Culture` (invariant by default) and never accept thousands separators, so `1,5` is an error, not `15`.
- Dates are ISO 8601 or `Culture`'s format; a value without an offset is UTC.
- Enums accept names (case-insensitive) or the numbers of defined values.
- An empty field is `null` for a nullable member and an error for any other value type. A row with fewer fields than
  the header reads the missing fields as empty.

### Manual mapping

For full control, pass a mapper. `CsvRow.Get<T>` is strict and names the column when it fails; `TryGet<T>` is lenient.

```csharp
var source = CsvConnector.Source(
    StorageUri.FromFilePath("orders.csv"),
    row => new Order(
        row.Get<int>("id"),
        row.Get<string>("customer"),
        row.TryGet<decimal>("total", out var total) ? total : 0m,
        row.Get<DateTimeOffset>(3)));
```

| `CsvRow` member | Description |
| --- | --- |
| `Get<T>(string name)`, `Get<T>(int index)` | Read and convert a field; throws `FieldMappingException` naming the column |
| `TryGet<T>(string name, out T value)`, `TryGet<T>(int index, out T value)` | Read and convert, returning `false` instead of throwing |
| `this[string name]`, `this[int index]` | The raw text of a field, or `null` |
| `HasColumn(string name)` | Whether the header has the column |
| `Headers`, `FieldCount`, `RawRecord`, `RecordNumber` | The header, this row's field count, its raw text and its position |

The `CsvRow` is reused for every row, so read what you need inside the mapper.

### Read options

| Option | Default | Description |
| --- | --- | --- |
| `Delimiter` | `,` | The field delimiter; `"\t"` for TSV |
| `DetectDelimiter` | `false` | Detect the delimiter from the start of each file instead |
| `Quote` | `"` | The quote character |
| `HasHeader` | `null` | `null`: a header is expected, except for a single-value type read without a mapper. Without a header, columns map to members in declaration order |
| `Culture` | invariant | The culture for numbers and non-ISO dates |
| `Trim` | `None` | CsvHelper's `TrimOptions` for whitespace around fields |
| `Encoding` | `null` | `null` reads UTF-8, or the encoding a byte order mark names |
| `Naming` | `AsIs` | How member names become column names, such as `ColumnNamingPolicy.SnakeCaseLower` |
| `MissingColumns` | `ThrowForRequired` | What a member without a column means; see [column binding](file-connectors.md#column-binding) |
| `ConfigureCsvHelper` | `null` | Adjusts CsvHelper's configuration after these options are applied |

Plus the [shared source options](file-connectors.md#options-every-file-source-and-sink-has): `Provider`, `Resolver`,
`Compression`, `BufferSize`, `Recursive`, `RowErrorHandler` and `RawExcerptLength`.

A quote in the wrong place (text after a closing quote, for example) is a row error for the row it is in, so a handler
can skip it without failing the file.

## Writing

```csharp
var sink = CsvConnector.Sink<Order>(StorageUri.FromFilePath("orders.csv"));
builder.AddSink(sink, "orders-out");
```

The sink writes a header of member names as they are (`PlacedAt`, not `placedat`), then one row per item. Values are
written so the source reads them back unchanged, whatever the machine's culture:

| Type | Written as |
| --- | --- |
| Numbers | Invariant, shortest round-trip form for `double` and `float` |
| `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly` | ISO 8601 round-trip (`2026-01-02T03:04:05.0000000+10:00`) |
| `TimeSpan` | `c` format (`1.02:03:04`) |
| Enums | Names |
| `bool` | `true` or `false` |
| `byte[]` | Base64 |
| `null` | An empty field |

Fields containing the delimiter, a quote, a line break or leading or trailing spaces are quoted. A target ending in `.gz`,
`.br` or `.zz` is compressed. The file appears only when the write is complete; see
[Commit behavior](file-connectors.md#commit-behavior).

A `null` item fails the write by default; set `NullItems = NullItemHandling.Skip` to drop them.

### Manual writing

```csharp
var sink = CsvConnector.Sink<Order>(
    StorageUri.FromFilePath("orders.csv"),
    ["id", "customer", "total"],
    (row, order) =>
    {
        row.Write(order.Id);
        row.WriteText(order.Customer.Trim());
        row.Write(order.Total);
    });
```

`Write<T>` formats a value as the table above describes; `WriteText` writes text as it is.

### Write options

| Option | Default | Description |
| --- | --- | --- |
| `Delimiter` | `,` | The field delimiter |
| `Quote` | `"` | The quote character |
| `HasHeader` | `null` | `null`: records get a header and single-value types do not |
| `Culture` | invariant | The culture for numbers; dates are always ISO 8601 |
| `Encoding` | `null` | `null` writes UTF-8 without a byte order mark |
| `NewLine` | `\n` | The line terminator |
| `Naming` | `AsIs` | How member names become column names |
| `ConfigureCsvHelper` | `null` | Adjusts CsvHelper's configuration after these options are applied |

Plus the [shared sink options](file-connectors.md#options-every-file-source-and-sink-has): `Provider`, `Resolver`,
`Compression`, `BufferSize` and `NullItems`.

## Examples

### TSV with a European number format

```csharp
var source = CsvConnector.Source<Reading>(
    StorageUri.FromFilePath("readings.tsv"),
    o => o with { Delimiter = "\t", Culture = CultureInfo.GetCultureInfo("de-DE") });
```

### Skip bad rows and send them to the dead-letter sink

```csharp
var source = CsvConnector.Source<Order>(uri, o => o with { RowErrorHandler = _ => RowErrorAction.DeadLetter });
builder.AddSource(source, "orders");
builder.AddDeadLetterSink(new MyDeadLetterSink()); // receives ConnectorRecordFailure items
```

### CSV to CSV

```csharp
public sealed class CsvTransformPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(CsvConnector.Source<User>(StorageUri.FromFilePath("users.csv")), "users");
        var transform = builder.AddTransform<Summarizer, User, UserSummary>("summarize");
        var sink = builder.AddSink(CsvConnector.Sink<UserSummary>(StorageUri.FromFilePath("summaries.csv.gz")), "summaries");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

### Reading from S3

```csharp
var source = CsvConnector.Source<Order>(
    StorageUri.Parse("s3://my-bucket/orders/2026/**/*.csv"),
    o => o with { Provider = new AwsS3StorageProvider(s3Options) });
```

## Next Steps

- [File Connectors: Shared Behaviour](file-connectors.md): globs, compression, commit behavior, row errors, metrics
- [JSON Connector](json.md): structured and nested data
- [Parquet Connector](parquet.md): columnar format for large datasets
- [Storage Providers](../storage-providers/index.md): read CSV from S3, Azure Blob, GCS or SFTP

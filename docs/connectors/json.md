---
title: "JSON Connector"
description: "Read and write JSON arrays, NDJSON and nested API exports with System.Text.Json."
order: 3
---

# JSON Connector

The `NPipeline.Connectors.Json` package reads and writes JSON files with `System.Text.Json`. A source reads the
elements of a root array, an array nested inside a root object, or a sequence of top-level values (NDJSON). Each
record is deserialized straight from UTF-8, so anything the serializer supports works: nested objects, lists, enums,
records and source-generated metadata. A record that fails to deserialize is a row error, and the read can skip it
and go on.

Globs, compression, commit behavior, row errors and metrics work the same in every file connector; see
[File Connectors: Shared Behaviour](file-connectors.md).

## Installation

```bash
dotnet add package NPipeline.Connectors.Json
```

**Dependencies:** `System.Text.Json`, `NPipeline.Connectors`, `NPipeline.StorageProviders`

## Reading

```csharp
public sealed record Order(int Id, string Customer, decimal Total, OrderStatus Status, List<OrderLine> Lines);

var source = JsonConnector.Source<Order>(StorageUri.FromFilePath("orders.json"));
builder.AddSource(source, "orders");
```

### What a file can hold

| File content | How it is read | Options |
| --- | --- | --- |
| `[{…}, {…}]` | Each element is a record | Default |
| `{…}\n{…}\n` (NDJSON, JSON Lines) | Each top-level value is a record; a record may span lines | Default |
| `{…}` | One record | Default |
| `{"data": {"items": [{…}, {…}]}, "total": 2}` | Each element of the nested array is a record | `ItemsPath = "data.items"` |

`Format = JsonFormat.Auto` (the default) looks at each file's first character: `[` is an array, anything else is a
sequence of top-level values. Set `Format` to `Array` or `NewlineDelimited` to require one.

`ItemsPath` names properties separated by dots, matched case-insensitively; a leading `$.` is allowed. The source
streams past everything before the array, however large, and fails with the properties it found when the path does
not exist.

### Serialization

By default the source and sink use System.Text.Json's web defaults, with enums as names:

- Property names are camelCase when writing and match case-insensitively when reading.
- Numbers may also be read from strings (`"12"`).
- Enums are written as names and read from names or numbers.
- Dates are ISO 8601.
- `[Column("name")]` and `[IgnoreColumn]` from `NPipeline.Connectors.Attributes` work as they do in the other
  connectors; `[JsonPropertyName]` wins over `[Column]`.

To change anything, pass your own options. The connector copies them and adds the column attributes, so your instance
is not modified:

```csharp
var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };
options.Converters.Add(new JsonStringEnumConverter());

var source = JsonConnector.Source<Order>(uri, o => o with { SerializerOptions = options });
```

For trimming and Native AOT, pass source-generated metadata:

```csharp
[JsonSerializable(typeof(Order))]
public sealed partial class AppJsonContext : JsonSerializerContext;

var source = JsonConnector.Source(uri, AppJsonContext.Default.Order);
var sink = JsonConnector.Sink(outUri, AppJsonContext.Default.Order);
```

### Errors

A record that does not deserialize (a string where a number belongs, for example) fails the read with a
`RecordMappingException`. The exception carries the file, the record's position, the JSON path of the bad value
(`$.total`) and the start of the record. With a `RowErrorHandler`, the record can be skipped or dead-lettered instead,
and the rest of the file is still read, including the rest of an array.

In NDJSON, a malformed line (invalid JSON) is also a row error, and reading goes on from the next line. In an array,
invalid JSON fails the file, because there is no reliable place to resume.

### Manual mapping

A mapper receives a `JsonRow` for each record:

```csharp
var source = JsonConnector.Source(uri, row => new OrderSummary(
    row.Get<int>("id"),
    row.Get<string>("customer.name"),
    row.TryGet<decimal>("total", out var total) ? total : 0m));
```

| `JsonRow` member | Description |
| --- | --- |
| `Get<T>(string name)` | Reads and converts a property; a dotted name reads a nested one. Throws `FieldMappingException` naming it |
| `TryGet<T>(string name, out T value)` | The lenient form |
| `HasProperty(string name)` | Whether the property exists |
| `As<T>()` | The whole record as `T` |
| `Element`, `RecordNumber` | The record as a `JsonElement` (valid during the call), and its position |

### Read options

| Option | Default | Description |
| --- | --- | --- |
| `Format` | `Auto` | `Auto`, `Array` or `NewlineDelimited` |
| `ItemsPath` | `null` | The path to an array inside a root object |
| `SerializerOptions` | `null` | System.Text.Json options; `null` uses the defaults above |

Plus the [shared source options](file-connectors.md#options-every-file-source-and-sink-has): `Provider`, `Resolver`,
`Compression`, `BufferSize`, `Recursive`, `RowErrorHandler` and `RawExcerptLength`. A directory reads its `.json`,
`.ndjson` and `.jsonl` files.

## Writing

```csharp
var sink = JsonConnector.Sink<Order>(StorageUri.FromFilePath("orders.json"));
builder.AddSink(sink, "orders-out");
```

The sink writes an array, or NDJSON when the file ends in `.ndjson` or `.jsonl` (before any `.gz`, `.br` or `.zz`
suffix, which compresses the output). Set `Format` to choose explicitly. Output is written to storage every 64 KB, so
memory usage remains constant regardless of the number of records written.

A `null` item fails the write by default. Set `NullItems = NullItemHandling.Write` to write JSON `null`, or `Skip` to
drop it.

### Write options

| Option | Default | Description |
| --- | --- | --- |
| `Format` | `Auto` | `Auto` (by file name), `Array` or `NewlineDelimited` |
| `WriteIndented` | `false` | Indent array output; NDJSON is never indented |
| `SerializerOptions` | `null` | System.Text.Json options; `null` uses the defaults above |

Plus the [shared sink options](file-connectors.md#options-every-file-source-and-sink-has): `Provider`, `Resolver`,
`Compression`, `BufferSize` and `NullItems`.

## Examples

### An API export with a wrapper object

```csharp
// {"meta": {...}, "data": {"orders": [ ... ]}}
var source = JsonConnector.Source<Order>(StorageUri.Parse("s3://exports/2026-09-28.json"), o => o with
{
    ItemsPath = "data.orders",
    Provider = s3,
});
```

### NDJSON logs, skipping bad lines

```csharp
var source = JsonConnector.Source<LogEvent>(StorageUri.Parse("file:///var/log/app/*.jsonl.gz"), o => o with
{
    RowErrorHandler = error =>
    {
        logger.LogWarning("Skipping line {Line} of {File}", error.RecordNumber, error.Source);
        return RowErrorAction.Skip;
    },
});
```

### JSON to NDJSON

```csharp
public sealed class ConvertPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(JsonConnector.Source<Order>(StorageUri.FromFilePath("orders.json")), "orders");
        var sink = builder.AddSink(JsonConnector.Sink<Order>(StorageUri.FromFilePath("orders.ndjson.gz")), "ndjson");

        builder.Connect(source, sink);
    }
}
```

## Next Steps

- [File Connectors: Shared Behaviour](file-connectors.md): globs, compression, commit behavior, row errors, metrics
- [CSV Connector](csv.md): tabular data
- [HTTP Connector](http.md): read JSON straight from REST APIs
- [Storage Providers](../storage-providers/index.md): read JSON from S3, Azure Blob, GCS or SFTP

---
title: "JSON Connector"
description: "Read and write JSON files with attribute-based or lambda-based mapping, supporting JSON arrays and NDJSON."
order: 3
---

# JSON Connector

The `NPipeline.Connectors.Json` package reads and writes JSON files using `System.Text.Json`. It supports JSON arrays and newline-delimited JSON (NDJSON), attribute-based mapping, explicit row mappers, configurable naming policies, and per-row error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.Json
```

**Dependencies:** `System.Text.Json`, `NPipeline.Connectors`, `NPipeline.StorageProviders`

## Storage Abstraction

The JSON connector uses NPipeline's storage abstraction layer. See the [CSV Connector — Storage Abstraction](csv.md#storage-abstraction) section for full details on `StorageUri`, `IStorageResolver`, and when you need an explicit resolver.

**Short version:** omit the resolver for local files; provide one for cloud storage.

```csharp
// Local file (no resolver needed)
var source = new JsonSourceNode<Order>(StorageUri.FromFilePath("orders.json"));

// Cloud storage (explicit resolver)
var source = new JsonSourceNode<Order>(
    StorageUri.Parse("s3://bucket/orders.json"),
    resolver: myResolver);

// Explicit provider (bypass resolution)
var source = new JsonSourceNode<Order>(myProvider, StorageUri.FromFilePath("orders.json"));
```

## Column Mapping

### Attribute-Based Mapping

Properties map to JSON fields using these attributes (checked in priority order):

1. `[Column("name")]` from `NPipeline.Connectors.Attributes` (highest priority)
2. `[JsonPropertyName("name")]` from `System.Text.Json.Serialization`
3. The `PropertyNamingPolicy` applied to the property name (default: lowercase)

Properties are excluded with `[IgnoreColumn]`, `[Column(Ignore = true)]`, or `[JsonIgnore]`.

```csharp
using NPipeline.Connectors.Attributes;

public class Order
{
    [Column("order_id")]
    public int Id { get; set; }

    public string CustomerName { get; set; } = string.Empty; // maps to "customername" (lowercase policy)

    [IgnoreColumn]
    public string InternalNote { get; set; } = string.Empty;
}
```

### Lambda-Based Mapping

Provide a `Func<JsonRow, T>` for explicit control:

```csharp
var source = new JsonSourceNode<Order>(
    StorageUri.FromFilePath("orders.json"),
    row => new Order
    {
        Id = row.Get<int>("order_id") ?? 0,
        CustomerName = row.Get<string>("customer_name") ?? string.Empty
    });
```

`JsonRow` methods:

| Method | Description |
|--------|-------------|
| `Get<T>(string name, T? defaultValue)` | Read a field by name, return converted value or default |
| `TryGet<T>(string name, out T? value, T? defaultValue)` | Try to read and convert; returns `false` if missing |
| `HasProperty(string name)` | Check whether a property exists |
| `GetElement(string name)` | Get the raw `JsonElement` for complex/nested access |

## Source Node — `JsonSourceNode<T>`

Reads a JSON file and emits each object as an item of type `T`.

### Constructors

```csharp
// Attribute-based mapping with optional resolver
public JsonSourceNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    JsonConfiguration? configuration = null)

// Attribute-based mapping with explicit provider
public JsonSourceNode(
    IStorageProvider provider,
    StorageUri uri,
    JsonConfiguration? configuration = null)

// Lambda-based mapping with optional resolver
public JsonSourceNode(
    StorageUri uri,
    Func<JsonRow, T> rowMapper,
    IStorageResolver? resolver = null,
    JsonConfiguration? configuration = null)

// Lambda-based mapping with explicit provider
public JsonSourceNode(
    IStorageProvider provider,
    StorageUri uri,
    Func<JsonRow, T> rowMapper,
    JsonConfiguration? configuration = null)
```

### Example

```csharp
// JSON array: [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
var source = new JsonSourceNode<User>(StorageUri.FromFilePath("users.json"));

// NDJSON: {"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}
var config = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
var source = new JsonSourceNode<User>(
    StorageUri.FromFilePath("users.ndjson"),
    configuration: config);
```

## Sink Node — `JsonSinkNode<T>`

Writes items to a JSON file using attribute-based mapping.

### Constructors

```csharp
// Attribute-based mapping with optional resolver
public JsonSinkNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    JsonConfiguration? configuration = null)

// Attribute-based mapping with explicit provider
public JsonSinkNode(
    IStorageProvider provider,
    StorageUri uri,
    JsonConfiguration? configuration = null)
```

### Example

```csharp
var config = new JsonConfiguration { WriteIndented = true };
var sink = new JsonSinkNode<UserSummary>(
    StorageUri.FromFilePath("output.json"),
    configuration: config);
```

## Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BufferSize` | `int` | `4096` | Stream buffer size in bytes |
| `Format` | `JsonFormat` | `Array` | `Array` for `[...]` or `NewlineDelimited` for NDJSON |
| `WriteIndented` | `bool` | `false` | Pretty-print JSON output |
| `PropertyNameCaseInsensitive` | `bool` | `true` | Case-insensitive property matching when reading |
| `PropertyNamingPolicy` | `JsonPropertyNamingPolicy` | `LowerCase` | Naming convention for serialization |
| `RowErrorHandler` | `Func<Exception, JsonRow, bool>?` | `null` | Per-row error handler. Return `true` to skip, `false` to throw. |

### Naming Policies

| Policy | Example: `FirstName` → |
|--------|----------------------|
| `LowerCase` (default) | `firstname` |
| `CamelCase` | `firstName` |
| `SnakeCase` | `first_name` |
| `PascalCase` | `FirstName` |
| `AsIs` | `FirstName` (no transformation) |

### Per-Row Error Handling

```csharp
var config = new JsonConfiguration
{
    RowErrorHandler = (ex, row) =>
    {
        Console.WriteLine($"Skipping bad row: {ex.Message}");
        return true; // skip and continue
    }
};
```

## Example: Transform Pipeline (JSON → JSON)

```csharp
public sealed class JsonTransformPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            new JsonSourceNode<Order>(StorageUri.FromFilePath("orders.json")),
            "json-source");

        var transform = builder.AddTransform<EnrichOrder, Order, EnrichedOrder>("enrich");

        var config = new JsonConfiguration
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonPropertyNamingPolicy.CamelCase
        };
        var sink = builder.AddSink(
            new JsonSinkNode<EnrichedOrder>(
                StorageUri.FromFilePath("enriched-orders.json"),
                configuration: config),
            "json-sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

## Next Steps

- [CSV Connector](csv.md) — similar file-based connector for tabular data
- [Parquet Connector](parquet.md) — columnar format for large datasets
- [Storage Providers](../storage-providers/index.md) — read JSON from S3, Azure Blob, or GCS

## Storage Abstraction

All file connectors use `StorageUri` + `IStorageResolver` for pluggable storage:

```csharp
// Local file
var source = new JsonSourceNode<Order>(StorageUri.FromFilePath("orders.json"));

// Cloud storage
var source = new JsonSourceNode<Order>(
    StorageUri.Parse("s3://my-bucket/data/orders.json"),
    resolver: myStorageResolver);
```

See [Storage Providers](../storage-providers/index.md) for configuring S3, Azure Blob, GCS, and ADLS Gen2.

## JSON Formats

### Array Format (default)

Standard JSON array — entire file is `[{...}, {...}, ...]`:

```json
[
  { "id": 1, "name": "Alice" },
  { "id": 2, "name": "Bob" }
]
```

### Newline-Delimited JSON (NDJSON)

One JSON object per line — ideal for streaming and append-only:

```json
{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
```

```csharp
var source = new JsonSourceNode<Order>(
    StorageUri.FromFilePath("events.ndjson"),
    configuration: new JsonConfiguration { Format = JsonFormat.NewlineDelimited });
```

NDJSON is more memory-efficient for large files since each line is parsed independently.

## Nested Properties

Use `ItemsJsonPath` (dot-separated) to extract items from a nested JSON structure:

```csharp
// JSON: { "response": { "data": { "orders": [...] } } }
var source = new JsonSourceNode<Order>(
    StorageUri.FromFilePath("response.json"),
    configuration: new JsonConfiguration { ItemsJsonPath = "response.data.orders" });
```

## Best Practices

1. **Use NDJSON** for large files and streaming — lower memory footprint
2. **Use `ItemsJsonPath`** to extract nested arrays without pre-processing
3. **Increase `BufferSize`** for large files (default 4096)
4. **Use `RowErrorHandler`** to skip malformed records
5. **Set `PropertyNamingPolicy`** to match the source API convention
6. **Use `StorageUri`** with `IStorageResolver` for cloud storage portability

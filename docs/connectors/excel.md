---
title: "Excel Connector"
description: "Read and write Excel files (XLS/XLSX) with header detection, sheet selection, and type conversion."
order: 5
---

# Excel Connector

The `NPipeline.Connectors.Excel` package reads and writes Excel workbooks. It reads both legacy XLS (binary) and modern XLSX (Open XML) formats, and writes XLSX. Supports sheet selection, header detection, attribute-based column mapping, and configurable type analysis.

## Installation

```bash
dotnet add package NPipeline.Connectors.Excel
```

**Dependencies:** [ExcelDataReader](https://www.nuget.org/packages/ExcelDataReader) 3.x, [DocumentFormat.OpenXml](https://www.nuget.org/packages/DocumentFormat.OpenXml) 3.x, `NPipeline.Connectors`, `NPipeline.StorageProviders`

## Storage Abstraction

The Excel connector uses NPipeline's storage abstraction layer. See the [CSV Connector — Storage Abstraction](csv.md#storage-abstraction) section for full details on `StorageUri`, `IStorageResolver`, and when you need an explicit resolver.

```csharp
// Local file (no resolver needed)
var source = new ExcelSourceNode<Order>(StorageUri.FromFilePath("orders.xlsx"));

// Cloud storage (explicit resolver)
var source = new ExcelSourceNode<Order>(
    StorageUri.Parse("s3://bucket/orders.xlsx"),
    resolver: myResolver);
```

## Column Mapping

Use `[Column]` and `[IgnoreColumn]` from `NPipeline.Connectors.Attributes` to control property-to-column mapping:

```csharp
using NPipeline.Connectors.Attributes;

public class Product
{
    [Column("product_id")]
    public int Id { get; set; }

    [Column("product_name")]
    public string Name { get; set; } = string.Empty;

    public decimal Price { get; set; }

    [IgnoreColumn]
    public string DisplayLabel => $"{Name} (${Price:F2})";
}
```

When no `[Column]` attribute is present, properties are matched by name (case-insensitive).

### Lambda-Based Mapping

```csharp
var source = new ExcelSourceNode<Product>(
    StorageUri.FromFilePath("products.xlsx"),
    row => new Product
    {
        Id = row.Get<int>("product_id") ?? 0,
        Name = row.Get<string>("product_name") ?? string.Empty,
        Price = row.Get<decimal>("price") ?? 0m
    });
```

## Source Node — `ExcelSourceNode<T>`

Reads an Excel file and emits each row as an item of type `T`.

> ⚠️ **Note:** Excel files are fully materialized in memory during read (ExcelDataReader requirement). For datasets larger than a few hundred MB, consider converting to CSV or Parquet first.

### Constructors

```csharp
// Attribute-based mapping with optional resolver
public ExcelSourceNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    ExcelConfiguration? configuration = null)

// Lambda-based mapping with optional resolver
public ExcelSourceNode(
    StorageUri uri,
    Func<ExcelRow, T> rowMapper,
    IStorageResolver? resolver = null,
    ExcelConfiguration? configuration = null)

// Attribute-based mapping with explicit provider
public ExcelSourceNode(
    IStorageProvider provider,
    StorageUri uri,
    ExcelConfiguration? configuration = null)

// Lambda-based mapping with explicit provider
public ExcelSourceNode(
    IStorageProvider provider,
    StorageUri uri,
    Func<ExcelRow, T> rowMapper,
    ExcelConfiguration? configuration = null)
```

### Example: Reading a Specific Sheet

```csharp
var config = new ExcelConfiguration { SheetName = "Q1 Orders" };

var source = new ExcelSourceNode<Order>(
    StorageUri.FromFilePath("report.xlsx"),
    configuration: config);
```

## Sink Node — `ExcelSinkNode<T>`

Writes items to an XLSX file using attribute-based mapping.

### Constructors

```csharp
// Attribute-based mapping with optional resolver
public ExcelSinkNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    ExcelConfiguration? configuration = null)

// Attribute-based mapping with explicit provider
public ExcelSinkNode(
    IStorageProvider provider,
    StorageUri uri,
    ExcelConfiguration? configuration = null)
```

### Example

```csharp
var sink = new ExcelSinkNode<OrderSummary>(
    StorageUri.FromFilePath("summary.xlsx"),
    configuration: new ExcelConfiguration { SheetName = "Summary" });
```

## Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BufferSize` | `int` | `4096` | Stream buffer size in bytes |
| `SheetName` | `string?` | `null` | Sheet to read/write. `null` reads the first sheet; writes to "Sheet1". |
| `FirstRowIsHeader` | `bool` | `true` | Treat the first row as column headers |
| `HasHeaderRow` | `bool` | `true` | Alias for `FirstRowIsHeader` |
| `Encoding` | `Encoding?` | `null` | Override text encoding for legacy XLS files |
| `AutodetectSeparators` | `bool` | `true` | Auto-detect separators in CSV-like data |
| `AnalyzeAllColumns` | `bool` | `false` | Analyze the entire workbook for type detection (slower, more accurate) |
| `AnalyzeInitialRowCount` | `int` | `30` | Rows to analyze for type detection when `AnalyzeAllColumns` is `false` |

### Type Detection

ExcelDataReader infers .NET types from cell values. By default, it analyzes the first 30 rows (`AnalyzeInitialRowCount`). If your data has mixed types further down:

```csharp
// Accurate but slower: analyze all rows
var config = new ExcelConfiguration { AnalyzeAllColumns = true };

// Compromise: analyze more rows
var config = new ExcelConfiguration { AnalyzeInitialRowCount = 500 };
```

### Legacy XLS Encoding

If a legacy `.xls` file uses non-UTF-8 text:

```csharp
var config = new ExcelConfiguration
{
    Encoding = Encoding.GetEncoding("windows-1252")
};
```

## Example: Full Pipeline (Excel → Parquet)

```csharp
public sealed class ExcelToParquetPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            new ExcelSourceNode<Order>(
                StorageUri.FromFilePath("orders.xlsx"),
                configuration: new ExcelConfiguration { SheetName = "Orders" }),
            "excel-source");

        var sink = builder.AddSink(
            new ParquetSinkNode<Order>(StorageUri.FromFilePath("orders.parquet")),
            "parquet-sink");

        builder.Connect(source, sink);
    }
}
```

## Next Steps

- [CSV Connector](csv.md) — streaming alternative for tabular data
- [Parquet Connector](parquet.md) — efficient columnar format for large datasets
- [Storage Providers](../storage-providers/index.md) — read Excel from cloud storage

## Storage Abstraction

All file connectors use `StorageUri` + `IStorageResolver`:

```csharp
// Local file
var source = new ExcelSourceNode<Order>(StorageUri.FromFilePath("orders.xlsx"));

// Cloud storage
var source = new ExcelSourceNode<Order>(
    StorageUri.Parse("az://container/data/orders.xlsx"),
    resolver: myStorageResolver);
```

## Format Support

| Format | Extension | Library | Notes |
|--------|-----------|---------|-------|
| XLSX (Open XML) | `.xlsx` | ExcelDataReader | Full support — recommended |
| XLS (BIFF) | `.xls` | ExcelDataReader | Legacy — may need encoding config |

The connector auto-detects the format from the file content.

## Multi-Sheet Scenarios

```csharp
// Read a specific sheet by name
var source = new ExcelSourceNode<Order>(
    StorageUri.FromFilePath("workbook.xlsx"),
    configuration: new ExcelConfiguration { SheetName = "Q4 Orders" });

// Write to a named sheet
var sink = new ExcelSinkNode<Summary>(
    StorageUri.FromFilePath("report.xlsx"),
    configuration: new ExcelConfiguration { SheetName = "Summary" });
```

When `SheetName` is `null`, the source reads the first sheet and the sink writes to "Sheet1".

## Limitations

- **Read-only streaming**: Excel files are fully loaded into memory (ExcelDataReader reads the entire stream)
- **No formula evaluation**: Formulas are not evaluated — only cached values are read
- **No cell formatting**: Styles, colors, and formatting are not preserved through the sink
- **Type detection**: Based on cell value sampling — mixed-type columns may produce unexpected results

## Best Practices

1. **Use XLSX** format — XLS is legacy and has row limits (65,536)
2. **Set `AnalyzeInitialRowCount`** appropriately for mixed-type columns
3. **Specify `SheetName`** explicitly in multi-sheet workbooks
4. **Configure `Encoding`** for legacy XLS files with non-UTF-8 text
5. **Prefer CSV or Parquet** for large datasets — Excel has a 1,048,576 row limit

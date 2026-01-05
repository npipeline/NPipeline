---
title: Excel Connector
description: Read from and write to Excel files (XLS and XLSX) with NPipeline using the Excel connector.
sidebar_position: 2
---

# Excel Connector

The `NPipeline.Connectors.Excel` package provides specialized source and sink nodes for working with Excel files. This allows you to easily integrate Excel data into your pipelines as an input source or an output destination.

This connector uses the [ExcelDataReader](https://github.com/ExcelDataReader/ExcelDataReader) library for reading both legacy XLS (binary) and modern XLSX (Open XML) formats, and the [DocumentFormat.OpenXml](https://github.com/dotnet/Open-XML-SDK) library for writing XLSX files.

## Installation

To use the Excel connector, install the `NPipeline.Connectors.Excel` NuGet package:

```bash
dotnet add package NPipeline.Connectors.Excel
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Storage Abstraction Layer

The Excel connector uses NPipeline's storage abstraction layer, which provides a unified way to work with different storage systems. This layer allows you to work with local files, cloud storage (like S3 or Azure Blob), and other storage systems using the same API.

### StorageUri

The `StorageUri` class represents a normalized storage location URI. It supports both absolute URIs (e.g., "s3://bucket/key") and local file paths. For local files, use the `StorageUri.FromFilePath()` method:

```csharp
// For local files
var localFileUri = StorageUri.FromFilePath("data/input.xlsx");

// For absolute URIs (e.g., cloud storage)
var cloudUri = StorageUri.Parse("s3://my-bucket/path/to/file.xlsx");
```

### IStorageResolver

The `IStorageResolver` interface is responsible for discovering and resolving storage providers capable of handling a given `StorageUri`. You must provide a resolver to both `ExcelSourceNode` and `ExcelSinkNode`.

To create a resolver for standard file system operations, use:

```csharp
var resolver = StorageProviderFactory.CreateResolver().Resolver;
```

You may need a custom resolver when:

- Working with cloud storage systems (S3, Azure, etc.)
- Using custom storage providers
- Needing to override default provider selection

## `ExcelSourceNode<T>`

The `ExcelSourceNode<T>` reads data from an Excel file and emits each row as an item of type `T`.

### Configuration

The constructor for `ExcelSourceNode<T>` takes the file path, a storage resolver, and optional configuration for parsing the Excel file.

```csharp
public ExcelSourceNode(
    StorageUri uri,
    IStorageResolver resolver,
    ExcelConfiguration? configuration = null)
```

- **`uri`**: The `StorageUri` representing the location of the Excel file. Use `StorageUri.FromFilePath("path/to/file.xlsx")` for local files.
- **`resolver`**: The `IStorageResolver` to resolve storage providers. Create one using `StorageProviderFactory.CreateResolver().Resolver` for standard file system support.
- **`configuration`**: An optional `ExcelConfiguration` object to customize parsing (e.g., sheet selection, header handling, encoding).

### Example: Reading an Excel File

Let's assume you have a `products.xlsx` file with the following structure:

| Id | Name | Price | Category |
|----|------|-------|----------|
| 1 | Widget | 19.99 | Electronics |
| 2 | Gadget | 29.99 | Electronics |
| 3 | Tool | 9.99 | Hardware |

And a corresponding C# record:

```csharp
public sealed record Product(int Id, string Name, decimal Price, string Category);
```

You can read this data into your pipeline as follows:

```csharp
using NPipeline;
using NPipeline.Connectors;
using NPipeline.Connectors.Excel;
using NPipeline.DataFlow.DataPipes;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tracing;

public sealed record Product(int Id, string Name, decimal Price, string Category);

public sealed class ExcelReaderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var resolver = StorageProviderFactory.CreateResolver().Resolver;
        var source = builder.AddSource("excel_source", new ExcelSourceNode<Product>(StorageUri.FromFilePath("products.xlsx"), resolver));
        var sink = builder.AddSink<ConsoleSinkNode, Product>("console_sink");

        builder.Connect(source, sink);
    }
}

public sealed class ConsoleSinkNode : SinkNode<Product>
{
    public override async Task ExecuteAsync(
        IDataPipe<Product> input,
        PipelineContext context,
        IPipelineActivity parentActivity,
        CancellationToken cancellationToken)
    {
        await foreach (var product in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Received: {product}");
        }
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        // Create and run the pipeline
        var runner = PipelineRunner.Create();
        await runner.RunAsync<ExcelReaderPipeline>();

        Console.WriteLine("Excel reading completed");
    }
}
```

**Expected Output:**

```text
Received: Product { Id = 1, Name = Widget, Price = 19.99, Category = Electronics }
Received: Product { Id = 2, Name = Gadget, Price = 29.99, Category = Electronics }
Received: Product { Id = 3, Name = Tool, Price = 9.99, Category = Hardware }
Excel reading completed
```

## `ExcelSinkNode<T>`

The `ExcelSinkNode<T>` writes items from the pipeline to an Excel file in XLSX format.

### Configuration

The constructor for `ExcelSinkNode<T>` takes the file path, a storage resolver, and optional configuration for writing the Excel file.

```csharp
public ExcelSinkNode(
    StorageUri uri,
    IStorageResolver resolver,
    ExcelConfiguration? configuration = null)
```

- **`uri`**: The `StorageUri` representing the location of the output Excel file. Use `StorageUri.FromFilePath("path/to/file.xlsx")` for local files.
- **`resolver`**: The `IStorageResolver` to resolve storage providers. Create one using `StorageProviderFactory.CreateResolver().Resolver` for standard file system support.
- **`configuration`**: An optional `ExcelConfiguration` object to customize writing (e.g., sheet name, header handling).

### Example: Writing to an Excel File

Let's take processed product data and write it to an `output.xlsx` file.

```csharp
using NPipeline.Connectors;
using NPipeline.Connectors.Excel;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record ProcessedProduct(int Id, string FullName, decimal AdjustedPrice, string Status);

public sealed class ExcelWriterPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var resolver = StorageProviderFactory.CreateResolver().Resolver;
        var source = builder.AddSource<InMemorySourceNode<ProcessedProduct>, ProcessedProduct>("source");
        var sink = builder.AddSink("excel_sink", new ExcelSinkNode<ProcessedProduct>(StorageUri.FromFilePath("output.xlsx"), resolver));

        builder.Connect(source, sink);
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var products = new List<ProcessedProduct>
        {
            new(1, "Widget Pro", 21.99m, "In Stock"),
            new(2, "Gadget Ultra", 32.99m, "Low Stock"),
            new(3, "Tool Basic", 10.99m, "Out of Stock")
        };

        // Set up test data
        var context = PipelineContext.Default;
        context.Items[typeof(InMemorySourceNode<ProcessedProduct>).FullName!] = products.ToArray();

        var runner = PipelineRunner.Create();
        await runner.RunAsync<ExcelWriterPipeline>(context);

        Console.WriteLine("\nExcel file created: output.xlsx");
    }
}
```

**Expected `output.xlsx` Content:**

| Id | FullName | AdjustedPrice | Status |
|----|----------|---------------|--------|
| 1 | Widget Pro | 21.99 | In Stock |
| 2 | Gadget Ultra | 32.99 | Low Stock |
| 3 | Tool Basic | 10.99 | Out of Stock |

## Configuration Reference

### ExcelConfiguration

The `ExcelConfiguration` class provides comprehensive options for configuring Excel read and write operations.

#### Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BufferSize` | `int` | `4096` | Buffer size for stream operations in bytes. Larger buffers improve I/O performance but use more memory. |
| `SheetName` | `string?` | `null` | Name of the sheet to read from or write to. When `null`, uses the first sheet for reading or creates a default sheet ("Sheet1") for writing. |
| `FirstRowIsHeader` | `bool` | `true` | Indicates whether the first row contains column headers. When `true`, the first row is used for property mapping. |
| `HasHeaderRow` | `bool` | `true` | Convenience property that syncs with `FirstRowIsHeader`. |
| `Encoding` | `Encoding?` | `null` | Encoding for reading legacy XLS files with text data. When `null`, ExcelDataReader auto-detects encoding. |
| `AutodetectSeparators` | `bool` | `true` | Indicates whether to automatically detect separators in CSV-like data within Excel cells. |
| `AnalyzeAllColumns` | `bool` | `false` | Indicates whether to analyze the entire workbook to determine data types. Provides more accurate detection but is slower. |
| `AnalyzeInitialRowCount` | `int` | `30` | Number of rows to analyze for data type detection when `AnalyzeAllColumns` is `false`. |

## Advanced Configuration

### Buffer Size Configuration

The [`BufferSize`](../../../src/NPipeline.Connectors.Excel/ExcelConfiguration.cs:42) property controls the internal buffer size for Excel I/O operations:

- **Default value**: 4096 bytes (4KB)
- **Purpose**: Determines the size of the buffer used for stream operations when reading or writing Excel files
- **Performance impact**: Larger buffers can improve I/O performance for large files but use more memory

When to adjust BufferSize:
- **Increase** (e.g., 8192, 16384) for:
  - Processing very large Excel files
  - High-throughput scenarios where I/O performance is critical
  - Systems with abundant memory resources
- **Decrease** (e.g., 2048, 1024) for:
  - Memory-constrained environments
  - Processing many small Excel files concurrently
  - Scenarios where memory usage must be tightly controlled

```csharp
// Example: Custom buffer size for large file processing
var largeFileConfig = new ExcelConfiguration
{
    BufferSize = 8192, // 8KB buffer for better performance with large files
    SheetName = "LargeDataset",
    FirstRowIsHeader = true
};

var resolver = StorageProviderFactory.CreateResolver().Resolver;
var source = new ExcelSourceNode<Product>(StorageUri.FromFilePath("large_dataset.xlsx"), resolver, largeFileConfig);
```

### Sheet Selection

You can specify which sheet to read from or write to using the `SheetName` property:

```csharp
// Read from a specific sheet
var readConfig = new ExcelConfiguration
{
    SheetName = "Q4Sales",
    FirstRowIsHeader = true
};

var resolver = StorageProviderFactory.CreateResolver().Resolver;
var source = new ExcelSourceNode<SalesRecord>(StorageUri.FromFilePath("sales_report.xlsx"), resolver, readConfig);
```

```csharp
// Write to a specific sheet
var writeConfig = new ExcelConfiguration
{
    SheetName = "ProcessedData",
    FirstRowIsHeader = true
};

var resolver = StorageProviderFactory.CreateResolver().Resolver;
var sink = new ExcelSinkNode<ProcessedRecord>(StorageUri.FromFilePath("output.xlsx"), resolver, writeConfig);
```

### Type Detection Configuration

ExcelDataReader provides two modes for data type detection:

#### Fast Mode (Default)

Analyzes only the first N rows to determine data types. Faster but may be less accurate if data types vary throughout the column.

```csharp
var fastConfig = new ExcelConfiguration
{
    AnalyzeAllColumns = false,
    AnalyzeInitialRowCount = 50, // Analyze first 50 rows
    FirstRowIsHeader = true
};
```

#### Accurate Mode

Analyzes all rows in each column to determine the most appropriate data type. Slower but more accurate.

```csharp
var accurateConfig = new ExcelConfiguration
{
    AnalyzeAllColumns = true,
    FirstRowIsHeader = true
};
```

### Encoding Configuration

For legacy XLS files with text data, you may need to specify an explicit encoding:

```csharp
using System.Text;

var encodingConfig = new ExcelConfiguration
{
    Encoding = Encoding.GetEncoding("Windows-1252"), // Western European encoding
    FirstRowIsHeader = true
};

var resolver = StorageProviderFactory.CreateResolver().Resolver;
var source = new ExcelSourceNode<LegacyRecord>(StorageUri.FromFilePath("legacy_data.xls"), resolver, encodingConfig);
```

### Example: Transforming and Writing to Excel

This pipeline transforms product data and writes the result to a new Excel file.

```csharp
using NPipeline.Connectors.Excel;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record ProductSummary(string Name, string Category, string PriceRange);

public sealed class ProductSummarizer : TransformNode<Product, ProductSummary>
{
    public override Task<ProductSummary> ExecuteAsync(
        Product item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var priceRange = item.Price switch
        {
            < 10 => "Low",
            < 20 => "Medium",
            _ => "High"
        };
        return Task.FromResult(new ProductSummary(item.Name, item.Category, priceRange));
    }
}

public sealed class ExcelTransformPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var resolver = StorageProviderFactory.CreateResolver().Resolver;
        var source = builder.AddSource("excel_source", new ExcelSourceNode<Product>(StorageUri.FromFilePath("products.xlsx"), resolver));
        var transform = builder.AddTransform<ProductSummarizer, Product, ProductSummary>("summarizer");
        var sink = builder.AddSink("excel_sink", new ExcelSinkNode<ProductSummary>(StorageUri.FromFilePath("summaries.xlsx"), resolver));

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        var runner = PipelineRunner.Create();
        await runner.RunAsync<ExcelTransformPipeline>();
    }
}
```

After running, this will create a `summaries.xlsx` file with the following content:

| Name | Category | PriceRange |
|------|----------|------------|
| Widget | Electronics | Medium |
| Gadget | Electronics | High |
| Tool | Hardware | Low |

## Supported Data Types

The Excel connector supports automatic type conversion for the following .NET types:

### Primitive Types
- `int`, `long`, `short` (and nullable variants)
- `float`, `double`, `decimal` (and nullable variants)
- `bool` (and nullable variant)

### String Types
- `string`

### Date/Time Types
- `DateTime` (and nullable variant)

### GUID Types
- `Guid` (and nullable variant)

### Type Conversion Behavior

- **Reading**: ExcelDataReader attempts to convert cell values to the target type automatically. If conversion fails, the property is skipped.
- **Writing**: Values are written with appropriate Excel cell types (Number, String, Boolean, DateTime). Complex types default to string representation.

## Format Support

### Reading

| Format | Extension | Library | Notes |
|--------|-----------|---------|-------|
| Legacy Excel | `.xls` | ExcelDataReader | Binary format, supports encoding configuration |
| Modern Excel | `.xlsx` | ExcelDataReader | Open XML format, UTF-8 encoding |

### Writing

| Format | Extension | Library | Notes |
|--------|-----------|---------|-------|
| Modern Excel | `.xlsx` | DocumentFormat.OpenXml | Open XML format only |
| Legacy Excel | `.xls` | Not supported | Use XLSX format instead |

**Important**: The sink node only supports writing XLSX format. If you need to write legacy XLS files, convert them to XLSX first.

## Performance Considerations

### Reading Performance

The `ExcelSourceNode<T>` uses streaming access for memory-efficient processing of large Excel files:

- **Streaming**: Data is read row-by-row, minimizing memory usage
- **Buffer Size**: Configure [`BufferSize`](../../../src/NPipeline.Connectors.Excel/ExcelConfiguration.cs:42) to optimize I/O performance
- **Type Detection**: Use `AnalyzeAllColumns = false` with appropriate `AnalyzeInitialRowCount` for better performance with large files
- **Sheet Selection**: Specify `SheetName` explicitly to avoid unnecessary sheet traversal

### Writing Performance

The `ExcelSinkNode<T>` collects all items in memory before writing due to XLSX format requirements:

- **Memory Usage**: All items are collected in a list before writing to the Excel file
- **Buffer Size**: Configure [`BufferSize`](../../../src/NPipeline.Connectors.Excel/ExcelConfiguration.cs:42) to optimize I/O performance
- **Batching**: Consider batching large datasets to manage memory usage
- **Sheet Creation**: Only creates one sheet per write operation

### Performance Optimization Tips

1. **Use appropriate buffer sizes**: Increase `BufferSize` for large files, decrease for memory-constrained environments
2. **Optimize type detection**: Use `AnalyzeAllColumns = false` with `AnalyzeInitialRowCount` set appropriately for your data
3. **Specify sheet names**: Avoid unnecessary sheet traversal by specifying `SheetName`
4. **Batch large writes**: For very large datasets, consider splitting into multiple files or batches
5. **Use streaming for reading**: Leverage the streaming capability of `ExcelSourceNode<T>` for large files

## Limitations

### Format Limitations

- **XLS writing not supported**: Legacy XLS (binary) format is not supported for writing. Use XLSX format instead.
- **Single sheet per write**: Each `ExcelSinkNode<T>` operation creates a single sheet. Multi-sheet workbooks require multiple write operations.

### Memory Limitations

- **Writing requires all data in memory**: The sink node collects all items before writing, which may be a concern for very large datasets
- **No streaming write**: XLSX format requires all data to be available before writing the file structure

### Type Mapping Limitations

- **Header-less mapping**: When `FirstRowIsHeader = false`, properties are mapped by column index using a hash-based approach, which may not be deterministic for all scenarios
- **Complex type conversion**: Some complex type conversions may fail silently. Ensure your data types are compatible
- **Nullable handling**: Null values are handled gracefully but may result in empty cells in the output

### Error Handling

- **Conversion errors**: Type conversion errors during reading are silently skipped, and the property is not populated
- **Missing sheets**: If a specified sheet is not found, an `InvalidOperationException` is thrown
- **Missing files**: File not found errors are propagated through the storage provider

## Best Practices

### File Format

1. **Use XLSX format for new files**: XLSX is the modern standard and supports both reading and writing
2. **Convert legacy XLS to XLSX**: If you need to write legacy XLS files, convert them to XLSX first
3. **Specify file extensions**: Always include the file extension (`.xlsx` or `.xls`) in your `StorageUri`

### Configuration

1. **Specify sheet names explicitly**: This improves code clarity and prevents errors when working with multi-sheet workbooks
2. **Enable headers for structured data**: Set `FirstRowIsHeader = true` for better property mapping
3. **Adjust buffer size for large files**: Increase `BufferSize` for better I/O performance with large files
4. **Configure type detection**: Choose between fast and accurate type detection based on your data characteristics

### Data Modeling

1. **Validate data types**: Ensure your model properties match the data types in your Excel files
2. **Use appropriate nullable types**: Handle missing or null data with nullable properties
3. **Keep property names simple**: Use property names that match Excel column headers when using header mapping
4. **Consider string conversion**: For complex types, consider converting to strings in your model

### Error Handling

1. **Handle missing sheets**: Wrap source node creation in try-catch to handle missing sheet exceptions
2. **Validate data before writing**: Ensure data is valid and complete before passing to the sink node
3. **Monitor memory usage**: Be aware of memory consumption when writing large datasets
4. **Log conversion errors**: Consider implementing logging to track type conversion issues

### Performance

1. **Use streaming for reading**: Leverage the streaming capability of `ExcelSourceNode<T>` for large files
2. **Batch large datasets**: Split very large datasets into smaller batches to manage memory usage
3. **Optimize buffer sizes**: Tune `BufferSize` based on your file sizes and system resources
4. **Profile your workload**: Test with representative data to identify performance bottlenecks

## Advanced Scenarios

### Reading Multiple Sheets

To read from multiple sheets in a workbook, create multiple source nodes with different sheet configurations:

```csharp
var resolver = StorageProviderFactory.CreateResolver().Resolver;

var q1Source = new ExcelSourceNode<SalesRecord>(
    StorageUri.FromFilePath("sales.xlsx"),
    resolver,
    new ExcelConfiguration { SheetName = "Q1", FirstRowIsHeader = true }
);

var q2Source = new ExcelSourceNode<SalesRecord>(
    StorageUri.FromFilePath("sales.xlsx"),
    resolver,
    new ExcelConfiguration { SheetName = "Q2", FirstRowIsHeader = true }
);

// Add both sources to your pipeline
var q1Node = builder.AddSource("q1_source", q1Source);
var q2Node = builder.AddSource("q2_source", q2Source);
```

### Round-Trip Processing

Read from an Excel file, process the data, and write back to a new Excel file:

```csharp
public sealed class RoundTripPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var resolver = StorageProviderFactory.CreateResolver().Resolver;

        // Read from input file
        var source = builder.AddSource(
            "excel_source",
            new ExcelSourceNode<Product>(
                StorageUri.FromFilePath("input.xlsx"),
                resolver,
                new ExcelConfiguration { SheetName = "RawData", FirstRowIsHeader = true }
            )
        );

        // Process data
        var transform = builder.AddTransform<ProductProcessor, Product, Product>("processor");

        // Write to output file
        var sink = builder.AddSink(
            "excel_sink",
            new ExcelSinkNode<Product>(
                StorageUri.FromFilePath("output.xlsx"),
                resolver,
                new ExcelConfiguration { SheetName = "ProcessedData", FirstRowIsHeader = true }
            )
        );

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

### Working with Legacy XLS Files

Read legacy XLS files with explicit encoding:

```csharp
using System.Text;

var legacyConfig = new ExcelConfiguration
{
    Encoding = Encoding.GetEncoding("Windows-1252"),
    FirstRowIsHeader = true,
    AnalyzeInitialRowCount = 100
};

var resolver = StorageProviderFactory.CreateResolver().Resolver;
var source = new ExcelSourceNode<LegacyRecord>(
    StorageUri.FromFilePath("old_data.xls"),
    resolver,
    legacyConfig
);
```

### Handling Mixed Data Types

When columns contain mixed data types, use accurate type detection:

```csharp
var mixedDataConfig = new ExcelConfiguration
{
    AnalyzeAllColumns = true, // Analyze all rows for accurate type detection
    FirstRowIsHeader = true
};

var resolver = StorageProviderFactory.CreateResolver().Resolver;
var source = new ExcelSourceNode<MixedDataRecord>(
    StorageUri.FromFilePath("mixed_data.xlsx"),
    resolver,
    mixedDataConfig
);
```

## Related Topics

- **[NPipeline Extensions Index](../.)**: Return to the extensions overview.
- **[CSV Connector](./csv.md)**: Learn about working with CSV files.
- **[Storage Provider Interface](./storage-provider.md)**: Understand the storage layer architecture.
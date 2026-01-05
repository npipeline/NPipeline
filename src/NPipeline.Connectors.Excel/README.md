# NPipeline Excel Connector

NPipeline Excel Connector provides source and sink nodes for reading and writing Excel files using ExcelDataReader and DocumentFormat.OpenXml libraries. This
package enables seamless integration of Excel data processing into your NPipeline workflows with configurable parsing options and type-safe operations.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.Excel
```

## Features

- **Excel Source Node**: Read Excel files (both XLS and XLSX formats) and deserialize to strongly-typed objects
- **Excel Sink Node**: Serialize objects to Excel format and write to XLSX files
- **Dual Format Support**: Read from legacy XLS (binary) and modern XLSX (Open XML) formats
- **ExcelDataReader Integration**: Leverages the robust ExcelDataReader library for reliable Excel processing
- **DocumentFormat.OpenXml Integration**: Uses Open XML SDK for writing XLSX files
- **Configurable Parsing Options**: Customize sheet selection, header handling, encoding, and type detection
- **Type-Safe Operations**: Compile-time safety with generic type parameters
- **Storage Abstraction**: Works with pluggable storage providers for flexible file access
- **Streaming Processing**: Memory-efficient streaming for large Excel files (reading)
- **Automatic Type Conversion**: Built-in type mapping for common .NET types

## Usage

### Reading Excel Files

```csharp
using NPipeline.Connectors.Excel;
using NPipeline.Connectors;

// Define your data model
public class Product
{
    public int Id { get; set; }
    public string Name { get; set; }
    public decimal Price { get; set; }
    public string Category { get; set; }
}

// Create a storage resolver for file operations
var resolver = StorageProviderFactory.CreateResolver().Resolver;

// Create an Excel source node
var excelSource = new ExcelSourceNode<Product>(
    StorageUri.FromFilePath("products.xlsx"),
    resolver
);

// Use in a pipeline
var builder = new PipelineBuilder();
var source = builder.AddSource(() => excelSource, "excel-source");
```

### Writing Excel Files

```csharp
// Create a storage resolver for file operations
var resolver = StorageProviderFactory.CreateResolver().Resolver;

// Create an Excel sink node
var excelSink = new ExcelSinkNode<Product>(
    StorageUri.FromFilePath("output.xlsx"),
    resolver
);

// Use in a pipeline
var sink = builder.AddSink(() => excelSink, "excel-sink");
```

### Configuration Options

```csharp
using System.Text;

// Custom Excel configuration
var excelConfig = new ExcelConfiguration
{
    SheetName = "Products",
    FirstRowIsHeader = true,
    BufferSize = 8192,
    AutodetectSeparators = true,
    AnalyzeAllColumns = false,
    AnalyzeInitialRowCount = 30,
    Encoding = Encoding.UTF8
};

// Create storage resolver
var resolver = StorageProviderFactory.CreateResolver().Resolver;

// Create source with custom configuration
var excelSource = new ExcelSourceNode<Product>(
    StorageUri.FromFilePath("data.xlsx"),
    resolver,
    excelConfig
);
```

### Complete Pipeline Example

```csharp
using NPipeline.Connectors.Excel;
using NPipeline.Connectors;
using NPipeline.Pipeline;

public class ExcelProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add Excel source
        var source = builder.AddSource<ExcelSourceNode<Product>, Product>("excel-source");

        // Add a transform (example)
        var transform = builder.AddTransform<ProductTransform, Product, Product>("transform");

        // Add Excel sink
        var sink = builder.AddSink<ExcelSinkNode<Product>, Product>("excel-sink");

        // Connect the nodes
        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

### Reading from Specific Sheet

```csharp
var config = new ExcelConfiguration
{
    SheetName = "SalesData",
    FirstRowIsHeader = true
};

var resolver = StorageProviderFactory.CreateResolver().Resolver;
var excelSource = new ExcelSourceNode<SalesRecord>(
    StorageUri.FromFilePath("quarterly_report.xlsx"),
    resolver,
    config
);
```

### Writing to Specific Sheet

```csharp
var config = new ExcelConfiguration
{
    SheetName = "ProcessedData",
    FirstRowIsHeader = true
};

var resolver = StorageProviderFactory.CreateResolver().Resolver;
var excelSink = new ExcelSinkNode<ProcessedRecord>(
    StorageUri.FromFilePath("output.xlsx"),
    resolver,
    config
);
```

## Configuration Reference

### ExcelConfiguration Properties

| Property                 | Type        | Default | Description                                                                                                           |
|--------------------------|-------------|---------|-----------------------------------------------------------------------------------------------------------------------|
| `BufferSize`             | `int`       | `4096`  | Buffer size for stream operations in bytes                                                                            |
| `SheetName`              | `string?`   | `null`  | Name of the sheet to read from or write to. Uses first sheet for reading or creates default sheet for writing if null |
| `FirstRowIsHeader`       | `bool`      | `true`  | Indicates whether the first row contains column headers                                                               |
| `HasHeaderRow`           | `bool`      | `true`  | Convenience property that syncs with `FirstRowIsHeader`                                                               |
| `Encoding`               | `Encoding?` | `null`  | Encoding for reading legacy XLS files with text data                                                                  |
| `AutodetectSeparators`   | `bool`      | `true`  | Indicates whether to automatically detect separators in CSV-like data                                                 |
| `AnalyzeAllColumns`      | `bool`      | `false` | Indicates whether to analyze entire workbook to determine data types                                                  |
| `AnalyzeInitialRowCount` | `int`       | `30`    | Number of rows to analyze for data type detection when `AnalyzeAllColumns` is false                                   |

## Supported Data Types

The Excel connector supports automatic type conversion for the following .NET types:

- **Primitive Types**: `int`, `long`, `short`, `float`, `double`, `bool`
- **Decimal Types**: `decimal`
- **String Types**: `string`
- **Date/Time Types**: `DateTime`
- **GUID Types**: `Guid`
- **Nullable Types**: All of the above as nullable (e.g., `int?`, `DateTime?`)

## Format Support

### Reading

- **XLS**: Legacy binary Excel format (supported via ExcelDataReader)
- **XLSX**: Modern Open XML Excel format (supported via ExcelDataReader)

### Writing

- **XLSX**: Modern Open XML Excel format (supported via DocumentFormat.OpenXml)
- **XLS**: Not supported for writing (use XLSX format instead)

## Requirements

- **.NET 8.0, 9.0, or 10.0**
- **ExcelDataReader 3.7.0+** (automatically included as a dependency)
- **DocumentFormat.OpenXml 3.0.0+** (automatically included as a dependency)
- **NPipeline.Connectors** (automatically included as a dependency)

## Performance Considerations

### Reading Performance

- Uses streaming access for memory-efficient processing of large files
- Configure `BufferSize` appropriately for your workload (default 4KB)
- For large files with consistent data types, set `AnalyzeAllColumns = false` with appropriate `AnalyzeInitialRowCount`
- For files with varying data types, set `AnalyzeAllColumns = true` for accurate type detection

### Writing Performance

- Collects all items in memory before writing (required by XLSX format)
- Configure `BufferSize` to optimize I/O performance
- Consider batching large datasets to manage memory usage

## Limitations

- **Writing only supports XLSX format**: Legacy XLS format is not supported for writing
- **Memory usage for writing**: The sink node collects all items in memory before writing, which may be a concern for very large datasets
- **Header mapping**: When `FirstRowIsHeader = false`, properties are mapped by column index using a hash-based approach, which may not be deterministic for all
  scenarios
- **Type conversion**: Some complex type conversions may fail silently; ensure your data types are compatible

## Best Practices

1. **Use XLSX format for new files**: XLSX is the modern standard and supports both reading and writing
2. **Specify sheet names explicitly**: This improves code clarity and prevents errors when working with multi-sheet workbooks
3. **Enable headers for structured data**: Set `FirstRowIsHeader = true` for better property mapping
4. **Adjust buffer size for large files**: Increase `BufferSize` for better I/O performance with large files
5. **Validate data types**: Ensure your model properties match the data types in your Excel files
6. **Handle encoding for legacy XLS**: Specify an explicit `Encoding` when reading legacy XLS files with text data

## License

MIT License - see LICENSE file for details.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration
- **[NPipeline.Connectors.Csv](https://www.nuget.org/packages/NPipeline.Connectors.Csv)** - CSV connector for alternative tabular data processing

## Support

- **Documentation**: [https://npipeline.readthedocs.io](https://npipeline.readthedocs.io)
- **Issues**: [GitHub Issues](https://github.com/npipeline/NPipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/npipeline/NPipeline/discussions)
- **Discord**: [NPipeline Community](https://discord.gg/npipeline)

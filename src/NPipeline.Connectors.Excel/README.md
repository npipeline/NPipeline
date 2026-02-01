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

## Requirements

- **.NET 8.0, 9.0, or 10.0**
- **ExcelDataReader 3.7.0+** (automatically included as a dependency)
- **DocumentFormat.OpenXml 3.0.0+** (automatically included as a dependency)
- **NPipeline.Connectors** (automatically included as a dependency)

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
- **Streaming Writes**: Uses OpenXmlWriter to stream rows directly to the provider without double-buffering
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
var resolver = StorageProviderFactory.CreateResolver();

// Create an Excel source node
var excelSource = new ExcelSourceNode<Product>(
    StorageUri.FromFilePath("products.xlsx"),
    row => new Product(
        row.Get<int>("Id") ?? 0,
        row.Get<string>("Name") ?? string.Empty,
        row.Get<decimal>("Price") ?? 0m,
        row.Get<string>("Category") ?? string.Empty),
    resolver
);

// Use in a pipeline
var builder = new PipelineBuilder();
var source = builder.AddSource(() => excelSource, "excel-source");
```

### Writing Excel Files

```csharp
// Create a storage resolver for file operations
var resolver = StorageProviderFactory.CreateResolver();

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
var resolver = StorageProviderFactory.CreateResolver();

// Create source with custom configuration
var excelSource = new ExcelSourceNode<Product>(
    StorageUri.FromFilePath("data.xlsx"),
    row => new Product(
        row.Get<int>("Id") ?? 0,
        row.Get<string>("Name") ?? string.Empty,
        row.Get<decimal>("Price") ?? 0m,
        row.Get<string>("Category") ?? string.Empty),
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
        var source = builder.AddSource(
            new ExcelSourceNode<Product>(
                StorageUri.FromFilePath("products.xlsx"),
                row => new Product(
                    row.Get<int>("Id") ?? 0,
                    row.Get<string>("Name") ?? string.Empty,
                    row.Get<decimal>("Price") ?? 0m,
                    row.Get<string>("Category") ?? string.Empty)),
            "excel-source");

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

var resolver = StorageProviderFactory.CreateResolver();
var excelSource = new ExcelSourceNode<SalesRecord>(
    StorageUri.FromFilePath("quarterly_report.xlsx"),
    row => new SalesRecord(
        row.Get<string>("Region") ?? string.Empty,
        row.Get<string>("Product") ?? string.Empty,
        row.Get<decimal>("Amount") ?? 0m,
        row.Get<DateTime>("Date") ?? default),
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

var resolver = StorageProviderFactory.CreateResolver();
var excelSink = new ExcelSinkNode<ProcessedRecord>(
    StorageUri.FromFilePath("output.xlsx"),
    resolver,
    config
);
```

## Mapping

### Convention-Based Mapping

Properties are automatically mapped to Excel columns using lowercase conversion:

```csharp
public class Customer
{
    public int CustomerId,      // Maps to customerid
    public string FirstName,     // Maps to firstname
    public string EmailAddress    // Maps to emailaddress
}
```

**Why convention-based mapping:** The default lowercase convention provides a simple, predictable mapping for most Excel files while avoiding the complexity of
configuration.

### Attribute-Based Mapping

Override default mapping with attributes:

```csharp
using NPipeline.Connectors.Excel.Attributes;

public class Customer
{
    [ExcelColumn("cust_id")]
    public int Id { get; set; }

    [ExcelColumn("full_name")]
    public string Name { get; set; }

    [ExcelIgnore]
    public string TemporaryField { get; set; }
}
```

The [`ExcelColumnAttribute`](Attributes/ExcelColumnAttribute.cs:7) allows you to:

- Specify the exact Excel column name for a property
- Override the default lowercase convention
- Handle column names that don't follow naming conventions

The [`ExcelIgnoreAttribute`](Attributes/ExcelIgnoreAttribute.cs:7) excludes properties from Excel mapping entirely.

### Reading with Attribute Mapping

When using [`ExcelSourceNode<T>`](ExcelSourceNode.cs) with attributes, the mapper is automatically applied:

```csharp
using NPipeline.Connectors.Excel.Attributes;

public class Customer
{
    [ExcelColumn("CustomerID")]
    public int Id { get; set; }

    [ExcelColumn("FirstName")]
    public string FirstName { get; set; }

    [ExcelColumn("LastName")]
    public string LastName { get; set; }

    [ExcelColumn("EmailAddress")]
    public string Email { get; set; }
}

// The mapper is built automatically using ExcelMapperBuilder<T>
var resolver = StorageProviderFactory.CreateResolver();
var excelSource = new ExcelSourceNode<Customer>(
    StorageUri.FromFilePath("customers.xlsx"),
    resolver
);
```

**How it works:** The [`ExcelMapperBuilder<T>`](Mapping/ExcelMapperBuilder.cs:13) uses compiled expression tree delegates to map Excel rows to objects. This
approach provides:

- **Performance:** Compiled delegates are significantly faster than reflection-based mapping
- **Type safety:** Compile-time checking of property mappings
- **Caching:** Mappers are cached per type to avoid repeated compilation

### Writing with Attribute Mapping

When using [`ExcelSinkNode<T>`](ExcelSinkNode.cs) with attributes, column names are determined from attributes:

```csharp
using NPipeline.Connectors.Excel.Attributes;

public class Customer
{
    [ExcelColumn("CustomerID")]
    public int Id { get; set; }

    [ExcelColumn("FirstName")]
    public string FirstName { get; set; }

    [ExcelColumn("LastName")]
    public string LastName { get; set; }

    [ExcelColumn("EmailAddress")]
    public string Email { get; set; }

    [ExcelIgnore]
    public string InternalNotes { get; set; }  // Not written to Excel
}

// The writer mapper is built automatically using ExcelWriterMapperBuilder<T>
var resolver = StorageProviderFactory.CreateResolver();
var excelSink = new ExcelSinkNode<Customer>(
    StorageUri.FromFilePath("output.xlsx"),
    resolver
);
```

The [`ExcelWriterMapperBuilder<T>`](Mapping/ExcelWriterMapperBuilder.cs:12) determines column order and names based on property order and attributes.

### Advanced Scenarios

#### Mixed Mapping Strategies

You can combine convention-based and attribute-based mapping:

```csharp
public class Customer
{
    // Uses attribute mapping
    [ExcelColumn("cust_id")]
    public int Id { get; set; }

    // Uses convention-based mapping (maps to "firstname")
    public string FirstName { get; set; }

    // Ignored from Excel
    [ExcelIgnore]
    public string InternalId { get; set; }
}
```

#### Nullable Types

Nullable types are handled automatically:

```csharp
public class Customer
{
    [ExcelColumn("customer_id")]
    public int? Id { get; set; }

    [ExcelColumn("phone_number")]
    public string? PhoneNumber { get; set; }

    [ExcelColumn("last_order_date")]
    public DateTime? LastOrderDate { get; set; }
}
```

When an Excel column is missing or empty, nullable properties receive `null` instead of throwing an exception.

#### Case-Insensitive Matching

The mapper performs case-insensitive column matching when attributes are used:

```csharp
public class Customer
{
    // Matches "CustomerID", "customerid", "CUSTOMERID", etc.
    [ExcelColumn("CustomerID")]
    public int Id { get; set; }
}
```

This flexibility accommodates variations in Excel header casing without requiring exact matches.

#### Performance Considerations

The attribute mapping system uses compiled expression trees for optimal performance:

| Mapping Approach       | Performance   | Use Case                                        |
|------------------------|---------------|-------------------------------------------------|
| Convention-based       | Fast          | Simple Excel structures                         |
| Attribute-based        | Fast (cached) | Complex or non-standard Excel structures        |
| Custom mapper function | Variable      | Maximum control, requires manual implementation |

Mappers are cached per type using `ConcurrentDictionary<Type, Delegate>`, ensuring that compilation overhead occurs only once per type.

### Complete Pipeline Example with Attributes

```csharp
using NPipeline.Connectors.Excel;
using NPipeline.Connectors.Excel.Attributes;
using NPipeline.Connectors;
using NPipeline.Pipeline;

public class Customer
{
    [ExcelColumn("CustomerID")]
    public int Id { get; set; }

    [ExcelColumn("FirstName")]
    public string FirstName { get; set; } = string.Empty;

    [ExcelColumn("LastName")]
    public string LastName { get; set; } = string.Empty;

    [ExcelColumn("EmailAddress")]
    public string Email { get; set; } = string.Empty;

    [ExcelIgnore]
    public string InternalNotes { get; set; } = string.Empty;
}

public class ExcelProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var resolver = StorageProviderFactory.CreateResolver();

        // Source uses attribute-based mapping automatically
        var source = builder.AddSource(
            new ExcelSourceNode<Customer>(
                StorageUri.FromFilePath("customers.xlsx"),
                resolver),
            "excel-source");

        // Add transforms as needed
        var transform = builder.AddTransform<CustomerTransform, Customer, Customer>("transform");

        // Sink writes columns in attribute-defined order
        var sink = builder.AddSink(
            new ExcelSinkNode<Customer>(
                StorageUri.FromFilePath("output.xlsx"),
                resolver),
            "excel-sink");

        // Connect nodes
        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
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
- **XLS**: NOT supported for writing (use XLSX format instead)

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

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration
- **[NPipeline.Connectors.Csv](https://www.nuget.org/packages/NPipeline.Connectors.Csv)** - CSV connector for alternative tabular data processing

## License

MIT License - see LICENSE file for details.

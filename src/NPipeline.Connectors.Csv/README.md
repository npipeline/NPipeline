# NPipeline CSV Connector

NPipeline CSV Connector provides source and sink nodes for reading and writing CSV files using the popular CsvHelper library. This package enables seamless
integration of CSV data processing into your NPipeline workflows with configurable parsing options and type-safe operations.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.Csv
```

## Requirements

- **.NET 8.0, 9.0, or 10.0**
- **CsvHelper 33.1.0+** (automatically included as a dependency)
- **NPipeline.Connectors** (automatically included as a dependency)

## Features

- **CSV Source Node**: Read CSV files and deserialize to strongly-typed objects
- **CSV Sink Node**: Serialize objects to CSV format and write to files
- **CsvHelper Integration**: Leverages the robust CsvHelper library for reliable CSV processing
- **Configurable Parsing Options**: Customize delimiters, encoding, and culture settings
- **Type-Safe Operations**: Compile-time safety with generic type parameters
- **Storage Abstraction**: Works with pluggable storage providers for flexible file access
- **Streaming Processing**: Memory-efficient streaming for large CSV files
- **Row-Level Error Handling**: Opt-in handler to decide whether to skip or fail on mapping errors

## Usage

### Reading CSV Files

```csharp
using NPipeline.Connectors.Csv;
using NPipeline.Connectors;

// Define your data model
public class Customer
{
    public int Id { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public string Email { get; set; }
}

// Create a storage resolver for file operations
var resolver = StorageProviderFactory.CreateResolver();

// Create a CSV source node
var csvSource = new CsvSourceNode<Customer>(
    StorageUri.FromFilePath("customers.csv"),
    row => new Customer
    {
        Id = row.Get<int>("Id") ?? 0,
        FirstName = row.Get<string>("FirstName") ?? string.Empty,
        LastName = row.Get<string>("LastName") ?? string.Empty,
        Email = row.Get<string>("Email") ?? string.Empty,
    },
    resolver
);

// Use in a pipeline
var builder = new PipelineBuilder();
var source = builder.AddSource(() => csvSource, "csv-source");
```

### Writing CSV Files

```csharp
// Create a storage resolver for file operations
var resolver = StorageProviderFactory.CreateResolver();

// Create a CSV sink node
var csvSink = new CsvSinkNode<Customer>(
    StorageUri.FromFilePath("output.csv"),
    resolver
);

// Use in a pipeline
var sink = builder.AddSink(() => csvSink, "csv-sink");
```

### Configuration Options

```csharp
using System.Globalization;
using CsvHelper.Configuration;

// Custom CSV configuration
var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
{
    Delimiter = ";",
    HasHeaderRecord = true,
    DetectDelimiter = false,
    TrimOptions = TrimOptions.Trim
};

// Optional: decide how to handle mapping errors (return true to skip the row)
csvConfig.RowErrorHandler = (ex, row) =>
{
    // log and continue
    Console.WriteLine($"Row error: {ex.Message}");
    return true;
};

// Custom encoding
var encoding = Encoding.UTF8;

// Create storage resolver
var resolver = StorageProviderFactory.CreateResolver();

// Create source with custom configuration
var csvSource = new CsvSourceNode<Customer>(
    StorageUri.FromFilePath("data.csv"),
    row => new Customer
    {
        Id = row.Get<int>("Id") ?? 0,
        FirstName = row.Get<string>("FirstName") ?? string.Empty,
        LastName = row.Get<string>("LastName") ?? string.Empty,
        Email = row.Get<string>("Email") ?? string.Empty,
    },
    resolver,
    csvConfig,
    encoding
);
```

## Mapping

### Convention-Based Mapping

Properties are automatically mapped to CSV columns using lowercase conversion:

```csharp
public class Customer
{
    public int CustomerId,      // Maps to customerid
    public string FirstName,     // Maps to firstname
    public string EmailAddress    // Maps to emailaddress
}
```

**Why convention-based mapping:** The default lowercase convention provides a simple, predictable mapping for most CSV files while avoiding the complexity of
configuration.

### Attribute-Based Mapping

Override default mapping with attributes:

```csharp
using NPipeline.Connectors.Csv.Attributes;

public class Customer
{
    [CsvColumn("cust_id")]
    public int Id { get; set; }

    [CsvColumn("full_name")]
    public string Name { get; set; }

    [CsvIgnore]
    public string TemporaryField { get; set; }
}
```

The [`CsvColumnAttribute`](Attributes/CsvColumnAttribute.cs:7) allows you to:

- Specify the exact CSV column name for a property
- Override the default lowercase convention
- Handle column names that don't follow naming conventions

The [`CsvIgnoreAttribute`](Attributes/CsvIgnoreAttribute.cs:7) excludes properties from CSV mapping entirely.

### Reading with Attribute Mapping

When using [`CsvSourceNode<T>`](CsvSourceNode.cs) with attributes, the mapper is automatically applied:

```csharp
using NPipeline.Connectors.Csv.Attributes;

public class Customer
{
    [CsvColumn("CustomerID")]
    public int Id { get; set; }

    [CsvColumn("FirstName")]
    public string FirstName { get; set; }

    [CsvColumn("LastName")]
    public string LastName { get; set; }

    [CsvColumn("EmailAddress")]
    public string Email { get; set; }
}

// The mapper is built automatically using CsvMapperBuilder<T>
var csvSource = new CsvSourceNode<Customer>(
    StorageUri.FromFilePath("customers.csv"),
    resolver
);
```

**How it works:** The [`CsvMapperBuilder<T>`](Mapping/CsvMapperBuilder.cs:13) uses compiled expression tree delegates to map CSV rows to objects. This approach
provides:

- **Performance:** Compiled delegates are significantly faster than reflection-based mapping
- **Type safety:** Compile-time checking of property mappings
- **Caching:** Mappers are cached per type to avoid repeated compilation

### Writing with Attribute Mapping

When using [`CsvSinkNode<T>`](CsvSinkNode.cs) with attributes, column names are determined from attributes:

```csharp
using NPipeline.Connectors.Csv.Attributes;

public class Customer
{
    [CsvColumn("CustomerID")]
    public int Id { get; set; }

    [CsvColumn("FirstName")]
    public string FirstName { get; set; }

    [CsvColumn("LastName")]
    public string LastName { get; set; }

    [CsvColumn("EmailAddress")]
    public string Email { get; set; }

    [CsvIgnore]
    public string InternalNotes { get; set; }  // Not written to CSV
}

// The writer mapper is built automatically using CsvWriterMapperBuilder<T>
var csvSink = new CsvSinkNode<Customer>(
    StorageUri.FromFilePath("output.csv"),
    resolver
);
```

The [`CsvWriterMapperBuilder<T>`](Mapping/CsvWriterMapperBuilder.cs:12) determines column order and names based on property order and attributes.

### Advanced Scenarios

#### Mixed Mapping Strategies

You can combine convention-based and attribute-based mapping:

```csharp
public class Customer
{
    // Uses attribute mapping
    [CsvColumn("cust_id")]
    public int Id { get; set; }

    // Uses convention-based mapping (maps to "firstname")
    public string FirstName { get; set; }

    // Ignored from CSV
    [CsvIgnore]
    public string InternalId { get; set; }
}
```

#### Nullable Types

Nullable types are handled automatically:

```csharp
public class Customer
{
    [CsvColumn("customer_id")]
    public int? Id { get; set; }

    [CsvColumn("phone_number")]
    public string? PhoneNumber { get; set; }

    [CsvColumn("last_order_date")]
    public DateTime? LastOrderDate { get; set; }
}
```

When a CSV column is missing or empty, nullable properties receive `null` instead of throwing an exception.

#### Case-Insensitive Matching

The mapper performs case-insensitive column matching when attributes are used:

```csharp
public class Customer
{
    // Matches "CustomerID", "customerid", "CUSTOMERID", etc.
    [CsvColumn("CustomerID")]
    public int Id { get; set; }
}
```

This flexibility accommodates variations in CSV header casing without requiring exact matches.

#### Performance Considerations

The attribute mapping system uses compiled expression trees for optimal performance:

| Mapping Approach       | Performance   | Use Case                                        |
|------------------------|---------------|-------------------------------------------------|
| Convention-based       | Fast          | Simple CSV structures                           |
| Attribute-based        | Fast (cached) | Complex or non-standard CSV structures          |
| Custom mapper function | Variable      | Maximum control, requires manual implementation |

Mappers are cached per type using `ConcurrentDictionary<Type, Delegate>`, ensuring that compilation overhead occurs only once per type.

### Complete Pipeline Example with Attributes

```csharp
using NPipeline.Connectors.Csv;
using NPipeline.Connectors.Csv.Attributes;
using NPipeline.Connectors;
using NPipeline.Pipeline;

public class Customer
{
    [CsvColumn("CustomerID")]
    public int Id { get; set; }

    [CsvColumn("FirstName")]
    public string FirstName { get; set; } = string.Empty;

    [CsvColumn("LastName")]
    public string LastName { get; set; } = string.Empty;

    [CsvColumn("EmailAddress")]
    public string Email { get; set; } = string.Empty;

    [CsvIgnore]
    public string InternalNotes { get; set; } = string.Empty;
}

public class CsvProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var resolver = StorageProviderFactory.CreateResolver();

        // Source uses attribute-based mapping automatically
        var source = builder.AddSource(
            new CsvSourceNode<Customer>(
                StorageUri.FromFilePath("customers.csv"),
                resolver),
            "csv-source");

        // Add transforms as needed
        var transform = builder.AddTransform<CustomerTransform, Customer, Customer>("transform");

        // Sink writes columns in attribute-defined order
        var sink = builder.AddSink(
            new CsvSinkNode<Customer>(
                StorageUri.FromFilePath("output.csv"),
                resolver),
            "csv-sink");

        // Connect nodes
        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

### Complete Pipeline Example

```csharp
using NPipeline.Connectors.Csv;
using NPipeline.Connectors;
using NPipeline.Pipeline;

public class CsvProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add CSV source
        var source = builder.AddSource(
            new CsvSourceNode<Customer>(
                StorageUri.FromFilePath("customers.csv"),
                row => new Customer
                {
                    Id = row.Get<int>("Id") ?? 0,
                    FirstName = row.Get<string>("FirstName") ?? string.Empty,
                    LastName = row.Get<string>("LastName") ?? string.Empty,
                    Email = row.Get<string>("Email") ?? string.Empty,
                }),
            "csv-source");

        // Add a transform (example)
        var transform = builder.AddTransform<CustomerTransform, Customer, Customer>("transform");

        // Add CSV sink
        var sink = builder.AddSink<CsvSinkNode<Customer>, Customer>("csv-sink");

        // Connect the nodes
        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## License

MIT License - see LICENSE file for details.

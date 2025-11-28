# NPipeline CSV Connector

NPipeline CSV Connector provides source and sink nodes for reading and writing CSV files using the popular CsvHelper library. This package enables seamless integration of CSV data processing into your NPipeline workflows with configurable parsing options and type-safe operations.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.Csv
```

## Features

- **CSV Source Node**: Read CSV files and deserialize to strongly-typed objects
- **CSV Sink Node**: Serialize objects to CSV format and write to files
- **CsvHelper Integration**: Leverages the robust CsvHelper library for reliable CSV processing
- **Configurable Parsing Options**: Customize delimiters, encoding, and culture settings
- **Type-Safe Operations**: Compile-time safety with generic type parameters
- **Storage Abstraction**: Works with pluggable storage providers for flexible file access
- **Streaming Processing**: Memory-efficient streaming for large CSV files

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

// Create a CSV source node
var csvSource = new CsvSourceNode<Customer>(
    StorageUri.FromFilePath("customers.csv")
);

// Use in a pipeline
var builder = new PipelineBuilder();
var source = builder.AddSource(() => csvSource, "csv-source");
```

### Writing CSV Files

```csharp
// Create a CSV sink node
var csvSink = new CsvSinkNode<Customer>(
    StorageUri.FromFilePath("output.csv")
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

// Custom encoding
var encoding = Encoding.UTF8;

// Create source with custom configuration
var csvSource = new CsvSourceNode<Customer>(
    StorageUri.FromFilePath("data.csv"),
    configuration: csvConfig,
    encoding: encoding
);
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
        var source = builder.AddSource<CsvSourceNode<Customer>, Customer>("csv-source");
        
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

## Requirements

- **.NET 8.0, 9.0, or 10.0**
- **CsvHelper 33.1.0+** (automatically included as a dependency)
- **NPipeline.Connectors** (automatically included as a dependency)

## License

MIT License - see LICENSE file for details.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## Support

- **Documentation**: [https://npipeline.readthedocs.io](https://npipeline.readthedocs.io)
- **Issues**: [GitHub Issues](https://github.com/npipeline/NPipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/npipeline/NPipeline/discussions)
- **Discord**: [NPipeline Community](https://discord.gg/npipeline)
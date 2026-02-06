---
title: CSV Connector
description: Read from and write to Comma-Separated Values (CSV) files with NPipeline using the CSV connector.
sidebar_position: 1
---

## CSV Connector

The `NPipeline.Connectors.Csv` package provides specialized source and sink nodes for working with Comma-Separated Values (CSV) files. This allows you to easily integrate CSV data into your pipelines as an input source or an output destination.

This connector uses the popular [CsvHelper](https://joshclose.github.io/CsvHelper/) library under the hood, so it is powerful and highly configurable.

## Installation

To use the CSV connector, install the `NPipeline.Connectors.Csv` NuGet package:

```bash
dotnet add package NPipeline.Connectors.Csv
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Storage Abstraction Layer

The CSV connector uses NPipeline's storage abstraction layer, which provides a unified way to work with different storage systems. This layer allows you to work with local files, cloud storage (like S3 or Azure Blob), and other storage systems using the same API.

> **Note:** The storage abstraction layer is provided by the `NPipeline.StorageProviders` namespace/assembly.

### StorageUri

The `StorageUri` class represents a normalized storage location URI. It supports both absolute URIs (e.g., "s3://bucket/key") and local file paths. For local files, use the `StorageUri.FromFilePath()` method:

```csharp
// For local files
var localFileUri = StorageUri.FromFilePath("data/input.csv");

// For absolute URIs (e.g., cloud storage)
var cloudUri = StorageUri.Parse("s3://my-bucket/path/to/file.csv");
```

### IStorageResolver

The `IStorageResolver` interface is responsible for discovering and resolving storage providers capable of handling a given `StorageUri`.

**Default Behavior (Optional):** When no resolver is provided, `CsvSourceNode` and `CsvSinkNode` automatically create a default resolver configured with the standard file system provider. This is ideal for most use cases involving local files.

**When to Provide an Explicit Resolver:** You only need to provide a custom resolver when:

- Working with cloud storage systems (S3, Azure, etc.)
- Using custom storage providers
- Needing to override default provider selection

To create a custom resolver:

```csharp
using NPipeline.StorageProviders;

var resolver = StorageProviderFactory.CreateResolver();
```

### When You Need an Explicit Resolver

For most scenarios involving local files, you can omit the resolver parameter:

```csharp
// Simple case: reading local CSV file (resolver not needed)
var source = new CsvSourceNode<User>(
    StorageUri.FromFilePath("users.csv"),
    row => new User(
        row.Get<int>("Id") ?? 0,
        row.Get<string>("Name") ?? string.Empty,
        row.Get<string>("Email") ?? string.Empty)
);
```

However, you must provide an explicit resolver when working with cloud storage:

```csharp
// Advanced case: reading from S3 (custom resolver required)
var resolver = StorageProviderFactory.CreateResolver(
    new StorageResolverOptions
    {
        IncludeFileSystem = true,
        AdditionalProviders = new[] { new S3StorageProvider() } // Custom provider
    }
);

var source = new CsvSourceNode<User>(
    StorageUri.Parse("s3://my-bucket/users.csv"),
    row => new User(
        row.Get<int>("Id") ?? 0,
        row.Get<string>("Name") ?? string.Empty,
        row.Get<string>("Email") ?? string.Empty),
    resolver: resolver // Explicit resolver needed for cloud storage
);
```

## Common Attributes

The CSV connector supports common attributes from `NPipeline.Connectors.Attributes` that work across all connectors.

### `[Column]` Attribute

The `[Column]` attribute (from `NPipeline.Connectors.Attributes`) is a common attribute that allows you to specify column names and control property mapping across all connectors. It provides:

- **`Name`**: The column name in the data source
- **`Ignore`**: When `true`, skips mapping this property

This attribute is recommended for all scenarios where you need to specify column names or exclude properties.

```csharp
using NPipeline.Connectors.Attributes;

public class Customer
{
    [Column("customer_id")]
    public int Id { get; set; }

    [Column("first_name")]
    public string FirstName { get; set; } = string.Empty;

    [Column("last_name")]
    public string LastName { get; set; } = string.Empty;

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

### `[IgnoreColumn]` Attribute

The `[IgnoreColumn]` attribute (from `NPipeline.Connectors.Attributes`) is a marker attribute that excludes a property from mapping entirely. This is useful for computed properties or fields that should not be persisted.

```csharp
using NPipeline.Connectors.Attributes;

public class Customer
{
    public int Id { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public int Age { get; set; }

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";

    [IgnoreColumn]
    public bool IsAdult => Age >= 18;
}
```

## `CsvSourceNode<T>`

The `CsvSourceNode<T>` reads data from a CSV file and emits each row as an item of type `T`.

### Source Configuration

The constructor for `CsvSourceNode<T>` takes the file path and optional configuration for parsing the CSV.

```csharp
public CsvSourceNode(
    StorageUri uri,
    Func<CsvRow, T> rowMapper,
    IStorageResolver? resolver = null,
    CsvConfiguration? configuration = null,
    Encoding? encoding = null)
```

- **`uri`**: The `StorageUri` representing the location of the CSV file. Use `StorageUri.FromFilePath("path/to/file.csv")` for local files.
- **`rowMapper`**: The row mapper used to construct `T` from a `CsvRow`. This is required and avoids reflection.
- **`resolver`**: *(Optional)* The `IStorageResolver` to resolve storage providers. If omitted, a default resolver with file system support is used automatically.
- **`configuration`**: *(Optional)* A `CsvConfiguration` object to customize parsing (e.g., delimiter, culture, quoting).
- **`encoding`**: *(Optional)* Text encoding. Defaults to UTF-8.

### Example: Reading a CSV File

Let's assume you have a `users.csv` file:

```csv
Id,Name,Email
1,Alice,alice@example.com
2,Bob,bob@example.com
```

And a corresponding C# record:

```csharp
public sealed record User(int Id, string Name, string Email);
```

You can read this data into your pipeline as follows:

```csharp
using NPipeline;
using NPipeline.Connectors;
using NPipeline.Connectors.Csv;
using NPipeline.DataFlow.DataPipes;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tracing;

public sealed record User(int Id, string Name, string Email);

public sealed class CsvReaderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Create the CSV source node - resolver is optional; defaults to file system provider for local files
        var sourceNode = new CsvSourceNode<User>(
            StorageUri.FromFilePath("users.csv"),
            row => new User(
                row.Get<int>("Id") ?? 0,
                row.Get<string>("Name") ?? string.Empty,
                row.Get<string>("Email") ?? string.Empty));
        var source = builder.AddSource(sourceNode, "csv_source");
        var sink = builder.AddSink<ConsoleSinkNode, User>("console_sink");

        builder.Connect(source, sink);
    }
}

public sealed class ConsoleSinkNode : SinkNode<User>
{
    public override async Task ExecuteAsync(
        IDataPipe<User> input,
        PipelineContext context,
        IPipelineActivity parentActivity,
        CancellationToken cancellationToken)
    {
        await foreach (var user in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Received: {user}");
        }
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        // Create a dummy CSV file for demonstration
        await System.IO.File.WriteAllLinesAsync("users.csv", new[]
        {
            "Id,Name,Email",
            "1,Alice,alice@example.com",
            "2,Bob,bob@example.com"
        });

        // Create and run the pipeline
        var runner = PipelineRunner.Create();
        await runner.RunAsync<CsvReaderPipeline>();

        Console.WriteLine("CSV reading completed");
    }
}
```

**Expected Output:**

```text
Received: User { Id = 1, Name = Alice, Email = alice@example.com }
Received: User { Id = 2, Name = Bob, Email = bob@example.com }
CSV reading completed
```

## `CsvSinkNode<T>`

The `CsvSinkNode<T>` writes items from the pipeline to a CSV file.

### Sink Configuration

The constructor for `CsvSinkNode<T>` takes the file path and optional configuration for writing the CSV.

```csharp
public CsvSinkNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    CsvConfiguration? configuration = null,
    Encoding? encoding = null)
```

- **`uri`**: The `StorageUri` representing the location of the output CSV file. Use `StorageUri.FromFilePath("path/to/file.csv")` for local files.
- **`resolver`**: *(Optional)* The `IStorageResolver` to resolve storage providers. If omitted, a default resolver with file system support is used automatically.
- **`configuration`**: *(Optional)* A `CsvConfiguration` object to customize writing.
- **`encoding`**: *(Optional)* An `Encoding` for the file. Defaults to UTF-8.

### Example: Writing to a CSV File

Let's take processed user data and write it to an `output.csv` file.

```csharp
using NPipeline.Connectors;
using NPipeline.Connectors.Csv;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record ProcessedUser(int Id, string FullName, string Status);

public sealed class CsvWriterPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<InMemorySourceNode<ProcessedUser>, ProcessedUser>("source");
        // Create the CSV sink node - resolver is optional; defaults to file system provider for local files
        var sinkNode = new CsvSinkNode<ProcessedUser>(StorageUri.FromFilePath("output.csv"));
        var sink = builder.AddSink(sinkNode, "csv_sink");

        builder.Connect(source, sink);
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var users = new List<ProcessedUser>
        {
            new(1, "Alice Smith", "Active"),
            new(2, "Bob Johnson", "Inactive")
        };

        // Set up test data
        var context = PipelineContext.Default;
        context.Items[typeof(InMemorySourceNode<ProcessedUser>).FullName!] = users.ToArray();

        var runner = PipelineRunner.Create();
        await runner.RunAsync<CsvWriterPipeline>(context);

        Console.WriteLine("\nContent of output.csv:");
        Console.WriteLine(await System.IO.File.ReadAllTextAsync("output.csv"));
    }
}
```

**Expected `output.csv` Content:**

```csv
Id,FullName,Status
1,Alice Smith,Active
2,Bob Johnson,Inactive
```

## Advanced Configuration

Both `CsvSourceNode` and `CsvSinkNode` accept an optional `CsvConfiguration` object from the CsvHelper library in their constructors. This allows you to customize parsing and writing behavior.

Common configuration options include:

- `HasHeaderRecord`: Specify whether the CSV file has a header row (default is `true`).
- `Delimiter`: Change the field delimiter (e.g., to a tab `\t` or semicolon `;`).
- `CultureInfo`: Specify the culture to use for parsing numbers and dates.
- `BufferSize`: Controls the buffer size for the StreamWriter used in CSV operations (default is 1024).

### Buffer Size Configuration

The [`BufferSize`](../../../src/NPipeline.Connectors.Csv/CsvConfiguration.cs:16) property controls the internal buffer size for CSV I/O operations:

- **Default value**: 1024 bytes
- **Purpose**: Determines the size of the buffer used by StreamWriter when reading or writing CSV files
- **Performance impact**: Larger buffers can improve I/O performance for large files but use more memory

When to adjust BufferSize:

- **Increase** (e.g., 4096, 8192) for:
  - Processing very large CSV files
  - High-throughput scenarios where I/O performance is critical
  - Systems with abundant memory resources
- **Decrease** (e.g., 512) for:
  - Memory-constrained environments
  - Processing many small CSV files concurrently
  - Scenarios where memory usage must be tightly controlled

```csharp
// Example: Custom buffer size for large file processing
var largeFileConfig = new CsvConfiguration()
{
    BufferSize = 8192, // 8KB buffer for better performance with large files
    HelperConfiguration = {
        Delimiter = ",",
        HasHeaderRecord = true
    }
};

// Resolver is optional - omit it to use the default file system resolver
var source = new CsvSourceNode<User>(
    StorageUri.FromFilePath("large_dataset.csv"),
    row => new User(
        row.Get<int>("Id") ?? 0,
        row.Get<string>("Name") ?? string.Empty,
        row.Get<string>("Email") ?? string.Empty),
    configuration: largeFileConfig);
```

### Example: Using a custom delimiter and no header

```csharp
using CsvHelper.Configuration;
using System.Globalization;

// Configure for a tab-separated file with no header
var config = new CsvConfiguration(CultureInfo.InvariantCulture)
{
    Delimiter = "\t",
    HasHeaderRecord = false,
};

// Resolver is optional - omit it to use the default file system resolver
var source = new CsvSourceNode<User>(
    StorageUri.FromFilePath("users.tsv"),
    row => new User(
        row.GetByIndex<int>(0) ?? 0,
        row.GetByIndex<string>(1) ?? string.Empty,
        row.GetByIndex<string>(2) ?? string.Empty),
    configuration: config);
```

In this advanced scenario, we configure the source to read a tab-separated file (`.tsv`) that does not have a header. Because there's no header, map by index using `row.GetByIndex<T>(index)`.

### Example: Transforming and Writing to CSV

This pipeline transforms user data and writes the result to a new CSV file.

```csharp
using NPipeline.Connectors.Csv;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record UserSummary(string Name, string Domain);

public sealed class Summarizer : TransformNode<User, UserSummary>
{
    public override Task<UserSummary> ExecuteAsync(
        User item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var domain = item.Email.Split('@')[1];
        return Task.FromResult(new UserSummary(item.Name, domain));
    }
}

public sealed class CsvTransformPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Resolver is optional - default file system resolver is used automatically
        var source = builder.AddSource(new CsvSourceNode<User>(
            StorageUri.FromFilePath("users.csv"),
            row => new User(
                row.Get<int>("Id") ?? 0,
                row.Get<string>("Name") ?? string.Empty,
                row.Get<string>("Email") ?? string.Empty)), "csv_source");
        var transform = builder.AddTransform<Summarizer, User, UserSummary>("summarizer");
        var sinkNode = new CsvSinkNode<UserSummary>(StorageUri.FromFilePath("summaries.csv"));
        var sink = builder.AddSink(sinkNode, "csv_sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        var runner = PipelineRunner.Create();
        await runner.RunAsync<CsvTransformPipeline>();
    }
}
```

After running, this will create a `summaries.csv` file with the following content:

```csv
Name,Domain
Alice,example.com
Bob,example.com
```

For more advanced configuration, refer to the [CsvHelper documentation](https://joshclose.github.io/CsvHelper/getting-started/).

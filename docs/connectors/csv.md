---
title: CSV Connector
description: Read from and write to Comma-Separated Values (CSV) files with NPipeline using the CSV connector.
sidebar_position: 1
---

# CSV Connector

The `NPipeline.Connectors.Csv` package provides specialized source and sink nodes for working with Comma-Separated Values (CSV) files. This allows you to easily integrate CSV data into your pipelines as an input source or an output destination.

This connector uses the popular [CsvHelper](https://joshclose.github.io/CsvHelper/) library under the hood, so it is powerful and highly configurable.

## Installation

To use the CSV connector, install the `NPipeline.Connectors.Csv` NuGet package:

```bash
dotnet add package NPipeline.Connectors.Csv
```

## Storage Abstraction Layer

The CSV connector uses NPipeline's storage abstraction layer, which provides a unified way to work with different storage systems. This layer allows you to work with local files, cloud storage (like S3 or Azure Blob), and other storage systems using the same API.

### StorageUri

The `StorageUri` class represents a normalized storage location URI. It supports both absolute URIs (e.g., "s3://bucket/key") and local file paths. For local files, use the `StorageUri.FromFilePath()` method:

```csharp
// For local files
var localFileUri = StorageUri.FromFilePath("data/input.csv");

// For absolute URIs (e.g., cloud storage)
var cloudUri = StorageUri.Parse("s3://my-bucket/path/to/file.csv");
```

### IStorageResolver

The `IStorageResolver` interface is responsible for discovering and resolving storage providers capable of handling a given `StorageUri`. In most cases, you don't need to provide one - a default resolver will be used that can handle local file system operations. You might need to provide a custom resolver when:

- Working with cloud storage systems
- Using custom storage providers
- Needing to override default provider selection

If you don't specify a resolver, the connector will use a default one that supports local file operations.

## `CsvSourceNode<T>`

The `CsvSourceNode<T>` reads data from a CSV file and emits each row as an item of type `T`.

### Configuration

The constructor for `CsvSourceNode<T>` typically takes the file path and optional configuration for parsing the CSV.

```csharp
public CsvSourceNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    CsvConfiguration? configuration = null,
    Encoding? encoding = null)
```

- **`uri`**: The `StorageUri` representing the location of the CSV file. Use `StorageUri.FromFilePath("path/to/file.csv")` for local files.
- **`resolver`**: An optional `IStorageResolver` to resolve storage providers. If null, a default resolver will be used.
- **`configuration`**: An optional `CsvConfiguration` object to customize parsing (e.g., delimiter, culture, quoting).
- **`encoding`**: An optional `Encoding` for the file. Defaults to UTF-8.

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
        var source = builder.AddSource("csv_source", new CsvSourceNode<User>(StorageUri.FromFilePath("users.csv")));
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
        var runner = new PipelineRunner();
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

### Configuration

The constructor for `CsvSinkNode<T>` takes the file path and optional configuration for writing the CSV.

```csharp
public CsvSinkNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    CsvConfiguration? configuration = null,
    Encoding? encoding = null)
```

- **`uri`**: The `StorageUri` representing the location of the output CSV file. Use `StorageUri.FromFilePath("path/to/file.csv")` for local files.
- **`resolver`**: An optional `IStorageResolver` to resolve storage providers. If null, a default resolver will be used.
- **`configuration`**: An optional `CsvConfiguration` object to customize writing.
- **`encoding`**: An optional `Encoding` for the file. Defaults to UTF-8.

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
        var sink = builder.AddSink("csv_sink", new CsvSinkNode<ProcessedUser>(StorageUri.FromFilePath("output.csv")));

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

        var runner = new PipelineRunner();
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

// The node will expect properties to be mapped by index
// This requires using CsvHelper's class mapping feature
public sealed class UserMap : ClassMap<User>
{
    public UserMap()
    {
        Map(m => m.Id).Index(0);
        Map(m => m.Name).Index(1);
        Map(m => m.Email).Index(2);
    }
}

var source = new CsvSourceNode<User>(StorageUri.FromFilePath("users.tsv"), configuration: config);
```

In this advanced scenario, we configure the source to read a tab-separated file (`.tsv`) that does not have a header. Because there's no header, we must provide a `ClassMap` to tell CsvHelper how to map columns by their index to the properties of our `User` record. You can register the class map through the `CsvConfiguration` object.

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
        var source = builder.AddSource("csv_source", new CsvSourceNode<User>(StorageUri.FromFilePath("users.csv")));
        var transform = builder.AddTransform<Summarizer, User, UserSummary>("summarizer");
        var sink = builder.AddSink("csv_sink", new CsvSinkNode<UserSummary>(StorageUri.FromFilePath("summaries.csv")));

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        var runner = new PipelineRunner();
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

## Related Topics

- **[NPipeline Extensions Index](../.)**: Return to the extensions overview.


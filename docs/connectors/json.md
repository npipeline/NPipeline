---
title: JSON Connector
description: Read from and write to JSON files with NPipeline using the JSON connector.
sidebar_position: 3
---

## JSON Connector

The `NPipeline.Connectors.Json` package provides specialized source and sink nodes for working with JSON files. This allows you to easily integrate JSON data into your pipelines as an input source or an output destination.

This connector uses [System.Text.Json](https://learn.microsoft.com/en-us/dotnet/standard/serialization/system-text-json-overview) for efficient streaming and serialization, providing high performance with minimal dependencies.

## Installation

To use the JSON connector, install the `NPipeline.Connectors.Json` NuGet package:

```bash
dotnet add package NPipeline.Connectors.Json
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Storage Abstraction Layer

The JSON connector uses NPipeline's storage abstraction layer, which provides a unified way to work with different storage systems. This layer allows you to work with local files, cloud storage (like S3 or Azure Blob), and other storage systems using the same API.

> **Note:** The storage abstraction layer is provided by the `NPipeline.StorageProviders` namespace/assembly.

### StorageUri

The `StorageUri` class represents a normalized storage location URI. It supports both absolute URIs (e.g., "s3://bucket/key") and local file paths. For local files, use the `StorageUri.FromFilePath()` method:

```csharp
// For local files
var localFileUri = StorageUri.FromFilePath("data/input.json");

// For absolute URIs (e.g., cloud storage)
var cloudUri = StorageUri.Parse("s3://my-bucket/path/to/file.json");
```

### IStorageResolver

The `IStorageResolver` interface is responsible for discovering and resolving storage providers capable of handling a given `StorageUri`.

**Default Behavior (Optional):** When no resolver is provided, `JsonSourceNode` and `JsonSinkNode` automatically create a default resolver configured with the standard file system provider. This is ideal for most use cases involving local files.

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
// Simple case: reading local JSON file (resolver not needed)
var source = new JsonSourceNode<User>(
    StorageUri.FromFilePath("users.json"),
    row => new User(
        row.Get<int>("id") ?? 0,
        row.Get<string>("firstName") ?? string.Empty,
        row.Get<string>("lastName") ?? string.Empty)
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

var source = new JsonSourceNode<User>(
    StorageUri.Parse("s3://my-bucket/users.json"),
    row => new User(
        row.Get<int>("id") ?? 0,
        row.Get<string>("firstName") ?? string.Empty,
        row.Get<string>("lastName") ?? string.Empty),
    resolver: resolver // Explicit resolver needed for cloud storage
);
```

## Common Attributes

The JSON connector supports common attributes from `NPipeline.Connectors.Attributes` that work across all connectors.

### `[Column]` Attribute

The `[Column]` attribute (from `NPipeline.Connectors.Attributes`) is a common attribute that allows you to specify JSON property names and control property mapping across all connectors. It provides:

- **`Name`**: The JSON property name in the data source
- **`Ignore`**: When `true`, skips mapping this property

This attribute is recommended for all scenarios where you need to specify property names or exclude properties.

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

### System.Text.Json Attributes

The JSON connector also honors the standard `[JsonPropertyName]` and `[JsonIgnore]` attributes from `System.Text.Json.Serialization` for compatibility with existing code. However, the shared `ColumnAttribute` and `IgnoreColumnAttribute` are recommended for consistency across connectors.

```csharp
using System.Text.Json.Serialization;

public class Customer
{
    [JsonPropertyName("customer_id")]
    public int Id { get; set; }

    [JsonPropertyName("first_name")]
    public string FirstName { get; set; } = string.Empty;

    [JsonIgnore]
    public string FullName => $"{FirstName} {LastName}";
}
```

## `JsonSourceNode<T>`

The `JsonSourceNode<T>` reads data from a JSON file and emits each object as an item of type `T`.

### Source Configuration

The constructor for `JsonSourceNode<T>` takes the file path and optional configuration for parsing the JSON.

```csharp
// Attribute-based mapping (recommended)
public JsonSourceNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    JsonConfiguration? configuration = null)

// Manual mapper function
public JsonSourceNode(
    StorageUri uri,
    Func<JsonRow, T> rowMapper,
    IStorageResolver? resolver = null,
    JsonConfiguration? configuration = null)

// With explicit storage provider
public JsonSourceNode(
    IStorageProvider provider,
    StorageUri uri,
    JsonConfiguration? configuration = null)

public JsonSourceNode(
    IStorageProvider provider,
    StorageUri uri,
    Func<JsonRow, T> rowMapper,
    JsonConfiguration? configuration = null)
```

- **`uri`**: The `StorageUri` representing the location of the JSON file. Use `StorageUri.FromFilePath("path/to/file.json")` for local files.
- **`rowMapper`**: (Optional) The row mapper used to construct `T` from a `JsonRow`. When omitted, attribute-based mapping is used.
- **`resolver`**: *(Optional)* The `IStorageResolver` to resolve storage providers. If omitted, a default resolver with file system support is used automatically.
- **`provider`**: *(Optional)* An explicit `IStorageProvider` instance to use instead of resolver-based resolution.
- **`configuration`**: *(Optional)* A `JsonConfiguration` object to customize parsing (e.g., format, naming policy, error handling).

### Example: Reading a JSON Array File

Let's assume you have a `users.json` file with the following content:

```json
[
  {"id": 1, "firstName": "Alice", "lastName": "Smith", "email": "alice@example.com"},
  {"id": 2, "firstName": "Bob", "lastName": "Johnson", "email": "bob@example.com"}
]
```

And a corresponding C# record:

```csharp
public sealed record User(int Id, string FirstName, string LastName, string Email);
```

You can read this data into your pipeline as follows:

```csharp
using NPipeline;
using NPipeline.Connectors;
using NPipeline.Connectors.Json;
using NPipeline.DataFlow.DataPipes;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tracing;

public sealed record User(int Id, string FirstName, string LastName, string Email);

public sealed class JsonReaderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Create the JSON source node with manual mapper - resolver is optional; defaults to file system provider for local files
        var sourceNode = new JsonSourceNode<User>(
            StorageUri.FromFilePath("users.json"),
            row => new User(
                row.Get<int>("id") ?? 0,
                row.Get<string>("firstName") ?? string.Empty,
                row.Get<string>("lastName") ?? string.Empty,
                row.Get<string>("email") ?? string.Empty));
        var source = builder.AddSource(sourceNode, "json_source");
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
        // Create and run the pipeline
        var runner = PipelineRunner.Create();
        await runner.RunAsync<JsonReaderPipeline>();

        Console.WriteLine("JSON reading completed");
    }
}
```

**Expected Output:**

```text
Received: User { Id = 1, FirstName = Alice, LastName = Smith, Email = alice@example.com }
Received: User { Id = 2, FirstName = Bob, LastName = Johnson, Email = bob@example.com }
JSON reading completed
```

### Example: Reading NDJSON File

NDJSON (Newline-Delimited JSON) is a format where each line contains a separate JSON object. This is useful for streaming and log files.

Let's assume you have a `users.ndjson` file:

```json
{"id": 1, "firstName": "Alice", "lastName": "Smith", "email": "alice@example.com"}
{"id": 2, "firstName": "Bob", "lastName": "Johnson", "email": "bob@example.com"}
```

You can read this data with the following configuration:

```csharp
using NPipeline.Connectors.Json;

var config = new JsonConfiguration
{
    Format = JsonFormat.NewlineDelimited
};

// Resolver is optional - omit it to use the default file system resolver
var sourceNode = new JsonSourceNode<User>(
    StorageUri.FromFilePath("users.ndjson"),
    row => new User(
        row.Get<int>("id") ?? 0,
        row.Get<string>("firstName") ?? string.Empty,
        row.Get<string>("lastName") ?? string.Empty,
        row.Get<string>("email") ?? string.Empty),
    configuration: config);
```

### Example: Attribute-Based Mapping

You can use attributes on your model class to define property mappings:

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

    [Column("email_address")]
    public string Email { get; set; } = string.Empty;

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}

public sealed class AttributeMappingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Attribute-based mapping - no manual mapper needed
        var sourceNode = new JsonSourceNode<Customer>(
            StorageUri.FromFilePath("customers.json"));
        var source = builder.AddSource(sourceNode, "json_source");
        var sink = builder.AddSink<ConsoleSinkNode, Customer>("console_sink");

        builder.Connect(source, sink);
    }
}
```

## `JsonSinkNode<T>`

The `JsonSinkNode<T>` writes items from the pipeline to a JSON file.

### Sink Configuration

The constructor for `JsonSinkNode<T>` takes the file path and optional configuration for writing the JSON.

```csharp
// Attribute-based mapping (recommended)
public JsonSinkNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    JsonConfiguration? configuration = null)

// With explicit storage provider
public JsonSinkNode(
    IStorageProvider provider,
    StorageUri uri,
    JsonConfiguration? configuration = null)
```

- **`uri`**: The `StorageUri` representing the location of the output JSON file. Use `StorageUri.FromFilePath("path/to/file.json")` for local files.
- **`resolver`**: *(Optional)* The `IStorageResolver` to resolve storage providers. If omitted, a default resolver with file system support is used automatically.
- **`provider`**: *(Optional)* An explicit `IStorageProvider` instance to use instead of resolver-based resolution.
- **`configuration`**: *(Optional)* A `JsonConfiguration` object to customize writing (e.g., format, indentation, naming policy).

### Example: Writing to a JSON Array File

Let's take processed user data and write it to an `output.json` file.

```csharp
using NPipeline.Connectors;
using NPipeline.Connectors.Json;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record ProcessedUser(int Id, string FullName, string Status);

public sealed class JsonWriterPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<InMemorySourceNode<ProcessedUser>, ProcessedUser>("source");
        // Create the JSON sink node - resolver is optional; defaults to file system provider for local files
        var sinkNode = new JsonSinkNode<ProcessedUser>(StorageUri.FromFilePath("output.json"));
        var sink = builder.AddSink(sinkNode, "json_sink");

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
        await runner.RunAsync<JsonWriterPipeline>(context);

        Console.WriteLine("\nContent of output.json:");
        Console.WriteLine(await System.IO.File.ReadAllTextAsync("output.json"));
    }
}
```

**Expected `output.json` Content:**

```json
[{"id":1,"fullname":"Alice Smith","status":"Active"},{"id":2,"fullname":"Bob Johnson","status":"Inactive"}]
```

### Example: Writing NDJSON File

To write in NDJSON format, configure the sink with `JsonFormat.NewlineDelimited`:

```csharp
var config = new JsonConfiguration
{
    Format = JsonFormat.NewlineDelimited,
    WriteIndented = false
};

// Resolver is optional - omit it to use the default file system resolver
var sinkNode = new JsonSinkNode<ProcessedUser>(
    StorageUri.FromFilePath("output.ndjson"),
    configuration: config);
```

**Expected `output.ndjson` Content:**

```json
{"id":1,"fullname":"Alice Smith","status":"Active"}
{"id":2,"fullname":"Bob Johnson","status":"Inactive"}
```

### Example: Writing Indented JSON

For human-readable JSON output, enable indentation:

```csharp
var config = new JsonConfiguration
{
    Format = JsonFormat.Array,
    WriteIndented = true,
    PropertyNamingPolicy = JsonPropertyNamingPolicy.CamelCase
};

// Resolver is optional - omit it to use the default file system resolver
var sinkNode = new JsonSinkNode<ProcessedUser>(
    StorageUri.FromFilePath("output.json"),
    configuration: config);
```

**Expected `output.json` Content:**

```json
[
  {
    "id": 1,
    "fullName": "Alice Smith",
    "status": "Active"
  },
  {
    "id": 2,
    "fullName": "Bob Johnson",
    "status": "Inactive"
  }
]
```

## Configuration Reference

### JsonConfiguration

The `JsonConfiguration` class provides comprehensive options for configuring JSON read and write operations.

#### Properties

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `BufferSize` | `int` | `4096` | Buffer size for stream operations in bytes. Larger buffers improve I/O performance but use more memory. |
| `Format` | `JsonFormat` | `JsonFormat.Array` | JSON format to use: Array or NewlineDelimited (NDJSON). |
| `WriteIndented` | `bool` | `false` | Whether to write indented JSON for human readability. |
| `PropertyNameCaseInsensitive` | `bool` | `true` | Case-insensitive property matching (aligns with CSV/Excel header behavior). |
| `PropertyNamingPolicy` | `JsonPropertyNamingPolicy` | `JsonPropertyNamingPolicy.LowerCase` | Attribute-less property naming convention. |
| `RowErrorHandler` | `Func<Exception, JsonRow, bool>?` | `null` | Optional error handler for row mapping errors in source node. |
| `SerializerOptions` | `JsonSerializerOptions` | (configured) | Underlying System.Text.Json options (read-only, configured based on other settings). |

### JsonFormat Enum

The `JsonFormat` enum specifies the format of JSON data when reading or writing.

| Value | Description | Example |
| --- | --- | --- |
| `Array` | JSON array format (most common) | `[{"id":1},{"id":2}]` |
| `NewlineDelimited` | NDJSON format (one JSON object per line) | `{"id":1}\n{"id":2}\n` |

### JsonPropertyNamingPolicy Enum

The `JsonPropertyNamingPolicy` enum specifies the naming policy for JSON property names when reading or writing.

| Value | Description | Example |
| --- | --- | --- |
| `LowerCase` | lowercase property names (default) | `{"firstname":"John"}` |
| `CamelCase` | camelCase property names | `{"firstName":"John"}` |
| `SnakeCase` | snake_case property names | `{"first_name":"John"}` |
| `PascalCase` | PascalCase property names | `{"FirstName":"John"}` |
| `AsIs` | Keep property names as-is | Uses property name from type |

## Advanced Configuration

### Buffer Size Configuration

The [`BufferSize`](../../src/NPipeline.Connectors.Json/JsonConfiguration.cs:46) property controls the internal buffer size for JSON I/O operations:

- **Default value**: 4096 bytes (4KB)
- **Purpose**: Determines the size of the buffer used for stream operations when reading or writing JSON files
- **Performance impact**: Larger buffers can improve I/O performance for large files but use more memory

When to adjust BufferSize:

- **Increase** (e.g., 8192, 16384) for:
  - Processing very large JSON files
  - High-throughput scenarios where I/O performance is critical
  - Systems with abundant memory resources
- **Decrease** (e.g., 2048, 1024) for:
  - Memory-constrained environments
  - Processing many small JSON files concurrently
  - Scenarios where memory usage must be tightly controlled

```csharp
// Example: Custom buffer size for large file processing
var largeFileConfig = new JsonConfiguration
{
    BufferSize = 8192, // 8KB buffer for better performance with large files
    Format = JsonFormat.Array,
    PropertyNameCaseInsensitive = true
};

// Resolver is optional - omit it to use the default file system resolver
var source = new JsonSourceNode<User>(
    StorageUri.FromFilePath("large_dataset.json"),
    row => new User(
        row.Get<int>("id") ?? 0,
        row.Get<string>("firstName") ?? string.Empty,
        row.Get<string>("lastName") ?? string.Empty,
        row.Get<string>("email") ?? string.Empty),
    configuration: largeFileConfig);
```

### Property Naming Policies

You can configure how property names are transformed between .NET types and JSON:

```csharp
// Example: Using camelCase naming (common in JavaScript APIs)
var camelCaseConfig = new JsonConfiguration
{
    PropertyNamingPolicy = JsonPropertyNamingPolicy.CamelCase,
    WriteIndented = true
};

// Example: Using snake_case naming (common in databases and APIs)
var snakeCaseConfig = new JsonConfiguration
{
    PropertyNamingPolicy = JsonPropertyNamingPolicy.SnakeCase,
    WriteIndented = true
};

// Example: Using PascalCase naming (common in .NET)
var pascalCaseConfig = new JsonConfiguration
{
    PropertyNamingPolicy = JsonPropertyNamingPolicy.PascalCase,
    WriteIndented = true
};
```

### Error Handling

The JSON connector provides flexible error handling for row mapping errors:

```csharp
var config = new JsonConfiguration
{
    RowErrorHandler = (ex, row) =>
    {
        // Log the error
        Console.WriteLine($"Warning: Failed to map row - {ex.Message}");
        
        // Return true to skip the row and continue processing
        // Return false or rethrow to fail the pipeline
        return true;
    }
};

// Resolver is optional - omit it to use the default file system resolver
var source = new JsonSourceNode<User>(
    StorageUri.FromFilePath("users.json"),
    row => new User(
        row.Get<int>("id") ?? 0,
        row.Get<string>("firstName") ?? string.Empty,
        row.Get<string>("lastName") ?? string.Empty,
        row.Get<string>("email") ?? string.Empty),
    configuration: config);
```

### Example: Transforming and Writing to JSON

This pipeline transforms user data and writes the result to a new JSON file.

```csharp
using NPipeline.Connectors.Json;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record UserSummary(string Name, string Domain);

public sealed class UserSummarizer : TransformNode<User, UserSummary>
{
    public override Task<UserSummary> ExecuteAsync(
        User item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var domain = item.Email.Split('@')[1];
        return Task.FromResult(new UserSummary(item.FirstName, domain));
    }
}

public sealed class JsonTransformPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Resolver is optional - default file system resolver is used automatically
        var source = builder.AddSource(new JsonSourceNode<User>(
            StorageUri.FromFilePath("users.json"),
            row => new User(
                row.Get<int>("id") ?? 0,
                row.Get<string>("firstName") ?? string.Empty,
                row.Get<string>("lastName") ?? string.Empty,
                row.Get<string>("email") ?? string.Empty)), "json_source");
        var transform = builder.AddTransform<UserSummarizer, User, UserSummary>("summarizer");
        var sinkNode = new JsonSinkNode<UserSummary>(StorageUri.FromFilePath("summaries.json"));
        var sink = builder.AddSink(sinkNode, "json_sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        var runner = PipelineRunner.Create();
        await runner.RunAsync<JsonTransformPipeline>();
    }
}
```

After running, this will create a `summaries.json` file with the following content:

```json
[{"name":"Alice","domain":"example.com"},{"name":"Bob","domain":"example.com"}]
```

## Supported Data Types

The JSON connector supports automatic type conversion for the following .NET types:

### Primitive Types

- `int`, `long`, `short` (and nullable variants)
- `float`, `double`, `decimal` (and nullable variants)
- `bool` (and nullable variant)

### String Types

- `string`

### Date/Time Types

- `DateTime` (and nullable variant)
- `DateTimeOffset` (and nullable variant)

### GUID Types

- `Guid` (and nullable variant)

### Type Conversion Behavior

- **Reading**: System.Text.Json attempts to convert JSON values to the target type automatically. If conversion fails, the property is skipped.
- **Writing**: Values are written with appropriate JSON types (Number, String, Boolean, DateTime). Complex types default to string representation.

## Format Support

### Reading

| Format | Extension | Library | Notes |
| --- | --- | --- | --- |
| JSON Array | `.json` | System.Text.Json | Standard JSON array format |
| NDJSON | `.ndjson`, `.jsonl` | System.Text.Json | Newline-delimited JSON, one object per line |

### Writing

| Format | Extension | Library | Notes |
| --- | --- | --- | --- |
| JSON Array | `.json` | System.Text.Json | Standard JSON array format |
| NDJSON | `.ndjson`, `.jsonl` | System.Text.Json | Newline-delimited JSON, one object per line |

## Performance Considerations

### Reading Performance

The `JsonSourceNode<T>` uses streaming access for memory-efficient processing of large JSON files:

- **Streaming**: Data is read row-by-row (or line-by-line for NDJSON), minimizing memory usage
- **Buffer Size**: Configure [`BufferSize`](../../src/NPipeline.Connectors.Json/JsonConfiguration.cs:46) to optimize I/O performance
- **Type Detection**: Automatic type detection via System.Text.Json
- **Format Handling**: Efficient parsing for both JSON Array and NDJSON formats

### Writing Performance

The `JsonSinkNode<T>` writes items as they arrive from the pipeline using streaming:

- **Memory Usage**: Items are written as they arrive, no buffering of entire dataset
- **Buffer Size**: Configure [`BufferSize`](../../src/NPipeline.Connectors.Json/JsonConfiguration.cs:46) to optimize I/O performance
- **Streaming**: Uses `Utf8JsonWriter` for efficient serialization
- **Format Support**: Both JSON Array and NDJSON formats supported

### Performance Optimization Tips

1. **Use appropriate buffer sizes**: Increase `BufferSize` for large files, decrease for memory-constrained environments
2. **Choose the right format**: Use NDJSON for streaming scenarios, JSON Array for structured data
3. **Disable indentation for production**: Set `WriteIndented = false` for smaller file size and faster parsing
4. **Use attribute-based mapping**: Leverage compiled delegates for optimal performance
5. **Profile your workload**: Test with representative data to identify performance bottlenecks

## Error Handling Patterns

### Row Mapping Errors

When a row fails to map during reading, the connector can:

- **Throw immediately** (default): Fail the pipeline
- **Handle via RowErrorHandler**: Skip the row and continue

```csharp
var config = new JsonConfiguration
{
    RowErrorHandler = (ex, row) =>
    {
        // Log the error with context
        Console.WriteLine($"Error mapping row: {ex.Message}");
        
        // Return true to skip, false to rethrow
        return true;
    }
};
```

### Serialization Errors

When an item fails to serialize during writing:

- **Throw immediately**: Fail the pipeline (default behavior)
- Errors are propagated to the pipeline's error handling system

### File Access Errors

When file access fails:

- **Throw descriptive exceptions**: Include file path and operation details
- **Use storage provider exceptions**: Leverage `IStorageProvider` exception types
- **Provide actionable error messages**: Help developers understand and fix issues

### JSON Parsing Errors

When JSON is malformed:

- **Throw JsonException**: Include position and context information
- **Provide line/column numbers**: Help locate the error in large files
- **Include snippet**: Show surrounding JSON for context

## Best Practices

### File Format

1. **Use JSON Array for structured data**: JSON Array is the most common format and is easier to work with
2. **Use NDJSON for streaming**: NDJSON is ideal for log files and streaming scenarios where records are independent
3. **Specify file extensions**: Always include the file extension (`.json` or `.ndjson`) in your `StorageUri`

### Configuration Guidelines

1. **Enable case-insensitive matching**: Set `PropertyNameCaseInsensitive = true` for better compatibility
2. **Use appropriate naming policy**: Choose a naming policy that matches your data source or consumer
3. **Adjust buffer size for large files**: Increase `BufferSize` for better I/O performance with large files
4. **Disable indentation for production**: Set `WriteIndented = false` for smaller file size

### Data Modeling

1. **Validate data types**: Ensure your model properties match the data types in your JSON files
2. **Use appropriate nullable types**: Handle missing or null data with nullable properties
3. **Use attributes for mapping**: Use `ColumnAttribute` for explicit property name mapping
4. **Consider string conversion**: For complex types, consider converting to strings in your model

### Error Handling

1. **Handle row mapping errors**: Implement `RowErrorHandler` for graceful error handling
2. **Validate data before writing**: Ensure data is valid and complete before passing to the sink node
3. **Monitor memory usage**: Be aware of memory consumption when processing large datasets
4. **Log errors appropriately**: Use logging to track and debug issues

### Performance

1. **Use streaming for large files**: Leverage the streaming capability of `JsonSourceNode<T>` for large files
2. **Choose appropriate format**: Use NDJSON for streaming, JSON Array for structured data
3. **Optimize buffer sizes**: Tune `BufferSize` based on your file sizes and system resources
4. **Profile your workload**: Test with representative data to identify performance bottlenecks

## Advanced Scenarios

### Reading Nested JSON Properties

The `JsonRow` struct supports nested property access using dot notation:

```csharp
public class Customer
{
    public int Id { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
}

// JSON with nested structure
var sourceNode = new JsonSourceNode<Customer>(
    StorageUri.FromFilePath("customers.json"),
    row => new Customer
    {
        Id = row.Get<int>("id") ?? 0,
        FirstName = row.GetNested<string>("name.first") ?? string.Empty,
        LastName = row.GetNested<string>("name.last") ?? string.Empty,
        Email = row.GetNested<string>("contact.email") ?? string.Empty,
        Phone = row.GetNested<string>("contact.phone") ?? string.Empty
    });
```

### Round-Trip Processing

Read from a JSON file, process the data, and write back to a new JSON file:

```csharp
public sealed class RoundTripPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Resolver is optional - default file system resolver is used automatically
        // Read from input file
        var source = builder.AddSource(
            new JsonSourceNode<User>(
                StorageUri.FromFilePath("input.json"),
                row => new User(
                    row.Get<int>("id") ?? 0,
                    row.Get<string>("firstName") ?? string.Empty,
                    row.Get<string>("lastName") ?? string.Empty,
                    row.Get<string>("email") ?? string.Empty),
                configuration: new JsonConfiguration { PropertyNameCaseInsensitive = true }
            ),
            "json_source"
        );

        // Process data
        var transform = builder.AddTransform<UserProcessor, User, User>("processor");

        // Write to output file
        var sink = builder.AddSink(
            new JsonSinkNode<User>(
                StorageUri.FromFilePath("output.json"),
                configuration: new JsonConfiguration 
                { 
                    WriteIndented = true,
                    PropertyNamingPolicy = JsonPropertyNamingPolicy.CamelCase
                }
            ),
            "json_sink"
        );

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

### Handling Mixed Data Types

When JSON properties have varying types, use nullable properties and default values:

```csharp
public class FlexibleRecord
{
    public int Id { get; set; }
    public string? Name { get; set; }
    public int? Age { get; set; }
    public decimal? Value { get; set; }
    public bool IsActive { get; set; }
}

var sourceNode = new JsonSourceNode<FlexibleRecord>(
    StorageUri.FromFilePath("mixed_data.json"),
    row => new FlexibleRecord
    {
        Id = row.Get<int>("id") ?? 0,
        Name = row.Get<string>("name"),
        Age = row.Get<int?>("age"),
        Value = row.Get<decimal?>("value"),
        IsActive = row.Get<bool>("isActive") ?? false
    });
```

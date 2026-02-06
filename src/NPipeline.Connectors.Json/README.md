# NPipeline JSON Connector

NPipeline JSON Connector provides source and sink nodes for reading and writing JSON files using System.Text.Json. This package enables seamless integration of JSON data processing into your NPipeline workflows with configurable parsing options and type-safe operations.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.Json
```

## Requirements

- **.NET 8.0, 9.0, or 10.0**
- **System.Text.Json 9.0.0+** (automatically included as a dependency)
- **NPipeline.Connectors** (automatically included as a dependency)

## Features

- **JSON Source Node**: Read JSON files and deserialize to strongly-typed objects
- **JSON Sink Node**: Serialize objects to JSON format and write to files
- **System.Text.Json Integration**: Leverages modern System.Text.Json library for reliable JSON processing
- **Multiple JSON Formats**: Support for both JSON array and newline-delimited JSON (NDJSON) formats
- **Configurable Options**: Customize naming policies, case sensitivity, and indentation
- **Type-Safe Operations**: Compile-time safety with generic type parameters
- **Storage Abstraction**: Works with pluggable storage providers for flexible file access
- **Streaming Processing**: Memory-efficient streaming for large JSON files
- **Row-Level Error Handling**: Opt-in handler to decide whether to skip or fail on mapping errors

## Configuration Options

### JsonConfiguration

The [`JsonConfiguration`](JsonConfiguration.cs:18) class provides configuration options for JSON operations:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BufferSize` | `int` | `4096` | Buffer size for stream operations |
| `Format` | `JsonFormat` | `Array` | JSON format (Array or NewlineDelimited) |
| `WriteIndented` | `bool` | `false` | Whether to format JSON output with indentation |
| `PropertyNameCaseInsensitive` | `bool` | `true` | Whether property name comparison is case-insensitive |
| `PropertyNamingPolicy` | `JsonPropertyNamingPolicy` | `LowerCase` | Naming policy for JSON property names |
| `RowErrorHandler` | `Func<Exception, JsonRow, bool>?` | `null` | Handler for row mapping errors |

### JsonFormat

The [`JsonFormat`](JsonFormat.cs:11) enum specifies the format of JSON data:

- **Array**: JSON data is structured as a JSON array containing JSON objects (default)
- **NewlineDelimited**: JSON data is structured as newline-delimited JSON (NDJSON)

### JsonPropertyNamingPolicy

The [`JsonPropertyNamingPolicy`](JsonPropertyNamingPolicy.cs:11) enum specifies the naming policy for JSON property names:

- **LowerCase**: Property names are converted to lowercase (default)
- **CamelCase**: Property names are converted to camelCase
- **SnakeCase**: Property names are converted to snake_case
- **PascalCase**: Property names are converted to PascalCase
- **AsIs**: Property names are used as-is without transformation

## JsonRow

The [`JsonRow`](JsonRow.cs:11) readonly struct provides efficient, read-only access to JSON object properties:

- `TryGet<T>(string name, out T? value, T? defaultValue = default)`: Try to read a property by name
- `Get<T>(string name, T? defaultValue = default)`: Read a property by name
- `HasProperty(string name)`: Check if a property exists
- `GetNested<T>(string path, T? defaultValue = default)`: Read a nested property using dot notation

## JsonMappingException

The [`JsonMappingException`](JsonMappingException.cs:11) is thrown when a JSON mapping error occurs, such as:

- A required property is missing from the JSON object
- A property value cannot be converted to the target type
- A nested property path is invalid or does not exist
- The JSON structure does not match the expected schema

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## License

MIT License - see LICENSE file for details.

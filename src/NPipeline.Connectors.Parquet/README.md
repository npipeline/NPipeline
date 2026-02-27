# NPipeline Parquet Connector

NPipeline Parquet Connector provides source and sink nodes for reading and writing Parquet files using Parquet.Net. This package enables high-performance,
columnar data processing in your NPipeline workflows with configurable row-group sizing, compression, and schema evolution support.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.Parquet
```

## Requirements

- **.NET 8.0, 9.0, or 10.0**
- **Parquet.Net 5.1.1+** (automatically included as a dependency)
- **NPipeline.Connectors** (automatically included as a dependency)
- **NPipeline.StorageProviders** (automatically included as a dependency)

## Features

- **Parquet Source Node**: Read Parquet files with streaming row-group processing
- **Parquet Sink Node**: Write Parquet files with configurable row-group sizing
- **Parquet.Net Integration**: Leverages the Parquet.Net library for efficient columnar I/O
- **Row-Group Streaming**: Memory-efficient processing of large files via row-group iteration
- **Configurable Compression**: Snappy (default), Gzip, or uncompressed
- **Schema Evolution**: Multiple compatibility modes for handling schema changes
- **Attribute-Based Mapping**: Declarative column mapping with `[ParquetColumn]` and `[ParquetDecimal]`
- **Explicit Row Mapping**: Full control via `Func<ParquetRow, T>` delegates
- **Column Projection**: Read only required columns to reduce I/O
- **Atomic Writes**: Optional temp-file pattern for crash consistency
- **Storage Abstraction**: Works with any `IStorageProvider` implementation
- **Observability**: Hook into file and row-group events via `IParquetConnectorObserver`

## Usage

### Reading Parquet Files with Attribute Mapping

```csharp
using NPipeline.Connectors.Parquet;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;

public class Transaction
{
    [ParquetColumn("transaction_id")]
    public long Id { get; set; }

    [ParquetColumn("customer_name")]
    public string CustomerName { get; set; } = string.Empty;

    [ParquetDecimal(18, 2)]
    public decimal Amount { get; set; }

    public DateTime TransactionDate { get; set; }
}

// Create a storage resolver
var resolver = StorageProviderFactory.CreateResolver();

// Read with automatic attribute-based mapping
var source = new ParquetSourceNode<Transaction>(
    StorageUri.FromFilePath("transactions.parquet"),
    resolver
);
```

**Why attribute mapping:** Attributes provide compile-time verification of column mappings and keep the mapping logic co-located with the model definition.
The [`ParquetColumnAttribute`](Attributes/ParquetColumnAttribute.cs:8) specifies the exact Parquet column name, while [
`ParquetDecimalAttribute`](Attributes/ParquetDecimalAttribute.cs:8) is required for decimal properties to define precision and scale.

### Reading Parquet Files with Explicit Row Mapping

```csharp
using NPipeline.Connectors.Parquet;

// Explicit mapping gives full control over type conversions and null handling
var source = new ParquetSourceNode<Transaction>(
    StorageUri.FromFilePath("transactions.parquet"),
    row => new Transaction
    {
        Id = row.Get<long>("transaction_id"),
        CustomerName = row.GetOrDefault("customer_name", string.Empty),
        Amount = row.Get<decimal>("amount"),
        TransactionDate = row.Get<DateTime>("transaction_date")
    },
    resolver
);
```

**When to use explicit mapping:** Use explicit mapping when you need custom type conversions, want to handle missing columns gracefully, or prefer keeping
mapping logic separate from the model.

### Writing Parquet Files

```csharp
using NPipeline.Connectors.Parquet;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.StorageProviders.Models;

public class Transaction
{
    [ParquetColumn("transaction_id")]
    public long Id { get; set; }

    [ParquetColumn("customer_name")]
    public string CustomerName { get; set; } = string.Empty;

    [ParquetDecimal(18, 2)]
    public decimal Amount { get; set; }

    // Ignored from Parquet output
    [ParquetColumn(Ignore = true)]
    public string InternalNotes { get; set; } = string.Empty;
}

var sink = new ParquetSinkNode<Transaction>(
    StorageUri.FromFilePath("output.parquet"),
    resolver
);
```

### Configuration Options

The [`ParquetConfiguration`](ParquetConfiguration.cs:9) class provides comprehensive control over Parquet operations:

| Property              | Type                                 | Default   | Description                                                                                                                  |
|-----------------------|--------------------------------------|-----------|------------------------------------------------------------------------------------------------------------------------------|
| `RowGroupSize`        | `int`                                | `50,000`  | Rows accumulated before flushing a row group. Larger values improve scan performance; smaller values reduce memory pressure. |
| `Compression`         | `CompressionMethod`                  | `Snappy`  | Compression codec for column chunks. Options: `Snappy`, `Gzip`, `None`.                                                      |
| `TargetFileSizeBytes` | `long?`                              | `256 MB`  | Target file size for partitioned writes. Writer rotates to a new file when threshold is reached. Set to `null` to disable.   |
| `UseAtomicWrite`      | `bool`                               | `true`    | Write to temporary path and atomically publish on success. Prevents partial-file visibility during failures.                 |
| `MaxBufferedRows`     | `int`                                | `250,000` | Maximum buffered rows across partition buffers. Protects memory during high-cardinality partition fan-out.                   |
| `ProjectedColumns`    | `IReadOnlyList<string>?`             | `null`    | Subset of columns to materialize. Pushes projection down to avoid I/O for unused columns.                                    |
| `SchemaValidator`     | `Func<ParquetSchema, bool>?`         | `null`    | Callback to validate file schema before reading. Return `false` to throw.                                                    |
| `SchemaCompatibility` | `SchemaCompatibilityMode`            | `Strict`  | Behavior when file schema and CLR model diverge.                                                                             |
| `RecursiveDiscovery`  | `bool`                               | `false`   | Scan nested directories when discovering Parquet files.                                                                      |
| `FileReadParallelism` | `int`                                | `1`       | Number of files read in parallel per source node.                                                                            |
| `RowFilter`           | `Func<ParquetRow, bool>?`            | `null`    | Best-effort row-group predicate applied using metadata, then validated at row level.                                         |
| `RowErrorHandler`     | `Func<Exception, ParquetRow, bool>?` | `null`    | Handler for row mapping errors. Return `true` to skip the row, `false` to fail.                                              |
| `Observer`            | `IParquetConnectorObserver?`         | `null`    | Observer for connector-level metrics and events.                                                                             |

### Configuration Example

```csharp
using NPipeline.Connectors.Parquet;
using Parquet;

var config = new ParquetConfiguration
{
    // Write options
    RowGroupSize = 100_000,
    Compression = CompressionMethod.Gzip,
    TargetFileSizeBytes = 512L * 1024 * 1024, // 512 MB
    UseAtomicWrite = true,
    MaxBufferedRows = 500_000,

    // Read options
    ProjectedColumns = ["transaction_id", "amount", "transaction_date"],
    SchemaCompatibility = SchemaCompatibilityMode.Additive,
    RecursiveDiscovery = true,
    FileReadParallelism = 4,

    // Error handling
    RowErrorHandler = (ex, row) =>
    {
        Console.WriteLine($"Row error: {ex.Message}");
        return true; // Skip the row
    },

    // Observability
    Observer = new ConsoleParquetObserver()
};

var source = new ParquetSourceNode<Transaction>(
    StorageUri.FromFilePath("data/"),
    resolver,
    config
);
```

## Schema Compatibility Modes

The [`SchemaCompatibilityMode`](SchemaCompatibilityMode.cs:6) enum controls behavior when the Parquet file schema and CLR model diverge:

| Mode       | Behavior                                                                                                                |
|------------|-------------------------------------------------------------------------------------------------------------------------|
| `Strict`   | All mapped fields must exist in the file and types must match exactly. Any mismatch throws an exception.                |
| `Additive` | Missing columns map to default values. Nullable properties may be set to `null`. Extra columns in the file are ignored. |
| `NameOnly` | Columns matched by name only. Allows compatible type coercions (e.g., `int` to `long`, `float` to `double`).            |

**Why multiple modes:** Different scenarios require different trade-offs. `Strict` ensures data integrity for critical pipelines. `Additive` supports schema
evolution where new columns are added over time. `NameOnly` provides flexibility when integrating with external systems that may use slightly different type
representations.

## Attribute Mapping

### ParquetColumnAttribute

The [`ParquetColumnAttribute`](Attributes/ParquetColumnAttribute.cs:8) controls how properties map to Parquet columns:

```csharp
public class Record
{
    // Explicit column name
    [ParquetColumn("cust_id")]
    public int CustomerId { get; set; }

    // Use property name as column name (explicit opt-in)
    [ParquetColumn]
    public string Name { get; set; } = string.Empty;

    // Exclude from Parquet mapping
    [ParquetColumn(Ignore = true)]
    public string ComputedField { get; set; } = string.Empty;
}
```

### ParquetDecimalAttribute

The [`ParquetDecimalAttribute`](Attributes/ParquetDecimalAttribute.cs:8) is **required** for decimal properties when writing:

```csharp
public class FinancialRecord
{
    [ParquetDecimal(precision: 18, scale: 2)]
    public decimal Amount { get; set; }

    [ParquetDecimal(precision: 28, scale: 8)]
    public decimal ExchangeRate { get; set; }
}
```

**Why required:** Parquet requires explicit precision and scale for decimal columns. The attribute ensures the schema is correctly defined at write time and
prevents runtime errors.

## ParquetRow

The [`ParquetRow`](ParquetRow.cs:9) class provides typed access to Parquet row data:

```csharp
// Typed accessors
long id = row.Get<long>("transaction_id");
string name = row.GetOrDefault("customer_name", "Unknown");
decimal? amount = row.GetOrDefault<decimal?>("amount", null);

// Check for null
if (row.IsNull("optional_field"))
{
    // Handle null case
}

// Check column existence
if (row.HasColumn("legacy_field"))
{
    // Handle legacy data
}

// TryGet pattern
if (row.TryGet("discount", out decimal? discount))
{
    // discount has a value
}
```

## Observability

Implement [`IParquetConnectorObserver`](IParquetConnectorObserver.cs:9) to monitor Parquet operations:

```csharp
public class LoggingParquetObserver : IParquetConnectorObserver
{
    public void OnFileReadStarted(StorageUri uri)
        => Console.WriteLine($"Reading: {uri.Path}");

    public void OnFileReadCompleted(StorageUri uri, long rows, long bytes, TimeSpan elapsed)
        => Console.WriteLine($"Read {rows:N0} rows ({bytes:N0} bytes) in {elapsed.TotalMilliseconds:N0}ms");

    public void OnFileWriteCompleted(StorageUri uri, long rows, long bytes, TimeSpan elapsed)
        => Console.WriteLine($"Wrote {rows:N0} rows in {elapsed.TotalMilliseconds:N0}ms");

    public void OnRowGroupRead(StorageUri uri, int rowGroupIndex, long rowCount)
        => Console.WriteLine($"Row group {rowGroupIndex}: {rowCount:N0} rows");

    public void OnRowGroupWritten(StorageUri uri, int rowGroupIndex, long rowCount)
        => Console.WriteLine($"Wrote row group {rowGroupIndex}: {rowCount:N0} rows");

    public void OnRowMappingError(StorageUri uri, Exception exception)
        => Console.WriteLine($"Mapping error in {uri.Path}: {exception.Message}");
}
```

**Why observability:** Production pipelines need visibility into I/O operations for performance tuning and troubleshooting. The observer pattern allows
integration with logging frameworks, OpenTelemetry, or custom metrics systems without coupling the connector to any specific implementation.

## Supported Types

| CLR Type              | Parquet Type        | Notes                                 |
|-----------------------|---------------------|---------------------------------------|
| `string`              | `STRING`            | UTF-8 encoded                         |
| `int`                 | `INT32`             | 32-bit signed integer                 |
| `long`                | `INT64`             | 64-bit signed integer                 |
| `short`               | `INT32`             | 16-bit signed integer                 |
| `byte`                | `INT32`             | 8-bit unsigned integer                |
| `float`               | `FLOAT`             | IEEE 754 single-precision             |
| `double`              | `DOUBLE`            | IEEE 754 double-precision             |
| `bool`                | `BOOLEAN`           | Boolean flag                          |
| `decimal`             | `DECIMAL`           | Requires `[ParquetDecimal]` attribute |
| `DateTime`            | `INT64` (timestamp) | Stored as UTC ticks                   |
| `DateTimeOffset`      | `INT64` (timestamp) | Converted to UTC DateTime             |
| `byte[]`              | `BYTE_ARRAY`        | Binary data                           |
| `Guid`                | `STRING`            | Stored as formatted string            |
| `int?`, `long?`, etc. | Optional            | Nullable value types                  |

## Performance Considerations

### Row-Group Sizing

Row groups are the unit of I/O in Parquet. The default `RowGroupSize` of 50,000 rows balances:

- **Scan performance**: Larger row groups mean fewer seeks during columnar scans
- **Memory usage**: Entire row groups are buffered during writes
- **Compression ratio**: Larger row groups improve compression effectiveness

For memory-constrained environments, reduce `RowGroupSize` to 10,000-25,000 rows. For analytical workloads with ample memory, increase to 100,000-1,000,000
rows.

### Compression

- **Snappy** (default): Fast compression/decompression, moderate compression ratio. Best for most workloads.
- **Gzip**: Higher compression ratio, slower. Best for cold storage or network-constrained environments.
- **None**: No compression. Best when data is already compressed or CPU is the bottleneck.

### Column Projection

Use `ProjectedColumns` to read only required columns:

```csharp
var config = new ParquetConfiguration
{
    ProjectedColumns = ["id", "amount"] // Only read 2 of 50 columns
};
```

This pushes projection down to the row-group reader, avoiding I/O for unused columns entirely.

### Atomic Writes

When `UseAtomicWrite` is enabled (default), the sink writes to a temporary file and atomically renames on success. This prevents:

- Partial files being visible after crashes
- Readers seeing inconsistent data during writes
- Corrupt files from interrupted writes

Disable only when the storage provider doesn't support atomic operations or when write latency is critical.

## Storage Provider Compatibility

The Parquet connector works with any `IStorageProvider` implementation:

- **Local File System**: `file://` URIs with `LocalStorageProvider`
- **Azure Blob Storage**: `azure://` or `wasbs://` URIs
- **AWS S3**: `s3://` URIs
- **Google Cloud Storage**: `gs://` URIs
- **Custom Providers**: Implement `IStorageProvider` for other storage systems

```csharp
// The resolver automatically selects the appropriate provider based on URI scheme
var resolver = StorageProviderFactory.CreateResolver();
var uri = StorageUri.Parse("s3://my-bucket/data/sales.parquet");
var source = new ParquetSourceNode<SalesRecord>(uri, resolver);
```

## Complete Pipeline Example

```csharp
using NPipeline.Connectors.Parquet;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;

public class SalesRecord
{
    [ParquetColumn("sale_id")]
    public long Id { get; set; }

    [ParquetColumn("product_name")]
    public string ProductName { get; set; } = string.Empty;

    [ParquetDecimal(18, 2)]
    public decimal Amount { get; set; }

    public DateTime SaleDate { get; set; }

    [ParquetColumn(Ignore = true)]
    public string InternalCode { get; set; } = string.Empty;
}

public class SalesPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var resolver = StorageProviderFactory.CreateResolver();

        // Source: Read partitioned Parquet files from a directory
        var source = builder.AddSource(
            new ParquetSourceNode<SalesRecord>(
                StorageUri.FromFilePath("data/sales/"),
                resolver,
                new ParquetConfiguration
                {
                    RecursiveDiscovery = true,
                    SchemaCompatibility = SchemaCompatibilityMode.Additive
                }),
            "parquet-source");

        // Transform: Add processing logic
        var transform = builder.AddTransform<SalesTransform, SalesRecord, SalesRecord>("transform");

        // Sink: Write to output Parquet file
        var sink = builder.AddSink(
            new ParquetSinkNode<SalesRecord>(
                StorageUri.FromFilePath("output/processed_sales.parquet"),
                resolver,
                new ParquetConfiguration
                {
                    RowGroupSize = 100_000,
                    Compression = Parquet.CompressionMethod.Gzip
                }),
            "parquet-sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Connectors.DataLake](https://www.nuget.org/packages/NPipeline.Connectors.DataLake)** - Data Lake table abstractions built on this connector
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## License

MIT License - see LICENSE file for details.

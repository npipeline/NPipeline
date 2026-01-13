---
title: Lineage Configuration
description: Detailed guide to configuring NPipeline Lineage extension with sampling, redaction, and overflow policies.
---

# Lineage Configuration

This guide covers all configuration options for the NPipeline Lineage extension, including sampling strategies, data redaction, overflow policies, and custom sink registration.

## LineageOptions

The [`LineageOptions`](../../../src/NPipeline.Extensions.Lineage/LineageOptions.cs) class controls item-level lineage tracking behavior:

```csharp
public sealed record LineageOptions
{
    /// Sample every Nth item (1 = sample all items)
    int SampleEvery { get; init; } = 1;
    
    /// Use deterministic (hash-based) sampling vs random sampling
    bool DeterministicSampling { get; init; } = true;
    
    /// Redact actual data from lineage records (store only metadata)
    bool RedactData { get; init; } = false;
    
    /// Maximum number of items to materialize in memory
    int MaterializationCap { get; init; } = 10000;
    
    /// Behavior when materialization cap is reached
    LineageOverflowPolicy OverflowPolicy { get; init; } = LineageOverflowPolicy.Degrade;
}
```

## Sampling Configuration

### Deterministic Sampling

Deterministic sampling uses a hash-based approach to select items consistently across runs:

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 100;  // Sample 1% of items
    options.DeterministicSampling = true;
});
```

**When to Use:**

- Debugging specific issues (same items sampled across runs)
- Compliance scenarios requiring consistent tracking
- Reproducible testing

**How It Works:**

- Computes hash of item's lineage ID
- Items with hash % SampleEvery == 0 are sampled
- Same items always selected across multiple runs
- Provides predictable, repeatable behavior

### Random Sampling

Random sampling selects items at the specified rate without consistency across runs:

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 100;  // Sample 1% of items
    options.DeterministicSampling = false;
});
```

**When to Use:**

- Monitoring and analytics (representative samples)
- High-volume pipelines where consistency isn't required
- Reducing overhead with minimal bias

**How It Works:**

- Uses random number generator for each item
- Items selected with probability 1/SampleEvery
- Different items may be sampled across runs
- Provides statistically representative samples

### Sampling Rate Guidelines

| Scenario | Recommended Rate | Reasoning |
|-----------|------------------|-------------|
| Production compliance | 100% (SampleEvery = 1) | Complete audit trails required |
| Production monitoring | 1-10% (SampleEvery = 10-100) | Balance visibility and overhead |
| Development/debugging | 10-50% (SampleEvery = 2-10) | Good visibility with manageable overhead |
| High-volume analytics | 0.1-1% (SampleEvery = 100-1000) | Representative samples, minimal overhead |

## Data Redaction

Redaction excludes actual data from lineage records, storing only metadata:

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.RedactData = true;
});
```

**When to Use:**

- Sensitive data (PII, financial data, health records)
- Large data objects (reduces memory usage)
- Focus on flow patterns rather than data values

**Impact:**

- `LineageInfo.Data` field will be `null`
- All other lineage metadata preserved
- Reduces memory usage by not storing actual data
- No impact on tracking accuracy

**Example:**

```csharp
// Without redaction
var lineageInfo = collector.GetLineageInfo(lineageId);
Console.WriteLine(lineageInfo.Data);  // Outputs actual data

// With redaction
var lineageInfo = collector.GetLineageInfo(lineageId);
Console.WriteLine(lineageInfo.Data);  // Outputs: null
Console.WriteLine(lineageInfo.LineageId);  // Still available
Console.WriteLine(lineageInfo.TraversalPath);  // Still available
```

## Overflow Policies

Overflow policies control behavior when the materialization cap is reached:

### Materialization Cap

The default cap limits memory usage by materializing only a subset of items:

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.MaterializationCap = 10000;  // Default
});
```

**Adjust Based On:**

- Available memory
- Pipeline throughput
- Required visibility
- Sampling rate

### Overflow Policy Options

#### Materialize

Continues collecting all lineage data in memory:

```csharp
options.OverflowPolicy = LineageOverflowPolicy.Materialize;
```

**Characteristics:**

- No limit on in-memory lineage storage
- Complete lineage visibility
- Risk of out-of-memory errors on large datasets

**When to Use:**

- Development and debugging
- Small datasets
- Memory-constrained environments with guaranteed capacity

#### Degrade (Default)

Switches to streaming mode when cap is reached:

```csharp
options.OverflowPolicy = LineageOverflowPolicy.Degrade;  // Default
```

**Characteristics:**

- New items beyond cap are streamed to sinks
- Older items may be removed from in-memory collection
- Lineage is complete but older items not queryable
- Best balance of visibility and memory safety

**When to Use:**

- Production pipelines with sampling
- Compliance scenarios requiring persistence
- Most production use cases

#### Drop

Stops collecting lineage when cap is reached:

```csharp
options.OverflowPolicy = LineageOverflowPolicy.Drop;
```

**Characteristics:**

- No lineage for items beyond cap
- Minimal memory footprint
- Partial visibility

**When to Use:**

- High-volume monitoring only
- Memory-constrained environments
- Scenarios where complete lineage isn't required

### Choosing an Overflow Policy

| Scenario | Policy | Reasoning |
|-----------|----------|-------------|
| Production pipelines with sampling | Degrade | Safe default, maintains visibility |
| Development/debugging | Materialize | Complete information useful for investigation |
| High-volume monitoring | Drop | Prevents memory issues with sampling |
| Memory-constrained environments | Drop | Minimal memory footprint |
| Compliance/audit scenarios | Degrade | Ensures records are persisted to sinks |

## Custom Sink Registration

Register custom lineage sinks to export data to external systems:

### Built-in Logging Sink

```csharp
builder.UseLoggingPipelineLineageSink();
```

### Custom ILineageSink

Implement for item-level lineage export:

```csharp
public sealed class DatabaseLineageSink : ILineageSink
{
    private readonly IDbConnection _connection;

    public DatabaseLineageSink(IDbConnection connection)
    {
        _connection = connection;
    }

    public async Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
    {
        const string sql = @"
            INSERT INTO Lineage (LineageId, Data, TraversalPath, Timestamp)
            VALUES (@LineageId, @Data, @TraversalPath, @Timestamp)";
        
        await _connection.ExecuteAsync(sql, new
        {
            LineageId = lineageInfo.LineageId,
            Data = lineageInfo.Data?.ToString(),
            TraversalPath = string.Join(",", lineageInfo.TraversalPath),
            Timestamp = DateTime.UtcNow
        }, cancellationToken);
    }
}
```

### Custom IPipelineLineageSink

Implement for pipeline-level lineage export:

```csharp
public sealed class JsonFileLineageSink : IPipelineLineageSink
{
    private readonly string _filePath;

    public JsonFileLineageSink(string filePath)
    {
        _filePath = filePath;
    }

    public async Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(report, new JsonSerializerOptions
        {
            WriteIndented = true
        });
        
        await File.WriteAllTextAsync(_filePath, json, cancellationToken);
    }
}
```

### Register Custom Sinks via DI

```csharp
// Register with custom sink type
services.AddNPipelineLineage<DatabaseLineageSink>();

// Register with factory delegate
services.AddNPipelineLineage(sp =>
{
    var connectionString = sp.GetRequiredService<IConfiguration>()["ConnectionStrings:Lineage"];
    var connection = new SqlConnection(connectionString);
    return new DatabaseLineageSink(connection);
});

// Register both item and pipeline sinks
services.AddNPipelineLineage<DatabaseLineageSink, JsonFileLineageSink>();
```

## Dependency Injection Configuration

### Default Registration

```csharp
services.AddNPipelineLineage();
```

**Registers:**

- `ILineageCollector` (scoped)
- `ILineageSink` → `LoggingPipelineLineageSink` (transient)
- `IPipelineLineageSink` → `LoggingPipelineLineageSink` (transient)

### Custom Sink Registration

```csharp
services.AddNPipelineLineage<CustomLineageSink>();
```

### Factory-Based Registration

```csharp
services.AddNPipelineLineage(sp =>
{
    var logger = sp.GetRequiredService<ILogger<CustomLineageSink>>();
    var config = sp.GetRequiredService<IConfiguration>();
    return new CustomLineageSink(logger, config);
});
```

### Custom Collector Registration

```csharp
services.AddNPipelineLineage<CustomCollector, LoggingPipelineLineageSink, LoggingPipelineLineageSink>();
```

## Configuration Examples

### Production-Ready Configuration

```csharp
services.AddNPipelineLineage<DatabaseLineageSink, JsonFileLineageSink>();

// In pipeline
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 100;  // 1% sampling
    options.DeterministicSampling = true;
    options.RedactData = true;  // Don't store sensitive data
    options.MaterializationCap = 10000;
    options.OverflowPolicy = LineageOverflowPolicy.Degrade;
});
```

### Development Configuration

```csharp
services.AddNPipelineLineage();  // Use logging sink

// In pipeline
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 1;  // Track everything
    options.DeterministicSampling = true;
    options.RedactData = false;  // Keep data for debugging
    options.MaterializationCap = int.MaxValue;  // No cap
    options.OverflowPolicy = LineageOverflowPolicy.Materialize;
});
```

### High-Volume Monitoring Configuration

```csharp
services.AddNPipelineLineage<PrometheusLineageSink>();

// In pipeline
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 1000;  // 0.1% sampling
    options.DeterministicSampling = false;  // Random sampling
    options.RedactData = true;  // Minimal memory
    options.MaterializationCap = 1000;  // Small cap
    options.OverflowPolicy = LineageOverflowPolicy.Drop;  // Drop excess
});
```

## Best Practices

### 1. Start with Conservative Sampling

Begin with 1-10% sampling in production:

```csharp
options.SampleEvery = 100;  // Conservative start
```

Adjust based on requirements and performance impact.

### 2. Use Deterministic Sampling for Debugging

When investigating specific issues:

```csharp
options.DeterministicSampling = true;
options.SampleEvery = 1;  // Track all items temporarily
```

### 3. Enable Redaction for Sensitive Data

Always redact PII, financial data, or health records:

```csharp
options.RedactData = true;
```

### 4. Use Degrade Policy in Production

Default policy provides best balance:

```csharp
options.OverflowPolicy = LineageOverflowPolicy.Degrade;
```

### 5. Implement Async Sinks

Use async operations in custom sinks:

```csharp
public async Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
{
    await _database.SaveChangesAsync(cancellationToken);
}
```

## Related Topics

- **[Getting Started](./getting-started.md)** - Installation and basic setup
- **[Architecture](./architecture.md)** - Internal architecture and design decisions
- **[Performance](./performance.md)** - Performance characteristics and optimization
- **[Use Cases](./use-cases.md)** - Common use cases and examples

---
title: Lineage Extension Sample
description: Comprehensive sample demonstrating data lineage tracking capabilities in NPipeline.
sidebar_position: 7
slug: /samples/lineage
---

# Lineage Extension Sample

The [Sample_LineageExtension](../../../samples/Sample_LineageExtension/) project demonstrates the data lineage tracking capabilities of the NPipeline.Extensions.Lineage extension. This comprehensive sample showcases various lineage tracking features including basic tracking, sampling strategies, complex pipelines with joins, branching, error handling, and custom lineage sinks.

## Overview

The sample demonstrates a realistic e-commerce order processing pipeline with multiple scenarios:

- **Basic Lineage Tracking**: Track data as it flows through each node in a simple pipeline
- **Deterministic Sampling**: Sample every Nth item to reduce overhead while maintaining consistency
- **Random Sampling**: Sample approximately N% of items for representative data
- **Complex Join Pipeline**: Track lineage across multi-source joins
- **Branching with Lineage**: Track items through branching and recombining paths
- **Error Handling with Lineage**: Track validation failures and processing errors
- **Custom Lineage Sink**: Implement custom sinks for exporting lineage data

## Running the Sample

### Prerequisites

- .NET 8.0, 9.0, or 10.0 SDK
- The NPipeline project built and available

### Building

```bash
dotnet build samples/Sample_LineageExtension/Sample_LineageExtension.csproj
```

### Running Scenarios

The sample supports multiple scenarios that can be run individually:

```bash
# Run basic lineage tracking (default)
dotnet run --project samples/Sample_LineageExtension/Sample_LineageExtension.csproj

# Run specific scenario
dotnet run --project samples/Sample_LineageExtension/Sample_LineageExtension.csproj BasicLineageTracking
dotnet run --project samples/Sample_LineageExtension/Sample_LineageExtension.csproj DeterministicSampling
dotnet run --project samples/Sample_LineageExtension/Sample_LineageExtension.csproj RandomSampling
dotnet run --project samples/Sample_LineageExtension/Sample_LineageExtension.csproj ComplexJoin
dotnet run --project samples/Sample_LineageExtension/Sample_LineageExtension.csproj BranchingWithLineage
dotnet run --project samples/Sample_LineageExtension/Sample_LineageExtension.csproj ErrorHandlingWithLineage
dotnet run --project samples/Sample_LineageExtension/Sample_LineageExtension.csproj CustomLineageSink
```

## Scenario Details

### 1. Basic Lineage Tracking

**Purpose**: Demonstrates fundamental lineage tracking through a simple linear pipeline.

**Pipeline Structure**:

```
OrderSource → EnrichmentTransform → ValidationTransform → ProcessingTransform → ConsoleSink
```

**What It Demonstrates**:

- How to enable lineage tracking with default settings
- Tracking individual items through each node
- Recording traversal paths and hop details
- Accessing collected lineage information after pipeline execution

**Key Code**:

```csharp
// Enable lineage tracking (all items tracked)
services.AddNPipelineLineage();

// In pipeline definition
builder.EnableItemLevelLineage();
```

**Adapting for Your Use Case**:

- Replace [`OrderSource`](../../../samples/Sample_LineageExtension/Nodes/OrderSource.cs:12) with your own source node
- Replace transform nodes with your business logic
- Adjust the sink to write to your destination (database, API, file system)
- Use [`ILineageCollector.GetAllLineageInfo()`](../../../src/NPipeline.Extensions.Lineage/LineageCollector.cs:106) to access lineage data after execution

**When to Use This Pattern**:

- Simple linear pipelines
- Development and testing environments
- When you need complete visibility into data flow
- For debugging and troubleshooting

### 2. Deterministic Sampling

**Purpose**: Demonstrates how to sample every Nth item to reduce overhead while maintaining consistent behavior across runs.

**Pipeline Structure**:

```
OrderSource → EnrichmentTransform → ValidationTransform → ProcessingTransform → ConsoleSink
```

**What It Demonstrates**:

- Configuring deterministic sampling rate (every 3rd item)
- Consistent sampling across multiple runs (same items always sampled)
- Reducing lineage collection overhead in production
- Using custom lineage sinks with file output

**Key Code**:

```csharp
// Sample every 3rd item deterministically
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 3;
    options.DeterministicSampling = true;
});
```

**Why Deterministic Sampling Matters**:

- **Consistency**: The same items are always sampled across runs, making debugging reproducible
- **Predictability**: You know exactly which items will have lineage data
- **Compliance**: Consistent sampling meets audit trail requirements
- **Debugging**: When investigating issues, you can reliably trace the same items

**Adapting for Your Use Case**:

- Adjust `SampleEvery` based on your throughput requirements:
  - Low throughput (1000 items/sec): `SampleEvery = 10` (10%)
  - Medium throughput (10000 items/sec): `SampleEvery = 100` (1%)
  - High throughput (100000+ items/sec): `SampleEvery = 1000` (0.1%)
- Use deterministic sampling when you need reproducible behavior
- Combine with custom sinks to export sampled lineage data

**When to Use This Pattern**:

- Production environments with high throughput
- When you need consistent sampling for compliance
- Debugging specific issues that require reproducible traces
- Capacity planning and performance analysis

### 3. Random Sampling

**Purpose**: Demonstrates random sampling (~N% of items) for representative data with minimal overhead.

**Pipeline Structure**:

```
OrderSource → EnrichmentTransform → ValidationTransform → ProcessingTransform → ConsoleSink
```

**What It Demonstrates**:

- Configuring random sampling rate (~33% of items)
- Representative sampling without deterministic constraints
- Lower overhead than full tracking
- Using custom sinks for JSON file output

**Key Code**:

```csharp
// Sample approximately 33% of items randomly
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 3;
    options.DeterministicSampling = false;
});
```

**Why Random Sampling Matters**:

- **Representative**: Provides a statistically representative sample of all items
- **Unbiased**: No systematic bias in which items are sampled
- **Analytics**: Suitable for statistical analysis and monitoring
- **Performance**: Minimal overhead with good visibility

**Adapting for Your Use Case**:

- Adjust sampling rate based on your needs:
  - High visibility: `SampleEvery = 2` (50%)
  - Balanced: `SampleEvery = 10` (10%)
  - Low overhead: `SampleEvery = 100` (1%)
- Use random sampling for monitoring and analytics
- Combine with aggregation sinks for statistical analysis

**When to Use This Pattern**:

- Production monitoring and analytics
- When you need representative samples without deterministic constraints
- Performance monitoring and trend analysis
- Capacity planning based on sampled data

### 4. Complex Join Pipeline

**Purpose**: Demonstrates lineage tracking across multi-source joins where data from multiple sources is combined.

**Pipeline Structure**:

```
OrderSource → OrderCustomerJoinNode → ValidationTransform → ProcessingTransform → ConsoleSink
CustomerSource ↗
```

**What It Demonstrates**:

- Tracking lineage when combining data from multiple sources
- Maintaining provenance information through join operations
- Recording ancestry input indices for joined items
- Understanding cardinality changes (1:1, 1:N, N:1, N:M)

**Key Code**:

```csharp
// Join node combines orders with customer data
public class OrderCustomerJoinNode : SourceNode<EnrichedOrder>
{
    // Each enriched order contains both order and customer data
    // Lineage tracks the provenance of both sources
}
```

**Why Join Lineage Matters**:

- **Provenance**: Know which customer record was joined with each order
- **Debugging**: Identify if issues originate from source data or join logic
- **Impact Analysis**: Understand how changes to customer data affect orders
- **Data Quality**: Track which customer records are most frequently used

**Adapting for Your Use Case**:

- Replace [`OrderCustomerJoinNode`](../../../samples/Sample_LineageExtension/Nodes/OrderCustomerJoinNode.cs:12) with your join logic
- Add additional sources as needed (product data, inventory, pricing)
- Implement different join strategies (inner, left, right, full outer)
- Use lineage to analyze join patterns and identify bottlenecks

**When to Use This Pattern**:

- Pipelines that combine data from multiple sources
- Data enrichment scenarios
- When you need to track data provenance through joins
- For debugging join-related issues

### 5. Branching with Lineage

**Purpose**: Demonstrates lineage tracking through branching and recombining paths.

**Pipeline Structure**:

```
OrderSource → EnrichmentTransform → FraudDetectionBranch → ValidationTransform → ProcessingTransform → ConsoleSink
                                           ↓ (fraud path)
                                      FraudHandlingSink
```

**What It Demonstrates**:

- Tracking items as they split into different branches
- Recording decision outcomes at branch points
- Maintaining lineage through multiple paths
- Understanding which items took which path

**Key Code**:

```csharp
// Branch node routes orders based on fraud detection
public class FraudDetectionBranch : TransformNode<EnrichedOrder, EnrichedOrder>
{
    // Fraudulent orders are routed to separate sink
    // Lineage tracks the decision and path taken
}
```

**Why Branching Lineage Matters**:

- **Decision Tracking**: Know which branch each item took and why
- **Debugging**: Identify if items are being routed incorrectly
- **Flow Analysis**: Understand the distribution of items across branches
- **Error Isolation**: Track which branch introduced issues

**Adapting for Your Use Case**:

- Replace [`FraudDetectionBranch`](../../../samples/Sample_LineageExtension/Nodes/FraudDetectionBranch.cs:10) with your branching logic
- Add additional branches as needed (priority routing, geographic routing, etc.)
- Use lineage to analyze branch distribution and optimize routing
- Track decision outcomes for compliance and auditing

**When to Use This Pattern**:

- Pipelines with conditional routing
- When you need to track which path items take
- For debugging routing issues
- Compliance scenarios requiring decision tracking

### 6. Error Handling with Lineage

**Purpose**: Demonstrates lineage tracking for error outcomes and retry scenarios.

**Pipeline Structure**:

```
OrderSource → EnrichmentTransform → ValidationTransform → ProcessingTransform → ConsoleSink
                                                                               ↓ (errors)
                                                                          DatabaseSink
```

**What It Demonstrates**:

- Tracking validation failures and processing errors
- Recording error outcomes in lineage hops
- Understanding which items failed and where
- Analyzing error patterns and root causes

**Key Code**:

```csharp
// Validation transform records validation errors
public class ValidationTransform : TransformNode<EnrichedOrder, ValidatedOrder>
{
    // Validation failures are recorded in lineage
    // Error outcomes are tracked for debugging
}
```

**Why Error Lineage Matters**:

- **Root Cause Analysis**: Quickly identify which node introduced errors
- **Error Tracking**: Maintain complete record of all failures
- **Debugging**: Trace errors back to their source
- **Compliance**: Document all processing failures for audits

**Adapting for Your Use Case**:

- Replace [`ValidationTransform`](../../../samples/Sample_LineageExtension/Nodes/ValidationTransform.cs:10) with your validation logic
- Add error handling nodes for different types of errors
- Use lineage to analyze error patterns and identify common issues
- Implement retry logic with lineage tracking for retry attempts

**When to Use This Pattern**:

- Pipelines with validation and error handling
- When you need to track and analyze errors
- For debugging production issues
- Compliance scenarios requiring error documentation

### 7. Custom Lineage Sink

**Purpose**: Demonstrates implementing a custom [`IPipelineLineageSink`](../../../src/NPipeline.Extensions.Lineage/LoggingPipelineLineageSink.cs:10) for exporting lineage data.

**Pipeline Structure**:

```
OrderSource → EnrichmentTransform → ValidationTransform → ProcessingTransform → ConsoleSink
```

**What It Demonstrates**:

- Implementing custom lineage sinks for specific storage requirements
- Exporting lineage data to JSON files
- Displaying lineage information to console
- Providing summary statistics

**Key Code**:

```csharp
// Custom sink exports lineage to JSON file
public class CustomLineageSink : IPipelineLineageSink
{
    public async Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
    {
        // Export to JSON, database, or external service
        var json = JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(_filePath, json, cancellationToken);
    }
}
```

**Why Custom Sinks Matter**:

- **Flexibility**: Store lineage data in any format or destination
- **Integration**: Connect to existing monitoring and analytics systems
- **Custom Analysis**: Implement domain-specific lineage analysis
- **Performance**: Optimize storage for your specific use case

**Adapting for Your Use Case**:

- Implement sinks for your storage needs:
  - **Databases**: Store lineage in SQL or NoSQL databases
  - **Message Queues**: Publish lineage events to Kafka, RabbitMQ, etc.
  - **External Services**: Send to monitoring platforms (Datadog, New Relic, etc.)
  - **File Systems**: Export to CSV, Parquet, or custom formats
- Add domain-specific analysis and aggregation
- Implement filtering and transformation of lineage data

**When to Use This Pattern**:

- When you need to store lineage data in a specific format
- For integration with existing monitoring systems
- When you need custom lineage analysis
- Production environments with specific storage requirements

## Domain Models

The sample uses realistic e-commerce domain models that you can adapt to your use case:

### OrderEvent

Represents an order with customer, product, and payment information.

**Key Properties**:

- [`OrderId`](../../../samples/Sample_LineageExtension/Models.cs:12): Unique order identifier
- [`CustomerId`](../../../samples/Sample_LineageExtension/Models.cs:17): Customer identifier
- [`ProductId`](../../../samples/Sample_LineageExtension/Models.cs:22): Product identifier
- [`Quantity`](../../../samples/Sample_LineageExtension/Models.cs:27): Number of items ordered
- [`UnitPrice`](../../../samples/Sample_LineageExtension/Models.cs:32): Price per unit
- [`TotalAmount`](../../../samples/Sample_LineageExtension/Models.cs:37): Calculated total (quantity × unit price)
- [`OrderDate`](../../../samples/Sample_LineageExtension/Models.cs:42): When the order was placed
- [`Status`](../../../samples/Sample_LineageExtension/Models.cs:47): Current order status
- [`PaymentMethod`](../../../samples/Sample_LineageExtension/Models.cs:57): How payment was made
- [`IsFlaggedForFraud`](../../../samples/Sample_LineageExtension/Models.cs:62): Whether fraud is suspected

### CustomerData

Customer profile with loyalty tier and lifetime value.

**Key Properties**:

- [`CustomerId`](../../../samples/Sample_LineageExtension/Models.cs:100): Unique customer identifier
- [`FullName`](../../../samples/Sample_LineageExtension/Models.cs:105): Customer's name
- [`Email`](../../../samples/Sample_LineageExtension/Models.cs:110): Contact email
- [`LoyaltyTier`](../../../samples/Sample_LineageExtension/Models.cs:120): Bronze, Silver, Gold, or Platinum
- [`LifetimeValue`](../../../samples/Sample_LineageExtension/Models.cs:125): Total customer value
- [`OrderCount`](../../../samples/Sample_LineageExtension/Models.cs:130): Number of orders placed
- [`IsVip`](../../../samples/Sample_LineageExtension/Models.cs:140): Whether customer is VIP

### EnrichedOrder

Order enriched with customer data and calculated discounts.

**Key Properties**:

- [`Order`](../../../samples/Sample_LineageExtension/Models.cs:175): Original order event
- [`Customer`](../../../samples/Sample_LineageExtension/Models.cs:180): Customer data
- [`Discount`](../../../samples/Sample_LineageExtension/Models.cs:185): Calculated discount amount
- [`FinalAmount`](../../../samples/Sample_LineageExtension/Models.cs:190): Amount after discount
- [`Priority`](../../../samples/Sample_LineageExtension/Models.cs:195): Processing priority

### ValidatedOrder

Order after business rule validation.

**Key Properties**:

- [`EnrichedOrder`](../../../samples/Sample_LineageExtension/Models.cs:227): The enriched order
- [`IsValid`](../../../samples/Sample_LineageExtension/Models.cs:232): Whether validation passed
- [`ValidationErrors`](../../../samples/Sample_LineageExtension/Models.cs:237): List of validation errors

## Adapting the Sample

### Step 1: Replace Domain Models

Replace the e-commerce domain models with your own data structures:

```csharp
// Your domain model
public sealed record MyData
{
    public int Id { get; init; }
    public string Name { get; init; } = string.Empty;
    public DateTime Timestamp { get; init; }
    // Add your properties
}
```

### Step 2: Implement Source Node

Create a source node that generates or reads your data:

```csharp
public sealed class MySource : SourceNode<MyData>
{
    public override IDataPipe<MyData> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        // Generate, read from database, or load from files
        var data = LoadMyData();
        return new InMemoryDataPipe<MyData>(data, "MySource");
    }
}
```

### Step 3: Implement Transform Nodes

Create transform nodes for your business logic:

```csharp
public sealed class MyTransform : TransformNode<MyData, ProcessedData>
{
    public override Task<ProcessedData> ExecuteAsync(MyData item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Your transformation logic
        var processed = new ProcessedData(item);
        return Task.FromResult(processed);
    }
}
```

### Step 4: Configure Lineage

Configure lineage tracking based on your needs:

```csharp
// For development/testing (full tracking)
builder.EnableItemLevelLineage();

// For production (sampling)
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 100; // 1% sampling
    options.DeterministicSampling = true;
    options.RedactData = true; // Exclude actual data
});
```

### Step 5: Implement Custom Sink

Create a sink for your destination:

```csharp
public sealed class MySink : SinkNode<ProcessedData>
{
    public async Task ExecuteAsync(IDataPipe<ProcessedData> input, PipelineContext context, IPipelineActivity parentActivity, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            // Write to your destination
            await WriteToDestination(item);
        }
    }
}
```

### Step 6: Build Pipeline

Assemble your pipeline:

```csharp
public sealed class MyPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<MySource, MyData>();
        var transform = builder.AddTransform<MyTransform, MyData, ProcessedData>();
        var sink = builder.AddSink<MySink, ProcessedData>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

## Best Practices

### Sampling Strategy

Choose the right sampling strategy for your use case:

| Scenario | Sampling Rate | Type | Rationale |
| --- | --- | --- | --- |
| Development | 100% (SampleEvery = 1) | N/A | Full visibility for debugging |
| Testing | 10-50% | Deterministic | Consistent behavior for test reproducibility |
| Staging | 1-10% | Deterministic | Reduced overhead with consistency |
| Production Low Throughput | 1-10% | Deterministic | Balance visibility and performance |
| Production High Throughput | 0.1-1% | Random | Representative sampling with minimal overhead |
| Compliance | 100% or Deterministic | Deterministic | Meet audit requirements |

### Data Redaction

Enable data redaction when:

- Data objects are large (> 1 KB)
- Data contains sensitive information (PII, PHI, financial data)
- You only care about flow patterns, not actual values
- Memory usage is a concern

```csharp
options.RedactData = true;
```

### Custom Sinks

Implement custom sinks when you need to:

- Store lineage data in a specific format or database
- Integrate with existing monitoring systems
- Perform custom analysis or aggregation
- Filter or transform lineage data before storage

### Performance Monitoring

Use lineage data to:

- Identify slow nodes (analyze hop timestamps)
- Find bottlenecks (analyze hop counts and cardinality)
- Track error rates (analyze decision outcomes)
- Understand data distribution (analyze traversal paths)

## Troubleshooting

### No Lineage Data Collected

**Problem**: Lineage information is empty or not being collected.

**Solutions**:

1. Verify lineage is enabled: `builder.EnableItemLevelLineage()`
2. Check sampling rate: `options.SampleEvery` should be >= 1
3. Ensure [`ILineageCollector`](../../../src/NPipeline.Extensions.Lineage/LineageCollector.cs:10) is registered in DI
4. Verify items are actually flowing through the pipeline

### High Memory Usage

**Problem**: Memory usage increases significantly with lineage enabled.

**Solutions**:

1. Enable sampling: `options.SampleEvery = 100` or higher
2. Enable data redaction: `options.RedactData = true`
3. Set materialization cap: `options.MaterializationCap = 10000`
4. Use overflow policy: `options.OverflowPolicy = LineageOverflowPolicy.Degrade`

### Performance Degradation

**Problem**: Pipeline execution slows down with lineage enabled.

**Solutions**:

1. Increase sampling rate to reduce overhead
2. Disable unnecessary metadata capture
3. Use deterministic sampling for consistent performance
4. Consider async lineage sinks to avoid blocking

## Sampling Configuration Decision Tree

Use this decision tree to choose the right sampling strategy for your pipeline:

```text
Start: Is data lineage required?
├─ NO → Disable lineage (options.Enabled = false)
└─ YES: Is 100% item tracking acceptable?
   ├─ YES (low-volume pipeline) → No sampling (SampleEvery = 1)
   └─ NO: Do you need reproducible debugging?
      ├─ YES (development/debugging) → Deterministic sampling
      │  ├─ Deterministic.SampleEvery = 10 (10% sampling)
      │  └─ Set DeterministicSampling = true
      └─ NO: Do you need statistical accuracy?
         ├─ YES (monitoring, analytics) → Random sampling
         │  ├─ Random.Percentage = 10 (approximately 10%)
         │  └─ Set DeterministicSampling = false
         └─ NO: Choose based on throughput
            ├─ Low (< 1000 items/sec) → SampleEvery = 10 (10%)
            ├─ Medium (1000-10000 items/sec) → SampleEvery = 100 (1%)
            └─ High (> 10000 items/sec) → SampleEvery = 1000 (0.1%)
```

## Troubleshooting Guide for New Retrieval Methods

### Using `GetLineageInfo()`

**Purpose**: Retrieve lineage for a specific item after pipeline execution.

**Common Issues**:

**Problem**: Null or empty lineage returned

**Causes**:

- Item was not tracked (check sampling configuration)
- Item key doesn't match what was recorded
- Pipeline hasn't finished execution yet

**Solutions**:

```csharp
// Verify the item was tracked
if (collector.GetLineageInfo(itemKey) == null)
{
    // Check if sampling excluded this item
    if (!collector.ShouldCollectLineage(itemKey))
    {
        Console.WriteLine("Item was sampled out");
    }
}
```

### Using `GetAllLineageInfo()`

**Purpose**: Retrieve all tracked lineage records (respects sampling).

**Common Issues**:

**Problem**: Too many records causing memory issues

**Solutions**:

```csharp
// Stream results instead of loading all at once
var infos = collector.GetAllLineageInfo();
foreach (var info in infos.Take(1000))
{
    ProcessLineage(info);
}
```

**Problem**: Records are incomplete or partial

**Causes**:

- Overflow policy set to Drop
- Pipeline still executing (lineage not finalized)

**Solutions**:

```csharp
// Wait for pipeline to complete
await pipeline.ExecuteAsync(data);

// Then retrieve complete lineage
var allLineage = collector.GetAllLineageInfo();
var completeRecords = allLineage
    .Where(l => l.Hops.Count > 0)
    .ToList();
```

### Using `Clear()`

**Purpose**: Reset lineage collection between runs.

**Common Issues**:

**Problem**: Old lineage data interferes with new tracking

**Solutions**:

```csharp
// Clear before each pipeline execution
collector.Clear();
await pipeline.ExecuteAsync(data);

// Or create a fresh collector per execution
var newCollector = serviceProvider.GetRequiredService<ILineageCollector>();
```

**Problem**: Cannot clear lineage while pipeline is executing

**Causes**:

- Attempting to clear during async pipeline execution
- Race conditions in concurrent scenarios

**Solutions**:

```csharp
// Wait for execution to complete
var task = pipeline.ExecuteAsync(data);
await task; // Ensure complete

// Now safe to clear
collector.Clear();
```

## Additional Resources

- **[Lineage Extension README](../../../src/NPipeline.Extensions.Lineage/README.md)** - Complete API reference and configuration guide
- **[Lineage Extension Documentation](../extensions/lineage.md)** - High-level overview and use cases
- **[NPipeline Core Concepts](../core-concepts/index.md)** - Core pipeline concepts
- **[NPipeline Extensions](../extensions/index.md)** - Other available extensions

# NPipeline Lineage Extension Sample

This sample demonstrates the data lineage tracking capabilities of the NPipeline.Extensions.Lineage extension for NPipeline. Data lineage tracking provides
visibility into how data flows through a pipeline, enabling debugging, auditing, and compliance monitoring.

## Overview

The NPipeline.Extensions.Lineage extension provides comprehensive data lineage tracking for NPipeline pipelines. This sample demonstrates:

- **Basic Lineage Tracking**: Track data as it flows through each node in a pipeline
- **Sampling Strategies**: Reduce overhead by sampling items deterministically or randomly
- **Complex Pipeline Scenarios**: Lineage tracking across joins, branches, and error handling
- **Custom Lineage Sinks**: Implement custom sinks to export lineage data to external systems
- **Pipeline-Level Reports**: Generate high-level reports showing pipeline structure and data flow

## What This Sample Demonstrates

### 1. Basic Lineage Tracking

The [`BasicLineageTrackingPipeline`](LineageDemoPipeline.cs:15) demonstrates fundamental lineage tracking through a simple pipeline:

```
OrderSource → EnrichmentTransform → ValidationTransform → ProcessingTransform → ConsoleSink
```

Each item is tracked as it flows through the pipeline, recording:

- The nodes it passes through
- The order and timing of transformations
- The final outcome of processing

### 2. Deterministic Sampling

The [`DeterministicSamplingPipeline`](LineageDemoPipeline.cs:40) shows how to sample every Nth item to reduce overhead while maintaining visibility:

```csharp
// Sample every 3rd item deterministically
builder.ConfigureLineage(options => options
    .WithDeterministicSampling(samplingRate: 0.33));
```

Deterministic sampling ensures consistent behavior across runs - the same items are always sampled.

### 3. Random Sampling

The [`RandomSamplingPipeline`](LineageDemoPipeline.cs:65) demonstrates random sampling (~N% of items):

```csharp
// Sample approximately 30% of items randomly
builder.ConfigureLineage(options => options
    .WithRandomSampling(samplingRate: 0.3));
```

Random sampling provides a representative sample with minimal overhead.

### 4. Complex Join Pipeline

The [`ComplexJoinPipeline`](LineageDemoPipeline.cs:90) shows lineage tracking across multi-source joins:

```
OrderSource → EnrichmentTransform → ValidationTransform → ProcessingTransform → ConsoleSink
```

Lineage is maintained when combining data from multiple sources, showing the provenance of each data element.

### 5. Branching with Lineage

The [`BranchingWithLineagePipeline`](LineageDemoPipeline.cs:117) demonstrates lineage tracking through branching paths:

```
OrderSource → EnrichmentTransform → FraudDetectionBranch → ValidationTransform → ProcessingTransform → ConsoleSink
```

Lineage tracks items as they split into different branches and recombine, showing all paths taken.

### 6. Error Handling with Lineage

The [`ErrorHandlingWithLineagePipeline`](LineageDemoPipeline.cs:144) shows how lineage tracks error outcomes:

```
OrderSource → EnrichmentTransform → ValidationTransform → ProcessingTransform → ConsoleSink
                                                                              ↓
                                                                         DatabaseSink
```

Lineage records validation failures, processing errors, and retry attempts for debugging and compliance.

### 7. Custom Lineage Sink

The [`CustomLineageSinkPipeline`](LineageDemoPipeline.cs:171) demonstrates implementing a custom [`IPipelineLineageSink`](Nodes/CustomLineageSink.cs:13):

- Collects pipeline-level lineage reports
- Exports reports to JSON files
- Displays lineage information to console
- Provides summary statistics

## How to Run the Sample

### Prerequisites

- .NET 8.0, 9.0, or 10.0 SDK
- The NPipeline project built and available

### Building the Sample

```bash
# Build the sample project
dotnet build samples/Sample_LineageExtension/Sample_LineageExtension.csproj
```

### Running the Sample

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

### Expected Output

The sample generates:

1. **Console Output**: Shows pipeline execution progress and processed items
2. **Lineage Reports**: JSON files containing pipeline structure and data flow information
3. **Statistics**: Summary of processed items, validation results, and lineage information

## Key Concepts Illustrated

### Domain Models

The sample uses realistic e-commerce domain models:

- [`OrderEvent`](Models.cs:7): Represents an order with customer, product, and payment information
- [`CustomerData`](Models.cs:95): Customer profile with loyalty tier and lifetime value
- [`EnrichedOrder`](Models.cs:170): Order enriched with customer data and calculated discounts
- [`ValidatedOrder`](Models.cs:222): Order after business rule validation
- [`ProcessedOrder`](Models.cs:264): Final processed order ready for storage

### Pipeline Nodes

The sample implements various node types:

- **Source Nodes**: [`OrderSource`](Nodes/OrderSource.cs), [`CustomerSource`](Nodes/CustomerSource.cs)
- **Transform Nodes**: [`EnrichmentTransform`](Nodes/EnrichmentTransform.cs), [`ValidationTransform`](Nodes/ValidationTransform.cs), [
  `ProcessingTransform`](LineageDemoPipeline.cs:196)
- **Branch Node**: [`FraudDetectionBranch`](Nodes/FraudDetectionBranch.cs)
- **Sink Nodes**: [`ConsoleSink`](Nodes/ConsoleSink.cs), [`DatabaseSink`](Nodes/DatabaseSink.cs)

### Lineage Configuration

Lineage is configured through dependency injection:

```csharp
// Add lineage services with default logging sink
services.AddNPipelineLineage();

// Add lineage services with custom sink
services.AddNPipelineLineage(sp => new CustomLineageSink("lineage-reports.json"));
```

### Lineage Information

The lineage extension tracks:

- **Item-Level Lineage**: Traces individual items through the pipeline
- **Pipeline-Level Reports**: High-level structure showing nodes and edges
- **Sampling Configurations**: Deterministic or random sampling to reduce overhead
- **Custom Sinks**: Export lineage data to external systems

## Architecture

The sample follows NPipeline patterns:

1. **Pipeline Definitions**: Each scenario implements [`IPipelineDefinition`](LineageDemoPipeline.cs:15)
2. **Dependency Injection**: Nodes and services are registered and resolved through DI
3. **Assembly Scanning**: Nodes are automatically discovered and registered
4. **Lineage Integration**: Lineage tracking is seamlessly integrated into pipeline execution

## Files

- [`Program.cs`](Program.cs): Main entry point with scenario selection
- [`LineageDemoPipeline.cs`](LineageDemoPipeline.cs): Pipeline definitions for all scenarios
- [`Models.cs`](Models.cs): Domain models for the sample
- [`Nodes/`](Nodes/): Directory containing all pipeline nodes
    - [`OrderSource.cs`](Nodes/OrderSource.cs): Generates order events
    - [`CustomerSource.cs`](Nodes/CustomerSource.cs): Generates customer profiles
    - [`EnrichmentTransform.cs`](Nodes/EnrichmentTransform.cs): Enriches orders with customer data
    - [`ValidationTransform.cs`](Nodes/ValidationTransform.cs): Validates enriched orders
    - [`FraudDetectionBranch.cs`](Nodes/FraudDetectionBranch.cs): Branches based on fraud detection
    - [`ConsoleSink.cs`](Nodes/ConsoleSink.cs): Writes processed orders to console
    - [`DatabaseSink.cs`](Nodes/DatabaseSink.cs): Simulates database writes
    - [`CustomLineageSink.cs`](Nodes/CustomLineageSink.cs): Custom lineage sink implementation
- [`README.md`](README.md): This file

## Use Cases

Data lineage tracking is valuable for:

- **Debugging**: Trace data flow to identify issues
- **Auditing**: Record data transformations for compliance
- **Monitoring**: Understand pipeline performance and bottlenecks
- **Data Governance**: Track data provenance and quality
- **Root Cause Analysis**: Identify where data issues originated

## Additional Resources

- [NPipeline Documentation](../../docs/index.md)
- [Lineage Extension README](../../src/NPipeline.Extensions.Lineage/README.md)
- [NPipeline Core Concepts](../../docs/core-concepts/index.md)

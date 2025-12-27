# Sample 5: Parallel Processing Pipeline

## Overview

This sample demonstrates advanced NPipeline parallel processing capabilities for CPU-intensive workloads, showing how to configure and use parallel execution
strategies for optimal resource utilization.

## Key Concepts

1. **Parallel Execution Strategies**
    - Blocking parallelism with backpressure control
    - Drop-oldest and drop-newest queue policies
    - Configurable degree of parallelism

2. **Resource Management**
    - Thread pool utilization
    - Bounded queues for resource contention handling
    - Performance monitoring and metrics collection

3. **Thread Safety**
    - Thread-safe metrics collection
    - Concurrent data processing
    - Result aggregation across multiple threads

## Quick Setup and Run

### Prerequisites

- .NET 8.0, .NET 9.0 or .NET 10.0 SDK
- JetBrains Rider, Visual Studio 2022, VS Code, or .NET CLI

### Running the Sample

```bash
cd samples/Sample_ParallelProcessing
dotnet restore
dotnet run
```

## Pipeline Flow

1. **CpuIntensiveDataSource** - Generates CPU-intensive work items with varying complexity
2. **ParallelCpuTransform** - Processes items in parallel using multiple threads
3. **PerformanceMonitoringTransform** - Tracks execution metrics and timing
4. **ConsoleSinkWithMetrics** - Outputs results and performance statistics

## Parallel Configuration

The sample uses blocking parallelism with:

- Degree of parallelism: Number of processor cores
- Maximum queue length: 50 items
- Thread-safe metrics collection
- Performance analysis by complexity level

## Expected Output

The sample displays:

- Individual work item processing with thread IDs
- Thread utilization statistics
- Performance metrics summary
- Processing analysis by complexity level
- Average, minimum, and maximum processing times

## Key Features Demonstrated

- Configurable parallel execution strategies
- Resource contention handling with bounded queues
- Performance metrics collection and analysis
- Thread-safe operations in concurrent scenarios
- CPU-intensive workload optimization

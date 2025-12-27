# Sample 8: Custom Node Implementation

## Overview

This sample demonstrates advanced custom node development patterns in NPipeline, focusing on lifecycle management, performance optimization through caching and
batching, and observability.

## Key Concepts

1. **Custom Source Node Implementation**
    - Inheriting from `SourceNode<T>`
    - Lifecycle management with initialization and disposal
    - Simulated sensor data generation with realistic patterns

2. **Advanced Transform Node with Caching**
    - Performance optimization through intelligent caching
    - Cache invalidation strategies
    - Expensive calculation simulation

3. **Custom Sink Node with Batching**
    - Batch processing for improved throughput
    - Configurable batch sizes and timeouts
    - Resource utilization optimization

4. **Lifecycle Monitoring**
    - Node lifecycle event tracking
    - Performance metrics collection
    - Observability patterns

## Quick Setup and Run

### Prerequisites

- .NET 8.0, .NET 9.0 or .NET 10.0 SDK
- JetBrains Rider, Visual Studio 2022, VS Code, or .NET CLI

### Running the Sample

```bash
cd samples/Sample_CustomNodeImplementation
dotnet restore
dotnet run
```

## Pipeline Flow

1. **SensorDataSource** generates simulated sensor data with realistic patterns
2. **LifecycleMonitorNode** tracks node lifecycle events for observability
3. **CachedTransform** processes data with caching for performance optimization
4. **BatchingSink** outputs data in batches for improved throughput

## Implementation Patterns Demonstrated

- Custom node development by inheriting from base node types
- Performance optimization through caching and batching
- Lifecycle event management (initialization, execution, disposal)
- Structured code for testability (without actual tests)
- Monitoring and observability patterns

## Expected Output

The sample will generate simulated sensor data from 5 sensors, process it through a caching transform, and output in batches, with detailed logging of lifecycle
events and performance metrics throughout the execution.

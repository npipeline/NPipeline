---
title: Sample Applications
description: Real-world examples demonstrating NPipeline concepts and patterns for different skill levels
sidebar_position: 8
---

# Sample Applications

This guide provides practical examples of NPipeline implementations, organized by skill level and complexity. Each sample demonstrates specific concepts and patterns you can apply to your own pipelines.

> **Location:** All samples are located in the `/samples/` directory of the repository. Each sample includes complete source code, configuration files, and a README with detailed explanations.

## Basic Samples

### Sample 01: Basic Pipeline
**Concepts demonstrated:**
- Basic source, transform, and sink nodes
- Simple data flow between nodes
- Pipeline definition and execution
- Dependency injection integration

**What it does:** A "Hello World" pipeline that demonstrates the fundamental NPipeline concepts with a source that generates data, a transform that processes it, and a sink that outputs the results.

**Key takeaways:** How to structure a basic pipeline and connect nodes together.

---

### Sample 02: File Processing Pipeline
**Concepts demonstrated:**
- File-based source and sink nodes
- Stream processing for memory efficiency
- Line-by-line text transformation
- Atomic file writing operations

**What it does:** Reads text files line by line, processes each line with configurable transformations (prefixes, line numbers, case conversion), and writes the results to output files using atomic operations.

**Key takeaways:** Working with file-based data sources and sinks in NPipeline.

---

### Sample 03: Basic Error Handling
**Concepts demonstrated:**
- Try-catch patterns in pipeline nodes
- Basic retry logic with exponential backoff
- Error logging and collection
- Graceful degradation with fallback mechanisms
- Error isolation to prevent cascading failures

**What it does:** Implements a pipeline with comprehensive error handling, including retries with exponential backoff, fallback mechanisms, and error tracking. Shows how to build resilient pipelines that maintain service availability during failures.

**Key takeaways:** Designing pipelines that gracefully handle failures and maintain data integrity.

## Intermediate Samples

### Sample 04: Simple Data Transformation
**Concepts demonstrated:**
- CSV to object transformation
- Data validation patterns
- Filtering mechanisms
- Data enrichment

**What it does:** Reads CSV data, validates it according to business rules, filters based on age and location, and enriches it with additional information like country and age categories.

**Key takeaways:** Implementing data validation, filtering, and enrichment patterns.

---

### Sample 05: Parallel Processing
**Concepts demonstrated:**
- Parallel execution strategies
- Resource management
- Thread safety
- Performance monitoring

**What it does:** Demonstrates parallel processing capabilities for CPU-intensive workloads, showing how to configure and use parallel execution strategies for optimal resource utilization.

**Key takeaways:** How to leverage parallelism while avoiding common pitfalls.

---

### Sample 06: Advanced Error Handling
**Concepts demonstrated:**
- Circuit breaker patterns
- Dead letter queues
- Advanced retry strategies
- Error recovery mechanisms
- Monitoring and alerting

**What it does:** Implements production-grade resilience patterns including circuit breakers to prevent cascading failures, dead letter queues for failed items, and comprehensive error recovery mechanisms using Polly.

**Key takeaways:** Building production-ready pipelines with comprehensive error handling.

## Advanced Samples

### Sample 07: Streaming Analytics
**Concepts demonstrated:**
- Time-based windowing (tumbling and sliding windows)
- Watermarks and late data handling
- Real-time aggregations
- Branching pipelines

**What it does:** Implements a time-windowed aggregation pipeline that processes sensor data in real-time, handles late-arriving data, and performs statistical analysis with trend and anomaly detection.

**Key takeaways:** Building stream processing applications with correct temporal semantics.

---

### Sample 08: Custom Node Implementation
**Concepts demonstrated:**
- Creating custom node implementations
- Lifecycle management
- Performance optimization through caching and batching
- Observability patterns

**What it does:** Shows how to implement custom node types including a sensor data source, a cached transform for performance optimization, and a batching sink for improved throughput.

**Key takeaways:** Extending NPipeline to meet specific application requirements.

---

### Sample 09: Performance Optimization
**Concepts demonstrated:**
- ValueTask optimization
- Synchronous fast paths
- Memory allocation reduction
- Performance measurement

**What it does:** Demonstrates advanced performance optimization techniques including ValueTask usage, synchronous fast paths for simple operations, memory allocation reduction with ArrayPool, and comprehensive performance measurement.

**Key takeaways:** Advanced techniques for optimizing pipeline performance.

---

### Sample 10: Complex Data Transformations
**Concepts demonstrated:**
- Multi-stream joins
- External data lookups
- Complex aggregations
- Data lineage tracking

**What it does:** Implements sophisticated data processing scenarios including joining orders with customer data, product enrichment, complex aggregations, and complete data lineage tracking for auditability.

**Key takeaways:** Building complex data pipelines with joins, lookups, and lineage tracking.

---

### Sample 11: CSV Connector
**Concepts demonstrated:**
- CSV source and sink nodes with `CsvSourceNode<T>` and `CsvSinkNode<T>`
- StorageUri abstraction for file system access
- Custom validation transform with business rule enforcement
- Data transformation and enrichment patterns
- Error handling for malformed data with configurable filtering
- IPipelineDefinition pattern for reusable pipeline structures
- Pipeline configuration through parameters
- Node factory pattern for resolving constructor ambiguity

**What it does:** Implements a comprehensive CSV data processing pipeline that reads customer data from CSV files, validates it against business rules (ID, email format, age range, etc.), transforms and enriches the data (name normalization, country expansion, email formatting), and writes the processed results to output CSV files. The sample demonstrates proper error handling with configurable filtering of invalid records.

**Key takeaways:** Building robust CSV processing pipelines with validation, transformation, and error handling patterns using NPipeline's CSV connector components and the IPipelineDefinition pattern for creating reusable pipeline structures.

---

### Sample 12: Batching Node
**Concepts demonstrated:**
- `BatchingNode<T>` for collecting individual items into batches
- Size-based, time-based, and hybrid batching strategies
- BatchProcessingTransform for efficient batch operations
- Performance optimization through batch processing
- Bulk database operations with batched results
- BatchingExecutionStrategy for optimal performance
- Comprehensive testing patterns for batched pipelines

**What it does:** Demonstrates the `BatchingNode<T>` functionality for efficient batch processing of individual items. The sample simulates an IoT sensor data processing pipeline that collects individual sensor readings into batches, processes them with aggregations and calculations, and performs bulk database operations. It shows different batching strategies (size-based, time-based, and hybrid) and provides detailed performance analysis comparing batch processing to individual item processing.

**Key takeaways:** Using batching to significantly improve performance for computational operations, database operations, and network operations while maintaining flexibility and reliability in data processing pipelines.

---

### Sample 13: Keyed Join Node
**Concepts demonstrated:**
- KeyedJoinNode for joining data streams based on common keys
- Inner, Left Outer, Right Outer, and Full Outer join strategies
- Multi-stream correlation and data enrichment patterns
- Real-time aggregation on joined data
- Memory management for unmatched items
- Performance considerations for key distribution

**What it does:** Demonstrates how to use NPipeline's KeyedJoinNode to join data streams based on common keys in an e-commerce scenario. It shows how to join orders with customer data using different join strategies, enrich the results with product information, and generate business intelligence through real-time aggregations by customer tier and product category.

**Key takeaways:** Building sophisticated data pipelines that can correlate and enrich data from multiple streams using various join strategies while managing performance and memory efficiently.

---

### Sample 14: Time Windowed Join Node
**Concepts demonstrated:**
- TimeWindowedJoinNode for temporal correlation of events
- Tumbling and sliding window strategies with configurable sizes
- Timestamp extraction and watermark management for event-time processing
- Out-of-order data handling with configurable tolerance
- Memory-efficient state management with automatic cleanup
- Performance optimization for time-based joins

**What it does:** Demonstrates time-windowed joins for correlating IoT sensor readings with maintenance events that occur within specific time windows. The sample shows how to use tumbling and sliding windows to analyze maintenance effectiveness, handle out-of-order events, and perform temporal analysis on streaming data with proper watermark management.

**Key takeaways:** Implementing sophisticated temporal data processing patterns that can correlate events based on both keys and time proximity while handling real-world challenges like out-of-order data and managing memory efficiently.

## Getting Started with Samples

1. **Clone the repository** to access all sample code
2. **Start with Basic Samples** if you're new to NPipeline
   - Sample 01: Learn the fundamental pipeline concepts
   - Sample 02: Understand file-based processing
   - Sample 03: Master basic error handling patterns
3. **Progress to Intermediate Samples** once you're comfortable with the basics
   - Sample 04: Data transformation and validation
   - Sample 05: Parallel processing techniques
   - Sample 06: Advanced error handling and resilience
4. **Explore Advanced Samples** for production scenarios and optimization techniques
   - Sample 07: Streaming analytics and windowing
   - Sample 08: Custom node development
   - Sample 09: Performance optimization
   - Sample 10: Complex data transformations and joins
   - Sample 11: CSV connector and data processing
   - Sample 12: Batching node for efficient batch processing
   - Sample 13: Keyed join node for stream correlation
   - Sample 14: Time windowed join node for temporal analysis

Each sample includes:
- Complete source code with comments explaining key concepts
- Configuration files showing best practices
- README with detailed explanations and setup instructions
- Performance characteristics and tuning guidance

## Related Documentation

- [Quick Start](./getting-started/quick-start.md) - Get started with NPipeline
- [Core Concepts](./core-concepts/index.md) - Understand the building blocks
- [Learning Paths](./learning-paths.md) - Structured learning journey
- [Performance Optimization](./advanced-topics/synchronous-fast-paths.md) - Advanced performance techniques
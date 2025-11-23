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

**Key takeaways:** How to structure a basic pipeline and connect nodes together. Start here if you're new to NPipeline.

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

**Key takeaways:** Designing pipelines that gracefully handle failures and maintain data integrity. Builds on Sample 01 concepts and is essential for production systems.

## Intermediate Samples

### Sample 04: Simple Data Transformation

**Concepts demonstrated:**

- CSV to object transformation
- Data validation patterns
- Filtering mechanisms
- Data enrichment

**What it does:** Reads CSV data, validates it according to business rules, filters based on age and location, and enriches it with additional information like country and age categories.

**Key takeaways:** Implementing data validation, filtering, and enrichment patterns in data processing pipelines.

---

### Sample 05: Parallel Processing

**Concepts demonstrated:**

- Parallel execution strategies
- Resource management
- Thread safety
- Performance monitoring

**What it does:** Demonstrates parallel processing capabilities for CPU-intensive workloads, showing how to configure and use parallel execution strategies for optimal resource utilization.

**Key takeaways:** How to leverage parallelism while avoiding common pitfalls and managing resources effectively.

---

### Sample 06: Advanced Error Handling

**Concepts demonstrated:**

- Circuit breaker patterns
- Dead letter queues
- Advanced retry strategies
- Error recovery mechanisms
- Monitoring and alerting

**What it does:** Implements production-grade resilience patterns including circuit breakers to prevent cascading failures, dead letter queues for failed items, and comprehensive error recovery mechanisms using Polly.

**Key takeaways:** Building production-ready pipelines with comprehensive error handling. Extends Sample 03 with advanced patterns for mission-critical systems.

---

### Sample 20: LookupNode

**Concepts demonstrated:**

- LookupNode for data enrichment patterns using external data sources
- Key extraction and async lookup operations
- Data combination patterns merging original data with lookup results
- Error handling for missing or failed lookups
- Real-world IoT sensor data enrichment scenario
- Device metadata management and calibration validation
- Risk assessment and alerting patterns

**What it does:** Implements an IoT sensor data processing pipeline that demonstrates how to use LookupNode for enriching raw sensor readings with device metadata. The pipeline processes sensor data through multiple stages: metadata enrichment using LookupNode, calibration validation, risk assessment, and finally outputs to both regular and alerting sinks based on the calculated risk levels.

**Key takeaways:** Building data enrichment pipelines that can efficiently combine streaming data with external reference data. Understanding how to implement async lookup patterns, handle missing data gracefully, and create branching pipelines for different output scenarios based on business rules.

---

## Advanced Samples

### Foundational Advanced Concepts

#### Sample 07: Custom Node Implementation

**Concepts demonstrated:**

- Creating custom node implementations
- Lifecycle management
- Performance optimization through caching and batching
- Observability patterns

**What it does:** Shows how to implement custom node types including a sensor data source, a cached transform for performance optimization, and a batching sink for improved throughput.

**Key takeaways:** Extending NPipeline to meet specific application requirements. Essential foundation for advanced pipeline development.

---

#### Sample 08: Performance Optimization

**Concepts demonstrated:**

- ValueTask optimization
- Synchronous fast paths
- Memory allocation reduction
- Performance measurement

**What it does:** Demonstrates advanced performance optimization techniques including ValueTask usage, synchronous fast paths for simple operations, memory allocation reduction with ArrayPool, and comprehensive performance measurement.

**Key takeaways:** Advanced techniques for optimizing pipeline performance. Best practices for high-throughput systems.

---

### Branching and Distribution

#### Sample 19: BranchNode

**Concepts demonstrated:**

- BranchNode for fanning out data to multiple downstream pathways
- Parallel processing strategies with concurrent execution
- Data duplication patterns where each item is sent to all connected nodes
- Error isolation between branches to prevent cascading failures
- Type preservation with different output types per branch
- Real-world e-commerce scenario with inventory, analytics, and notifications
- Multi-stream processing with independent business logic per branch

**What it does:** Implements an e-commerce order processing pipeline that demonstrates how BranchNode can distribute order events to multiple parallel processing paths simultaneously. The pipeline processes orders through inventory management, business analytics, and customer notifications while maintaining the main order flow. Each branch operates independently with its own business logic, error handling, and output types.

**Key takeaways:** Building complex data distribution patterns with BranchNode for parallel processing while maintaining error isolation and type safety. Understanding how to implement real-world scenarios requiring multiple concurrent processing paths for the same data.

---

### Advanced Stream Merging

#### Sample 21: CustomMergeNode

**Concepts demonstrated:**

- CustomMergeNode for advanced stream merging strategies
- Priority-based merging with conflict resolution (NYSE > NASDAQ > International)
- Temporal alignment with configurable delay tolerance
- Data quality scoring and assessment (Completeness, Timeliness, Accuracy, Consistency)
- High-performance concurrent processing using Channel<T>
- Backpressure handling with intelligent buffering and drop strategies
- Real-world financial trading system scenario
- Multi-exchange market data processing
- Custom merge strategies extending IMergeStrategy<T>
- Performance optimization for high-frequency trading scenarios

**What it does:** Implements a sophisticated financial trading data processing pipeline that merges market data from multiple exchanges (NYSE, NASDAQ, International) using CustomMergeNode with priority-based conflict resolution and temporal alignment strategies. The pipeline demonstrates how to handle real-world challenges in financial data processing, including conflicting data from different exchanges, timing variations, and data quality assessment. It showcases advanced merging patterns where NYSE data takes precedence over NASDAQ and International exchanges, with temporal alignment to handle out-of-order data and configurable delay tolerance windows.

**Key takeaways:** Building complex stream merging systems with custom conflict resolution logic and business rule integration. Understanding how to implement priority-based merging strategies, temporal alignment for handling timing discrepancies, and data quality assessment for financial data processing. Learning advanced performance optimization techniques using Channel<T> for high-throughput scenarios and backpressure handling for reliable processing in high-frequency trading environments.

---

### Data Transformation and Type Conversion

#### Sample 09: Type Conversion Node

**Concepts demonstrated:**

- `TypeConversionNode<TIn, TOut>` for seamless data type transformations
- `AutoMap()` for automatic property mapping by name (case-insensitive)
- `Custom Map()` for explicit property-to-property mapping with converters
- Factory functions for complex object initialization
- Error handling with graceful fallbacks for conversion failures
- Performance optimization through compiled expressions
- String parsing, JSON deserialization, and legacy system modernization patterns

**What it does:** Demonstrates comprehensive TypeConversionNode functionality for transforming data between different types in real-world scenarios. Shows three parallel data processing paths: string data parsing (simulating CSV/log ingestion), JSON data processing (simulating API integration), and legacy data modernization.

**Key takeaways:** Building robust data integration pipelines that can seamlessly transform between different data formats while maintaining high performance and comprehensive error handling.

---

#### Sample 10: CSV Connector

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

### Stream Processing and Windowing

#### Sample 11: Streaming Analytics

**Concepts demonstrated:**

- Time-based windowing (tumbling and sliding windows)
- Watermarks and late data handling
- Real-time aggregations
- Branching pipelines

**What it does:** Implements a time-windowed aggregation pipeline that processes sensor data in real-time, handles late-arriving data, and performs statistical analysis with trend and anomaly detection.

**Key takeaways:** Building stream processing applications with correct temporal semantics and handling real-world challenges like out-of-order and late-arriving data.

---

### Aggregation and Windowing

#### Sample 17: AggregateNode

**Concepts demonstrated:**

- `AggregateNode<TIn, TKey, TResult>` for time-based aggregations
- Tumbling and sliding window strategies
- Key-based aggregation by different dimensions
- Event-time processing with watermarks
- Real-time analytics dashboard patterns
- Multi-dimensional aggregations with identical accumulator and result types

**What it does:** Implements a real-time analytics dashboard that processes user interaction events (page views, clicks, purchases) and generates aggregated metrics using tumbling windows for event counting and sliding windows for value summation. The sample demonstrates filtering irrelevant events and displaying formatted metrics in a console dashboard.

**Key takeaways:** Building real-time analytics systems with simple aggregations where accumulator and result types are identical. Understanding windowing patterns and key-based grouping for streaming analytics scenarios.

---

#### Sample 18: AdvancedAggregateNode

**Concepts demonstrated:**

- `AdvancedAggregateNode<TIn, TKey, TAccumulate, TResult>` for complex aggregations
- Separate accumulator and result types for sophisticated state management
- Financial risk calculations (volatility, Value at Risk, portfolio analytics)
- Statistical calculations using complex accumulator patterns
- Weighted metrics and risk-adjusted performance calculations
- Real-time risk monitoring with alerting

**What it does:** Implements a comprehensive financial risk analysis pipeline that processes simulated trading data and calculates various risk metrics including volatility, Value at Risk (VaR), and portfolio analytics. Demonstrates sophisticated accumulator patterns using tuples, dictionaries, and collections for complex state management.

**Key takeaways:** Building advanced analytics systems where accumulator and result types differ, enabling complex statistical calculations and sophisticated state management. Understanding when to use AdvancedAggregateNode over AggregateNode for complex scenarios.

---

### Advanced Join Operations

#### Sample 12: Keyed Join Node

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

#### Sample 13: Time Windowed Join Node

**Concepts demonstrated:**

- TimeWindowedJoinNode for temporal correlation of events
- Tumbling and sliding window strategies with configurable sizes
- Timestamp extraction and watermark management for event-time processing
- Out-of-order data handling with configurable tolerance
- Memory-efficient state management with automatic cleanup
- Performance optimization for time-based joins

**What it does:** Demonstrates time-windowed joins for correlating IoT sensor readings with maintenance events that occur within specific time windows. The sample shows how to use tumbling and sliding windows to analyze maintenance effectiveness, handle out-of-order events, and perform temporal analysis on streaming data with proper watermark management.

**Key takeaways:** Implementing sophisticated temporal data processing patterns that can correlate events based on both keys and time proximity while handling real-world challenges like out-of-order data and managing memory efficiently.

---

### Batch Processing

#### Sample 14: Batching Node

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

### Observability and Monitoring

#### Sample 15: TapNode

**Concepts demonstrated:**

- `TapNode<T>` for non-intrusive monitoring of data streams
- Multiple tap points for comprehensive pipeline observability
- Audit logging for compliance and regulatory requirements
- Real-time metrics collection and performance monitoring
- Alert generation for suspicious activities and operational issues
- Error isolation between monitoring and main processing flows
- Side-effect processing without modifying core business logic

**What it does:** Demonstrates how to use TapNode to add comprehensive monitoring capabilities to a financial transaction processing pipeline without affecting the main data flow. The sample shows multiple tap points at different pipeline stages (source, validation, processing) that generate audit trails, collect performance metrics, and create alerts for suspicious activities. It showcases how TapNode enables observability, compliance, and operational intelligence while maintaining separation between monitoring concerns and core business logic.

**Key takeaways:** Implementing non-intrusive monitoring patterns that provide comprehensive observability without modifying core pipeline logic or impacting performance.

---

### Complex End-to-End Scenarios

#### Sample 16: Complex Data Transformations

**Concepts demonstrated:**

- Multi-stream joins
- External data lookups
- Complex aggregations
- Data lineage tracking

**What it does:** Implements sophisticated data processing scenarios including joining orders with customer data, product enrichment, complex aggregations, and complete data lineage tracking for auditability.

**Key takeaways:** Building complex data pipelines with joins, lookups, and lineage tracking for production systems. Integrates many concepts from earlier samples.

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
   - Sample 20: LookupNode for data enrichment and external lookups
4. **Explore Advanced Samples** for production scenarios and optimization techniques
   - Sample 07: Custom node development
   - Sample 08: Performance optimization
   - Sample 09: Type conversion node for data transformation
   - Sample 10: CSV connector and data processing
   - Sample 11: Streaming analytics and windowing
   - Sample 12: Keyed join node for stream correlation
   - Sample 13: Time windowed join node for temporal analysis
   - Sample 14: Batching node for efficient batch processing
   - Sample 15: TapNode for non-intrusive monitoring and observability
   - Sample 16: Complex data transformations and joins
   - Sample 17: AggregateNode for real-time analytics and simple aggregations
   - Sample 18: AdvancedAggregateNode for complex state management and financial analytics
   - Sample 19: BranchNode for parallel data distribution and processing
   - Sample 21: CustomMergeNode for advanced stream merging and conflict resolution
   - Sample 22: Custom node development

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

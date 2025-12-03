---
title: Intermediate Samples
description: Real-world NPipeline samples for intermediate developers (Samples 6-10)
sidebar_position: 3
---

# Intermediate Samples (6-10)

These samples demonstrate real-world patterns and techniques for building practical data processing pipelines. Build on the fundamentals from Basic Samples.

---

## Sample 06: Advanced Error Handling

**Concepts demonstrated:**

- Circuit breaker patterns
- Dead letter queues
- Advanced retry strategies
- Error recovery mechanisms
- Monitoring and alerting

**What it does:** Implements production-grade resilience patterns including circuit breakers to prevent cascading failures, dead letter queues for failed items, and comprehensive error recovery mechanisms using Polly.

**Key takeaways:** Building production-ready pipelines with comprehensive error handling. Extends Sample 03 with advanced patterns for mission-critical systems.

---

## Sample 07: LookupNode

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

## Sample 08: CSV Connector

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

## Sample 09: AggregateNode

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

## Sample 10: BranchNode

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

## Next Steps

- Ready for advanced patterns? → [Advanced Samples (11-23)](./advanced.md)
- Want a quick refresher? → [Basic Samples (1-5)](./basic.md)
- Looking for a specific topic? → [Samples by Topic](./by-topic.md)

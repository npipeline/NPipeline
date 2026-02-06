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

## Sample 09: JSON Connector

**Concepts demonstrated:**

- JSON source and sink nodes with `JsonSourceNode<T>` and `JsonSinkNode<T>`
- JSON array and NDJSON (newline-delimited JSON) format support
- StorageUri abstraction for file system access
- Attribute-based mapping with ColumnAttribute
- Manual mapper functions for custom mapping logic
- Configurable property naming policies (lowercase, camelCase, snakeCase, PascalCase)
- Data transformation and validation patterns
- Error handling for malformed JSON data

**What it does:** Implements a comprehensive JSON data processing pipeline that reads customer data from JSON files (supporting both JSON array and NDJSON formats), validates it against business rules, transforms and enriches the data, and writes processed results to output JSON files. The sample demonstrates both attribute-based mapping using ColumnAttribute and manual mapper functions for custom scenarios. It also showcases different JSON output formats and property naming policies.

**Key takeaways:** Building flexible JSON processing pipelines with support for multiple JSON formats, configurable naming conventions, and robust error handling using NPipeline's JSON connector components. Understanding how to work with both JSON arrays and NDJSON formats for different use cases.

---

## Sample 10: AggregateNode

**Concepts demonstrated:**

- AggregateNode for aggregating data from multiple sources
- Windowing and grouping strategies
- Aggregation functions (sum, count, average, etc.)
- State management and checkpointing
- Error handling and recovery for partial data
- Real-world analytics and reporting scenario

**What it does:** Implements a data aggregation pipeline that reads data from multiple sources, applies windowing and grouping strategies, performs aggregation functions, and writes the results to a sink. The sample demonstrates how to handle partial data, manage state, and recover from errors during the aggregation process.

**Key takeaways:** Building scalable and fault-tolerant data aggregation pipelines. Understanding how to implement windowing, grouping, and aggregation functions, and how to manage state and checkpoints for long-running processes.

---

## Next Steps

- Ready for advanced patterns? → [Advanced Samples (11-23)](./advanced.md)
- Want a quick refresher? → [Basic Samples (1-5)](./basic.md)
- Back to overview? → [All Samples](./index.md)

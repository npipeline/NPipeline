---
title: Basic Samples
description: Fundamental NPipeline samples for beginners (Samples 1-5)
sidebar_position: 2
---

# Basic Samples (1-5)

These samples cover the fundamental concepts and patterns you need to get started with NPipeline. Perfect for learning the basics.

---

## Sample 01: Basic Pipeline

**Concepts demonstrated:**

- Basic source, transform, and sink nodes
- Simple data flow between nodes
- Pipeline definition and execution
- Dependency injection integration

**What it does:** A "Hello World" pipeline that demonstrates the fundamental NPipeline concepts with a source that generates data, a transform that processes it, and a sink that outputs the results.

**Key takeaways:** How to structure a basic pipeline and connect nodes together. Start here if you're new to NPipeline.

---

## Sample 02: File Processing Pipeline

**Concepts demonstrated:**

- File-based source and sink nodes
- Stream processing for memory efficiency
- Line-by-line text transformation
- Atomic file writing operations

**What it does:** Reads text files line by line, processes each line with configurable transformations (prefixes, line numbers, case conversion), and writes the results to output files using atomic operations.

**Key takeaways:** Working with file-based data sources and sinks in NPipeline.

---

## Sample 03: Basic Error Handling

**Concepts demonstrated:**

- Try-catch patterns in pipeline nodes
- Basic retry logic with exponential backoff
- Error logging and collection
- Graceful degradation with fallback mechanisms
- Error isolation to prevent cascading failures

**What it does:** Implements a pipeline with comprehensive error handling, including retries with exponential backoff, fallback mechanisms, and error tracking. Shows how to build resilient pipelines that maintain service availability during failures.

**Key takeaways:** Designing pipelines that gracefully handle failures and maintain data integrity. Builds on Sample 01 concepts and is essential for production systems.

---

## Sample 04: Simple Data Transformation

**Concepts demonstrated:**

- CSV to object transformation
- Data validation patterns
- Filtering mechanisms
- Data enrichment

**What it does:** Reads CSV data, validates it according to business rules, filters based on age and location, and enriches it with additional information like country and age categories.

**Key takeaways:** Implementing data validation, filtering, and enrichment patterns in data processing pipelines.

---

## Sample 05: Parallel Processing

**Concepts demonstrated:**

- Parallel execution strategies
- Resource management
- Thread safety
- Performance monitoring

**What it does:** Demonstrates parallel processing capabilities for CPU-intensive workloads, showing how to configure and use parallel execution strategies for optimal resource utilization.

**Key takeaways:** How to leverage parallelism while avoiding common pitfalls and managing resources effectively.

---

## Next Steps

- Ready for intermediate patterns? → [Intermediate Samples (6-10)](./intermediate.md)
- Need advanced techniques? → [Advanced Samples (11-23)](./advanced.md)
- Back to overview? → [All Samples](./index.md)

---
title: Why NPipeline?
description: Explore the benefits, design philosophy, and performance characteristics that make NPipeline an ideal choice for your data processing needs.
sidebar_position: 2
---

## Why NPipeline?

NPipeline is engineered to address the common challenges in building robust and efficient data processing systems. It offers a unique blend of flexibility, performance, and developer experience that sets it apart.

## Use Cases

NPipeline excels in scenarios requiring structured, high-throughput data processing. Common use cases include:

* **ETL (Extract, Transform, Load) Workflows:** Building reliable data ingestion and transformation pipelines.
* **Real-time Data Processing:** Handling streaming data from various sources with low latency.
* **Data Validation and Cleansing:** Implementing complex validation rules and data quality checks.
* **Event-Driven Architectures:** Processing events as they occur in a scalable manner.
* **Batch Processing:** Efficiently processing large volumes of historical data.
* **Microservice Integration:** Facilitating data exchange and transformation between microservices.

## Design Philosophy

NPipeline's design is rooted in several core principles:

1. **Simplicity and Clarity:** A clear, intuitive API that makes pipeline construction straightforward and understandable.
2. **Performance First:** Optimized for minimal memory allocations and high concurrency, crucial for demanding workloads.
3. **Testability:** Designed from the ground up to be easily testable, promoting robust and reliable solutions.
4. **Extensibility:** A modular architecture that allows developers to easily extend functionality and integrate with external systems.
5. **Modern .NET:** Embracing the latest C# features and asynchronous programming patterns for efficient and idiomatic code.

## Performance Characteristics

Performance is a cornerstone of NPipeline. It is built to:

* **Minimize Allocations:** Judicious use of memory to reduce garbage collection overhead, leading to smoother operation.
* **Maximize Throughput:** Efficient internal mechanisms and asynchronous processing ensure high data processing rates.
* **Low Latency:** Designed to process data items quickly, making it suitable for near real-time applications.
* **Scalability:** The graph-based approach and efficient resource management allow pipelines to scale with increasing data volumes.
* **Zero-Allocation Fast Paths:** Transform operations with synchronous execution paths (cache hits, simple calculations) eliminate heap allocations entirely. In high-cache-hit scenarios, this can drastically reduce garbage collection pressure, cutting out thousands of allocations per second in high-throughput pipelines.

By focusing on these aspects, NPipeline provides a powerful foundation for building data pipelines that are not only functional but also performant and maintainable.

### Zero-Allocation Fast Paths: A Competitive Advantage

One of NPipeline's most powerful features for high-performance scenarios is its support for **zero-allocation fast paths** in transform nodes.

**The Problem:** In typical async-first frameworks, even simple synchronous operations (like cache lookups or quick calculations) create heap-allocated `Task<T>` objects. Processing millions of items per second, where many transforms are synchronous or have high cache hit rates, this creates millions of tiny allocations per second—constant pressure on the garbage collector.

**The Solution:** Using `ValueTask<T>`, you can implement a two-path pattern:

* **Fast Path (Synchronous):** Result available immediately → allocates on the stack, zero GC pressure
* **Slow Path (Asynchronous):** Work required → transitions seamlessly to true async

**The Impact:**

* For a pipeline processing 100,000 items/second with 90% cache hits, you eliminate 90,000 Task allocations per second
* Measured reduction in garbage collection pressure: **up to 90%**
* Particularly effective for: data validation, filtering, cached enrichment (everyday pipeline tasks)

**Learn how to implement this:** [Synchronous Fast Paths and ValueTask Optimization](../advanced-topics/synchronous-fast-paths.md)

## Next Steps

* **[Installation](../getting-started/installation.md)**: Get NPipeline set up in your development environment
* **[Quick Start](../getting-started/quick-start.md)**: Begin building your first pipeline
* **[Core Concepts](../core-concepts/index.md)**: Dive deeper into the fundamental building blocks of NPipeline
* **[High-Performance Optimization](../advanced-topics/synchronous-fast-paths.md)**: Master zero-allocation fast paths for maximum throughput

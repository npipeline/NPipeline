---
title: Extension Samples
description: Samples demonstrating NPipeline extension packages and their integration capabilities
sidebar_position: 5
---

# Extension Samples

These samples demonstrate how to use NPipeline extension packages to enhance your pipelines with specialized functionality. Each extension provides specific capabilities for production scenarios.

---

## Lineage Extension Sample

**Concepts demonstrated:**

- Item-level data lineage tracking through pipelines
- Deterministic and random sampling strategies
- Lineage tracking across joins, branches, and error handling
- Custom lineage sinks for exporting lineage data
- Pipeline-level lineage reports

**What it does:** Demonstrates comprehensive data lineage tracking capabilities using the NPipeline.Extensions.Lineage package. Shows how to track data flow through complex pipelines with joins, branching, and error handling. Includes multiple scenarios for different sampling strategies and custom sink implementations.

**Key takeaways:** Understanding how to implement data lineage tracking for debugging, auditing, and compliance. Learning sampling strategies to balance visibility with performance overhead.

**→ [Lineage Extension Documentation](../extensions/lineage/index.md)**

---

## Nodes Extension Sample

**Concepts demonstrated:**

- Pre-built data processing nodes for common operations
- String cleansing and normalization
- DateTime processing and timezone handling
- Data enrichment with default values
- Custom validation with business rules
- Filtering based on predicates

**What it does:** Demonstrates the NPipeline.Extensions.Nodes library, which provides ready-made, high-performance nodes for validation, cleansing, transformation, and enrichment. Shows how to process customer records through multiple data quality stages.

**Key takeaways:** Using pre-built nodes to accelerate development and ensure consistent data processing patterns. Understanding the fluent builder API for configuring node behavior.

**→ [Nodes Extension Documentation](../extensions/nodes/index.md)**

---

## Observability Extension Sample

**Concepts demonstrated:**

- Automatic metrics collection at each node
- Performance tracking (duration, throughput, processing time)
- Data flow analysis (processed vs. emitted items)
- Thread information tracking
- Resource usage monitoring
- Error handling and failure recording

**What it does:** Demonstrates comprehensive observability features using the NPipeline.Extensions.Observability extension. Shows how to collect, track, and analyze detailed metrics about pipeline execution across multiple stages.

**Key takeaways:** Implementing production-grade monitoring for pipelines. Understanding metrics collection patterns and using observability data for performance tuning and troubleshooting.

**→ [Observability Extension Documentation](../extensions/observability/index.md)**

---

## Composition Extension Sample

**Concepts demonstrated:**

- Hierarchical pipeline composition
- Using sub-pipelines as reusable transform nodes
- Context inheritance between parent and child pipelines
- Type safety across pipeline hierarchies
- Nested composition patterns

**What it does:** Demonstrates the NPipeline.Extensions.Composition extension for creating modular, hierarchical pipelines. Shows how to break complex workflows into smaller, well-tested building blocks with full type safety.

**Key takeaways:** Building modular, reusable pipeline components. Understanding context control and inheritance patterns in composite pipelines. Creating maintainable pipeline architectures.

**→ [Composition Extension Documentation](../extensions/composition/index.md)**

---

## Extension Integration Patterns

### Common Patterns Across Extension Samples

All extension samples demonstrate:

- **Dependency Injection Integration**: Registering extension services with `IServiceCollection`
- **Pipeline Builder Extensions**: Using extension methods to configure pipeline behavior
- **Modular Design**: Each extension addresses a specific concern without coupling
- **Production-Ready Patterns**: Error handling, logging, and configuration best practices

### Combining Extensions

Extensions are designed to work together seamlessly. Common combinations include:

- **Lineage + Observability**: Track data flow while monitoring performance
- **Composition + Observability**: Monitor hierarchical pipeline execution
- **Nodes + Lineage**: Apply data quality rules with lineage tracking
- **All Extensions**: Full-featured production pipelines with comprehensive capabilities

---

## Next Steps

- Ready for core concepts? → [Core Concepts](../core-concepts/index.md)
- Want to explore specific extensions? → [Extensions Overview](../extensions/index.md)
- Back to samples overview? → [All Samples](./index.md)
- Need advanced patterns? → [Advanced Samples](./advanced.md)

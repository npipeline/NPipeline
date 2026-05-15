---
title: "Advanced Topics"
description: "Deep dives into NPipeline internals — architecture, execution model, data flow, and extensibility."
order: 10
---

# Advanced Topics

These pages explain how NPipeline works under the hood. You don't need them to use the library, but they're valuable if you want to understand the design decisions, extend the framework, or contribute to the core.

## Architecture and Design

| Page | What You'll Learn |
|------|------------------|
| [Architecture Overview](architecture-overview.md) | System layers, component relationships, and how the pieces fit together |
| [Design Principles](design-principles.md) | The core philosophy behind every design decision — streaming-first, fail-fast, zero-allocation hot paths, and more |
| [Execution Model](execution-model.md) | How `PipelineRunner` orchestrates node execution from graph to output |
| [Data Flow Internals](data-flow-internals.md) | `IDataStream<T>`, async enumeration, and how streams connect nodes |
| [Node Instantiation](node-instantiation.md) | Compiled expression factories, DI fallback, and caching |
| [Cancellation](cancellation.md) | Cooperative cancellation propagation through the entire execution stack |

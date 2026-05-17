---
title: Performance
description: Optimization techniques for high-throughput NPipeline data pipelines.
order: 8
---

# Performance

NPipeline is designed for throughput. The execution engine uses `ValueTask` fast paths, compiled node factories, execution plan caching, and object pooling to minimize per-item overhead.

Most pipelines are fast without tuning. When you need to squeeze out more, these guides cover the optimization techniques available - many of which are enforced automatically by [build-time analyzers](../analyzers/index.md).

## In This Section

- [Best Practices](best-practices.md) - do's and don'ts backed by analyzer rules
- [Synchronous Fast Paths](synchronous-fast-paths.md) - eliminate Task allocations with `ValueTask` for synchronous transforms
- [Execution Plan Caching](execution-plan-caching.md) - avoid reflection overhead on repeated pipeline runs

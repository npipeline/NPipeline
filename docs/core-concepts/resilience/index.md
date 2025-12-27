---
title: Resilience Overview
description: Understand resilience concepts in NPipeline and how to build fault-tolerant data pipelines that can recover from failures.
sidebar_position: 1
---

# Resilience Overview

Resilience in NPipeline refers to the ability of your data pipelines to detect, handle, and recover from failures without complete system breakdown.
This section provides a comprehensive guide to building robust, fault-tolerant pipelines.

---

## Where to Start

**If you want to enable node restarts and common retry patterns, [start here: Getting Started with Resilience](./getting-started.md)**

This guide provides a quick-start checklist and step-by-step instructions for configuring the three mandatory prerequisites for node restarts. **This is the canonical starting point for most users.**

---

## Why Resilience Matters

In production environments, pipelines inevitably encounter failures from various sources:

- **Transient infrastructure issues**: Network timeouts, database connection failures
- **Data quality problems**: Invalid formats, missing values, unexpected data types
- **Resource constraints**: Memory pressure, CPU saturation, I/O bottlenecks
- **External service dependencies**: API rate limits, service outages, authentication failures

Without proper resilience mechanisms, these failures can cascade through your pipeline, causing data loss, system instability, and costly manual intervention.

## Resilience Strategy Comparison

| Strategy | Best For | Memory Requirements | Complexity | Key Benefits |
|----------|----------|-------------------|------------|--------------|
| **Simple Retry** | Transient failures (network timeouts, temporary service issues) | Low | Low | Quick recovery from temporary issues |
| **Node Restart** | Persistent node failures, resource exhaustion | Medium (requires materialization) | Medium | Complete recovery from node-level failures |
| **Circuit Breaker** | Protecting against cascading failures, external service dependencies | Low | Medium | Prevents system overload during outages |
| **Dead-Letter Queues** | Handling problematic items that can't be processed | Low | High | Preserves problematic data for manual review |
| **Combined Approach** | Production systems with multiple failure types | High | High | Comprehensive protection against all failure types |

### Choosing the Right Strategy

- **For simple pipelines with basic needs**: Start with Simple Retry
- **For streaming data processing**: Use Node Restart with materialization
- **For external service dependencies**: Add Circuit Breaker to prevent cascade failures
- **For critical data pipelines**: Implement Dead-Letter Queues to preserve failed items
- **For production systems**: Combine multiple strategies for comprehensive protection

## Core Resilience Components

NPipeline's resilience framework is built around several interconnected components:

| Component | Role | Best For |
|-----------|------|----------|
| **[Getting Started with Resilience](./getting-started.md)** | Quick-start checklist for node restarts and retry delays | New users; configuring resilience for the first time |
| **[Error Handling](error-handling.md)** | How to respond to failures at node and pipeline levels | Understanding error recovery strategies |
| **[Retry Options](retries.md)** | Configure retry limits, delays, and materialization | Fine-tuning resilience behavior |
| **[Materialization & Buffering](materialization.md)** | How buffering enables replay during restarts | Understanding the replay mechanism |
| **[Circuit Breakers](./circuit-breakers.md)** | Prevent cascading failures to external services | Protecting against external service outages |
| **[Dead-Letter Queues](./dead-letter-queues.md)** | Handle problematic items separately | Preserving failed items for manual review |
| **[Troubleshooting](./troubleshooting.md)** | Diagnose and resolve common resilience issues | Debugging failed configurations |

## Choosing Your Resilience Approach

**Simple Retry Logic:** For transient failures, use error decision handlers with retry limits. See [Getting Started](./getting-started.md) for quick examples.

**Node Restart:** For recovering from node-level failures, follow the 3-step checklist in [Getting Started with Resilience](./getting-started.md).

**Circuit Breakers:** For protecting against cascading failures to external services, see [Circuit Breakers](./circuit-breakers.md).

**Dead-Letter Queues:** For preserving problematic items, see [Dead-Letter Queues](./dead-letter-queues.md).

## Recommended Reading Order

1. **[Getting Started with Resilience](./getting-started.md)** ← Start here for a quick checklist
2. **[Error Handling](./error-handling.md)** ← Understand error recovery strategies
3. **[Retry Options](./retries.md)** ← Fine-tune retry behavior
4. **[Materialization & Buffering](./materialization.md)** ← Understand how replay works
5. **[Troubleshooting](./troubleshooting.md)** ← Debug issues

## Advanced Topics

- **[Circuit Breakers](./circuit-breakers.md)** - Prevent cascading failures
- **[Dead-Letter Queues](./dead-letter-queues.md)** - Handle problematic items

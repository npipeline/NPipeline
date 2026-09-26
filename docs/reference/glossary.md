---
title: "Glossary"
description: "Definitions for all NPipeline-specific and data pipeline terminology."
order: 2
---

# Glossary

Quick reference for terminology used throughout NPipeline documentation. Terms are listed alphabetically.

---

### At-Least-Once Delivery

A guarantee that each item is processed one or more times. NPipeline targets at-least-once delivery - combine with [idempotent](#idempotent) operations to ensure correctness when retries produce duplicates.

### Backpressure

Flow control that prevents upstream nodes from producing data faster than downstream nodes can consume it. NPipeline manages backpressure automatically through async enumeration - a source node only produces the next item when the downstream transform requests it. The [parallelism extension](../extensions/index.md) provides configurable queue policies for parallel execution.

### Circuit Breaker

A fault-tolerance pattern that stops calling an unhealthy dependency once transient failures pass a threshold, either a number of consecutive failures or a failure rate. Has three states: **Closed** (normal), **Open** (all attempts blocked), and **Half-Open** (limited test attempts). See [Circuit Breakers](../error-handling/circuit-breakers.md).

### Connector

A NuGet package providing pre-built source and sink nodes for a specific data system (database, file format, message queue, or API). Examples: `NPipeline.Connectors.Csv`, `NPipeline.Connectors.Kafka`. See [Extensions](../extensions/index.md).

### DAG (Directed Acyclic Graph)

A graph where edges have direction and no cycles exist. NPipeline pipelines are DAGs - data flows in one direction through the graph, and you cannot create circular dependencies. The runtime validates this at build time.

### Dead Letter

An item that failed processing after exhausting all retry attempts. Dead-lettered items are routed to an `IDeadLetterSink` for later inspection or reprocessing. See [Dead-Letter Queues](../error-handling/dead-letter-queues.md).

### Event Time

The timestamp of when an event logically occurred, as opposed to when the pipeline received it. Used in windowed aggregations to group events by their real-world time. See [watermark](#watermark).

### Execution Strategy

An `IExecutionStrategy` implementation that controls how a transform node processes its input stream. Built-in strategies: `SequentialExecutionStrategy` (one item at a time) and `BatchingExecutionStrategy` (buffer then process). The [parallelism extension](../extensions/index.md) adds parallel strategies. A strategy that implements `IResumableExecutionStrategy` supports [node restart](#node-restart).

### Idempotent

A property of operations that produce the same result whether applied once or multiple times. Critical for correctness with [at-least-once delivery](#at-least-once-delivery) - if an item is retried, an idempotent sink (e.g., upsert instead of insert) prevents duplicate records.

### Item

A single data unit flowing through the pipeline. Items are the individual objects in a [stream](#stream). In code, an item is a strongly-typed instance of `T` in `IDataStream<T>`.

### Jitter

Randomization added to retry delays to prevent the [thundering herd problem](#thundering-herd-problem). A `RetryBackoff` applies one of three `RetryJitter` modes: `Full`, `Equal`, or `None`. For more information, see [Retry strategies](../error-handling/retry-strategies.md).

### Lineage

Data provenance tracking that records how each item was transformed as it passed through the pipeline. Captures hop timestamps, decision outcomes (emitted, filtered, joined), and cardinality. Requires the `NPipeline.Extensions.Lineage` package.

When item-level lineage is enabled, items are wrapped in `LineagePacket<T>` at the source. The effective runtime [stream](#stream) item type throughout execution is `LineagePacket<T>`. Route predicates and merge strategies are normalized to operate on this wrapped type during bind time; node implementations still receive unwrapped `T` values.

### Node

A processing unit in the pipeline graph. Three primary types: **Source** (produces data), **Transform** (modifies data item-by-item), and **Sink** (consumes data). Additional types include Branch, Tap, Join, Aggregate, Lookup, Batch, and Composite. See [Key Concepts](../getting-started/key-concepts.md).

### Node Restart

The second resilience layer (L2). When a transform node's output stream fails, node restart runs the node again from its *checkpoint*, the first input item whose outcome wasn't delivered, so earlier items aren't processed again. Enable it by setting `NodeRestart = new NodeRestartOptions { MaxRestarts = 3 }` in the node's resilience options. The node's execution strategy must implement `IResumableExecutionStrategy`. A restart happens when the resilience policy returns `ResilienceDecision.RestartNode`. For more information, see [Node restart and the replay window](../error-handling/materialization.md).

### Pipeline

A directed acyclic graph of [nodes](#node) connected by typed [streams](#stream). Defined by implementing `IPipelineDefinition` and executed by `PipelineRunner`. See [Key Concepts](../getting-started/key-concepts.md).

### Pipeline Context

The `PipelineContext` object that flows through all nodes during execution. Carries configuration, shared state (`Items` dictionary), resilience options, resilience policy, observability factories, and the cancellation token.

### Replay window

The input items a restartable transform holds between its checkpoint and the last item it read, so that a [node restart](#node-restart) can process them again. `NodeRestart.MaxReplayWindow` (default 10,000) bounds it. When the window is full, the node stops reading input until the checkpoint advances. The bound is [backpressure](#backpressure), never an error, and it doesn't limit the length of the input. For more information, see [Node restart and the replay window](../error-handling/materialization.md).

### Resilience Policy

An `IResiliencePolicy` implementation that decides how to handle failures. Returns a `ResilienceDecision` (Fail, Retry, Skip, DeadLetter, RestartNode, ContinueWithoutNode) for each failure type. See [Resilience Policies](../error-handling/resilience-policies.md).

### Sliding Window

A windowing strategy where windows overlap. Each window has a fixed duration and slides forward by a configurable interval. A single event may belong to multiple windows. Used for rolling averages and continuous metrics. See [tumbling window](#tumbling-window) for the non-overlapping alternative.

### Storage Provider

An `IStorageProvider` implementation that abstracts file system operations (read, write, list, delete) for a specific storage backend. Used by file-based connectors (CSV, JSON, Parquet, Excel) to transparently read from/write to S3, Azure Blob, GCS, SFTP, etc. See [Extensions](../extensions/index.md).

### Stream

A typed, asynchronous, lazy sequence of [items](#item) flowing between nodes. Represented by `IDataStream<T>`, which implements `IAsyncEnumerable<T>`. Streams are consumed item-by-item - data is not buffered in memory, apart from the bounded [replay window](#replay-window) that a restartable node holds.

At runtime, when item-level lineage is enabled, the stream item type `T` is `LineagePacket<TPayload>` rather than the raw payload type `TPayload`. The effective runtime item type is captured in each node's `RuntimeNodeStreamContract` by `RuntimePipelineBinder` before execution starts.

### Thundering Herd Problem

A failure pattern where many clients retry simultaneously after a shared dependency recovers, overwhelming it again. Solved by adding [jitter](#jitter) to retry delays so retries are spread across time.

### Tumbling Window

A windowing strategy where windows do not overlap. Each window covers a fixed duration, and each event belongs to exactly one window. Used for distinct time-period summaries (e.g., hourly counts). See [sliding window](#sliding-window) for the overlapping alternative.

### Watermark

A marker tracking event-time progress in windowed aggregations. The watermark represents the system's belief about the latest event time - when it advances past a window's end time, that window closes. It trails the latest event time by `MaxOutOfOrderness` (in `AggregateNodeConfiguration`, or the windowed join's constructor) and is re-evaluated on every item.

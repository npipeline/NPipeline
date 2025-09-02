---
title: Pipeline Execution
description: Understand how NPipeline executes data flows and manages pipeline lifecycle.
sidebar_position: 1
slug: /core-concepts/pipeline-execution
---

# Pipeline Execution

Once you have defined your pipeline using `IPipelineDefinition`, the next step is to execute it. In NPipeline, pipeline execution is managed by the `PipelineRunner` class. This section guides you through the process of executing your NPipeline pipelines, from basic execution to advanced error handling and recovery strategies.

For a deeper understanding of how pipeline execution works under the hood, see the [Architecture: Component Architecture](../../architecture/component-architecture.md#4-pipeline-runner) section.

## Quick Start: Basic Execution

The simplest way to execute a pipeline is with the `PipelineRunner`:

```csharp
var runner = new PipelineRunner();
await runner.RunAsync<MyPipelineDefinition>();
```

### PipelineRunner Overloads

The `PipelineRunner` class provides three overload patterns for convenience:

```csharp
// Overload 1: With PipelineContext only
public Task RunAsync<TDefinition>(PipelineContext context) 
    where TDefinition : IPipelineDefinition, new();

// Overload 2: With CancellationToken only (uses default context)
public Task RunAsync<TDefinition>(CancellationToken cancellationToken = default) 
    where TDefinition : IPipelineDefinition, new();

// Overload 3: With both PipelineContext and CancellationToken
public Task RunAsync<TDefinition>(PipelineContext context, CancellationToken cancellationToken) 
    where TDefinition : IPipelineDefinition, new();
```

## Execution with Pipeline Context

You can pass state and configuration to your pipeline using `PipelineContext`:

```csharp
var context = new PipelineContext(PipelineContextConfiguration.WithCancellation(cancellationToken));
context.Items["key"] = "value"; // Pass state to nodes

var runner = new PipelineRunner();
await runner.RunAsync<MyPipelineDefinition>(context);
```

## Cancellation

NPipeline supports cancellation through `CancellationToken` propagation. Pass a cancellation token to `RunAsync` to enable graceful shutdown:

```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

var runner = new PipelineRunner();
try
{
    await runner.RunAsync<MyPipelineDefinition>(cts.Token);
}
catch (OperationCanceledException)
{
    Console.WriteLine("Pipeline was cancelled.");
}
```

The cancellation token is propagated to all nodes, allowing them to clean up resources gracefully. See [Cancellation Model](../../architecture/cancellation-model.md) for more details.

## Key Aspects of Pipeline Execution

Pipeline execution involves several key aspects:

* **Starting and Stopping**: How to initiate and gracefully terminate a pipeline.
* **Concurrency**: How NPipeline handles parallel processing of data items.
* **Error Handling**: Mechanisms for dealing with errors that occur during execution.
* **Monitoring**: Observing the flow of data and the performance of your pipeline.

## Topics in this Section

* **[`IPipelineRunner`](ipipelinerunner.md)**: The primary interface for running pipelines.
* **[Execution Strategies](execution-strategies.md)**: Different strategies for executing your pipeline (Sequential vs. Resilient).
* **[Error Handling](../resilience/error-handling-guide.md)**: Comprehensive error handling mechanisms at both node and pipeline levels.
* **[Retry Configuration](../resilience/retry-configuration.md)**: Configure how your pipeline should respond to transient failures.
* **[Circuit Breaker Configuration](../resilience/circuit-breaker-configuration.md)**: Prevent cascading failures with circuit breaker patterns.
* **[Dead-Letter Queues](../resilience/dead-letter-queues.md)**: Handle and store items that cannot be processed.

## Next Steps

* **[Pipeline Context](../pipeline-context.md)** - Learn how to pass state and configuration to your pipeline
* **[Architecture: Execution Flow](../../architecture/execution-flow.md)** - Deep dive into sequential and parallel execution
* **[Architecture: Cancellation Model](../../architecture/cancellation-model.md)** - Understand cancellation propagation
* **[Error Handling](../resilience/error-handling-guide.md)** - Learn about error handling strategies

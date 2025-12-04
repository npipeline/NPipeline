---
title: Resilience Quick Start
description: Get started quickly with resilience in NPipeline. Covers node restarts and retry delay strategies.
sidebar_position: 2
---

# Resilience Quick Start

This guide provides quick-start checklists and patterns for the two most common resilience scenarios: node restarts and retry delay strategies.

## Node Restart - Quick Start Checklist

**:warning: BEFORE YOU USE `PipelineErrorDecision.RestartNode`, READ THIS.**

Node restart is a powerful resilience feature, but it requires **three mandatory prerequisites**. Missing even one silently disables the feature.

If you've experienced mysterious pipeline failures where restart seemed enabled but didn't work, one of these requirements was missing.

> **:bulb: Tip:** The NPipeline build-time analyzer automatically detects incomplete restart configurations at compile time. Enable [NP9002](../../tooling/analyzers/resilience.md) to catch these issues before deployment.

---

### The Three-Step Mandatory Checklist

#### :heavy_check_mark: STEP 1: Apply ResilientExecutionStrategy

Your node must be wrapped with `ResilientExecutionStrategy`. This enables the restart capability at the node level.

**What it does:** Allows the pipeline to restart the node when an error occurs.

**Without it:** Restart decisions are ignored; the node cannot recover.

**How to configure:**

```csharp
var nodeHandle = builder
    .AddTransform<MyTransform, Input, Output>("myNode")
    .WithExecutionStrategy(
        builder,
        new ResilientExecutionStrategy(new SequentialExecutionStrategy())
    );
```

**Learn more:** [Execution Strategies](../pipeline-execution/execution-strategies.md)

---

#### :heavy_check_mark: STEP 2: Configure Maximum Restart Attempts

Set `MaxNodeRestartAttempts > 0` in `PipelineRetryOptions`. This tells the pipeline how many times to attempt restarting a failed node.

**What it does:** Limits how many restart attempts the pipeline will make before giving up.

**Without it:** No restarts will be attempted.

**How to configure:**

```csharp
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,  // ← At least 1
    MaxMaterializedItems: 1000   // (see Step 3!)
);

var context = PipelineContext.WithRetry(options);
```

**Recommended starting values:**

- **Transient failures (network, temporary service issues):** 2-3 attempts
- **Persistent issues (resource exhaustion):** 3-5 attempts
- **Critical nodes:** 5+ attempts

**Learn more:** [Retry Configuration](./retries.md)

---

#### :heavy_check_mark: STEP 3: Enable Input Materialization (:warning: CRITICAL)

Set `MaxMaterializedItems` to a **non-null, positive number** on the input to the node you want to be restartable. This is the replay buffer.

**What it does:** Buffers items from the input source so the node can be replayed from a known state if it fails.

**:warning: CRITICAL ISSUE:** If `MaxMaterializedItems` is `null` (unbounded), the system silently falls back to `FailPipeline`, even if you've configured restart logic. Your entire pipeline crashes instead of just restarting the node.

**How to configure:**

```csharp
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxMaterializedItems: 1000  // ← Must be set! Not null!
);
```

**Choosing a value:**

| Scenario | Recommended Value | Rationale |
|----------|-------------------|-----------|
| Small objects (< 100 bytes) | 5,000-10,000 | Minimal memory overhead |
| Medium objects (100-1KB) | 1,000-5,000 | Balanced buffer size |
| Large objects (1KB+) | 500-1,000 | Respect memory constraints |
| Critical/high-priority node | 2,000-5,000 | More replay buffer = higher restart success rate |

### Conservative Starting Point

Here's a good default configuration:

```csharp
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxMaterializedItems: 1000  // ← Good default: buffers ~1000 items
);
```

**Learn more:** [Materialization and Buffering](./materialization.md)

---

#### :warning: CRITICAL WARNING: Unbounded Materialization

**Never set `MaxMaterializedItems` to `null`:**

```csharp
// :x: WRONG - This disables restart silently!
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxMaterializedItems: null  // ← DANGER!
);
```

**If you do:**

- **Your pipeline will NOT restart on failures**
- **The system silently falls back to `FailPipeline`**
- **Your entire pipeline will crash** (not just the failing node)
- **Risk of Out-of-Memory exceptions** with unbounded data streams

**Symptom:** You've configured restart logic, but when an error occurs, the pipeline fails completely instead of restarting the node.

### Why Unbounded Memory Buffers Break Resilience Guarantees

Unbounded materialization (`MaxMaterializedItems: null`) creates a fundamental contradiction in the resilience model:

1. **Memory Safety vs. Recovery Trade-off**: Unbounded buffers can consume all available memory, causing OutOfMemoryException that cannot be recovered from. This defeats the purpose of resilience.

2. **Silent Failure Mode**: When the system detects unbounded materialization with a RestartNode decision, it cannot safely buffer items for replay. Instead of risking memory exhaustion, it silently falls back to `FailPipeline` to protect the system.

3. **Unpredictable Behavior**: In production, unbounded buffers lead to unpredictable memory usage patterns that can cause cascading failures across the entire system.

4. **Resource Contention**: Unbounded buffers compete with other processes for memory, potentially causing system-wide instability.

**The Design Philosophy**: NPipeline prioritizes system stability over incomplete recovery. An unbounded buffer represents an undefined recovery boundary, making safe restart impossible. By requiring explicit buffer limits, NPipeline ensures that restart operations have predictable memory footprints and can be safely executed.

**Choosing Not to Set a Memory Cap = Choosing Complete Pipeline Failure**

When you set `MaxMaterializedItems: null`, you are making an explicit choice to sacrifice restart capability in favor of unlimited buffering. This means:

- **You accept that RestartNode will not work**
- **You accept that your pipeline will fail completely on node errors**
- **You accept the risk of OutOfMemoryException**

If you need node restart functionality, you **must** set a memory cap. The system cannot provide resilience guarantees without defined resource boundaries.

---

### Complete Configuration Example

Here's a complete example with all three requirements:

```csharp
// Step 1: Create retry options with ALL three settings
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,           // ← Step 2: Restart attempts
    MaxMaterializedItems: 1000            // ← Step 3: Replay buffer
);

var context = PipelineContext.WithRetry(options);

// Step 2: Build pipeline with resilient nodes
var definition = new MyPipelineDefinition();
definition.Define(builder, context);

// In MyPipelineDefinition.Define():
// 
//   Step 1: Wrap node with ResilientExecutionStrategy
//   var nodeHandle = builder
//       .AddTransform<MyTransform, Input, Output>("risky")
//       .WithExecutionStrategy(
//           builder,
//           new ResilientExecutionStrategy(
//               new SequentialExecutionStrategy()
//           )
//       );
//
//   Add error handler that returns RestartNode
//   builder.AddPipelineErrorHandler<MyErrorHandler>();
//
// In MyErrorHandler.HandleNodeFailureAsync():
//
//   return error switch
//   {
//       TimeoutException => PipelineErrorDecision.RestartNode,
//       _ => PipelineErrorDecision.FailPipeline
//   };
```

---

## Retry Delay Quick Start

Retry delays are essential for building resilient pipelines that can recover from transient failures without overwhelming downstream services.

### Quick Start: Common Retry Patterns

#### Web API Calls (Most Common)

```csharp
// Recommended for external API calls
context.UseExponentialBackoffDelay(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromMinutes(1));
```

**Why this pattern:**
- Exponential backoff gives services time to recover
- Prevents thundering herd problems
- Balanced for typical API rate limits

#### Database Operations

```csharp
// Recommended for database retry scenarios
context.UseLinearBackoffDelay(
    baseDelay: TimeSpan.FromMilliseconds(100),
    increment: TimeSpan.FromMilliseconds(200),
    maxDelay: TimeSpan.FromSeconds(5));
```

**Why this pattern:**
- Linear growth is predictable for database connection pools
- Shorter delays work well for transient lock contention
- Conservative max delay prevents long-running transactions

#### File Processing

```csharp
// Recommended for file system operations
context.UseFixedDelay(TimeSpan.FromSeconds(2));
```

**Why this pattern:**
- File system recovery is typically immediate
- Fixed delay provides predictable behavior
- Simple and effective for I/O operations

### Decision Flow for Choosing Retry Strategy

```mermaid
flowchart TD
    A[What type of operation?] --> B{External Service?}
    B -->|Yes| C[Use Exponential Backoff]
    B -->|No| D{Database Operation?}
    D -->|Yes| E[Use Linear Backoff]
    D -->|No| F{File System?}
    F -->|Yes| G[Use Fixed Delay]
    F -->|No| H[Use Exponential Backoff<br>Default choice]
    
    C --> I[Configure base delay: 1s<br>Configure multiplier: 2.0<br>Configure max delay: 1m]
    E --> J[Configure base delay: 100ms<br>Configure increment: 200ms<br>Configure max delay: 5s]
    G --> K[Configure delay: 2s]
    H --> I
    
    style A fill:#e1f5fe
    style C fill:#e8f5e9
    style E fill:#e8f5e9
    style G fill:#e8f5e9
    style I fill:#fff3e0
    style J fill:#f3e5f5
    style K fill:#f3e5f5
```

### Implementation Examples

#### Basic Pipeline with Retry Delays

```csharp
using NPipeline;
using NPipeline.Pipeline;

public sealed class RetryQuickstartPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Configure retry delay strategy based on operation type
        context.UseExponentialBackoffDelay(
            baseDelay: TimeSpan.FromSeconds(1),
            multiplier: 2.0,
            maxDelay: TimeSpan.FromMinutes(1));

        var source = builder.AddSource<ApiSource, ApiResponse>("api-source");
        var transform = builder.AddTransform<DataTransform, ApiResponse, ProcessedData>("transform");
        var sink = builder.AddSink<DataSink, ProcessedData>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Configure retry options
        builder.WithRetryOptions(new PipelineRetryOptions(
            MaxItemRetries: 3,
            MaxNodeRestartAttempts: 2,
            MaxSequentialNodeAttempts: 5
        ));
    }
}
```

#### Per-Node Retry Configuration

```csharp
public void Define(PipelineBuilder builder, PipelineContext context)
{
    var source = builder.AddSource<DataSource, string>("source");
    
    // Fast retries for in-memory operations
    context.UseFixedDelay(TimeSpan.FromMilliseconds(50));
    var memoryTransform = builder.AddTransform<MemoryTransform, string, string>("memory-ops");
    
    // Slower retries for external API calls
    context.UseExponentialBackoffDelay(
        baseDelay: TimeSpan.FromSeconds(1),
        multiplier: 2.0,
        maxDelay: TimeSpan.FromMinutes(1));
    var apiTransform = builder.AddTransform<ApiTransform, string, string>("api-ops");
    
    // Default retries for other operations
    var sink = builder.AddSink<DataSink, string>("sink");

    builder.Connect(source, memoryTransform);
    builder.Connect(memoryTransform, apiTransform);
    builder.Connect(apiTransform, sink);
}
```

### Common Pitfalls to Avoid

> :warning: **Common Mistake**: Not configuring retry delays at all
> 
> Without retry delays, failed operations retry immediately, which can overwhelm recovering services.

> :warning: **Common Mistake**: Using exponential backoff for in-memory operations
> 
> In-memory operations typically recover immediately, so exponential backoff adds unnecessary delays.

> :warning: **Common Mistake**: Setting max delay too high
> 
> Very high max delays can cause long recovery times. Consider your SLA requirements.

> :bulb: **Pro Tip**: Always test retry behavior in development
> 
> Use fixed delays in tests for predictable behavior, then switch to appropriate backoff in production.

## Next Steps

- **[Error Handling](./error-handling.md)** - Comprehensive error handling strategies
- **[Retry Configuration](./retries.md)** - Detailed configuration options and advanced patterns
- **[Circuit Breakers](./circuit-breakers.md)** - Preventing cascading failures
- **[Troubleshooting](./troubleshooting.md)** - Common issues and solutions

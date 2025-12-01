---
title: Node Restart - Quick Start Checklist
description: Three-step mandatory checklist for configuring node restarts. Read this before you enable RestartNode error handling.
sidebar_position: 2
---

## Node Restart - Quick Start Checklist

**⚠️ BEFORE YOU USE `PipelineErrorDecision.RestartNode`, READ THIS.**

Node restart is a powerful resilience feature, but it requires **three mandatory prerequisites**. Missing even one silently disables the feature.

If you've experienced mysterious pipeline failures where restart seemed enabled but didn't work, one of these requirements was missing.

> **💡 Tip:** The NPipeline build-time analyzer automatically detects incomplete restart configurations at compile time. Enable [NP9002](../../reference/error-codes.md#npl9002-incomplete-resilient-configuration) to catch these issues before deployment.

---

## The Three-Step Mandatory Checklist

### ✅ STEP 1: Apply ResilientExecutionStrategy

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

**Learn more:** [Resilient Execution Strategy](./execution-with-resilience.md)

---

### ✅ STEP 2: Configure Maximum Restart Attempts

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

**Learn more:** [Configuration Guide](./configuration-guide.md)

---

### ✅ STEP 3: Enable Input Materialization (⚠️ CRITICAL)

Set `MaxMaterializedItems` to a **non-null, positive number** on the input to the node you want to be restartable. This is the replay buffer.

**What it does:** Buffers items from the input source so the node can be replayed from a known state if it fails.

**⚠️ CRITICAL ISSUE:** If `MaxMaterializedItems` is `null` (unbounded), the system silently falls back to `FailPipeline`, even if you've configured restart logic. Your entire pipeline crashes instead of just restarting the node.

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

**Learn more:** [Materialization and Buffering](./materialization-and-buffering.md)

---

## ⚠️ CRITICAL WARNING: Unbounded Materialization

**Never set `MaxMaterializedItems` to `null`:**

```csharp
// ❌ WRONG - This disables restart silently!
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

## Complete Configuration Example

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
//       TimeoutException => Task.FromResult(
//           PipelineErrorDecision.RestartNode  // ← RestartNode requires all 3!
//       ),
//       _ => Task.FromResult(PipelineErrorDecision.FailPipeline)
//   };

var runner = PipelineRunner.Create();
await runner.RunAsync<MyPipelineDefinition>(context);
```

---

## Verification Checklist

Before you assume restarts are working, verify all three:

- [ ] **Strategy:** Node is wrapped with `ResilientExecutionStrategy`
- [ ] **Attempts:** `MaxNodeRestartAttempts > 0` in `PipelineRetryOptions`
- [ ] **Materialization:** `MaxMaterializedItems` is set to a positive number (not null)
- [ ] **Error Handler:** Handler returns `PipelineErrorDecision.RestartNode` for your error type

If any are missing, restarts won't work.

---

## What Happens If One Is Missing

| Missing Component | Symptom | What Happens |
|---|---|---|
| **ResilientExecutionStrategy** | Node not wrapped | Error handler decisions are ignored; pipeline always fails |
| **MaxNodeRestartAttempts** | Not configured or = 0 | No restarts are attempted; pipeline fails immediately |
| **MaxMaterializedItems** | Not set (null) | RestartNode falls back to `FailPipeline`; **entire pipeline crashes** |
| **Error Handler** | Returns `FailPipeline` for all errors | Restarts never triggered; all errors kill the pipeline |

---

## Next Steps

- **[Build-Time Resilience Analyzer](../../analyzers/resilience.md):** Catch incomplete configurations at compile time
- **[Resilient Execution Strategy](./execution-with-resilience.md):** Deep dive into how ResilientExecutionStrategy works
- **[Materialization and Buffering](./materialization-and-buffering.md):** Understand the replay buffer and memory implications
- **[Error Handling](./error-handling-guide.md):** Learn how to write error handlers that return RestartNode
- **[Troubleshooting](./troubleshooting.md):** Diagnose restart issues in your pipeline

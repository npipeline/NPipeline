---
title: Configuration Guide
description: Step-by-step workflow for configuring resilience in NPipeline.
sidebar_position: 5
---

# Configuration Guide

This guide provides a structured workflow for configuring resilience in NPipeline. For comprehensive code examples with real-world scenarios, see [Common Configuration Patterns](execution-with-resilience.md#common-configuration-patterns).

**For detailed step-by-step instructions on configuring node restart functionality, see the [Node Restart Quick Start Checklist](./node-restart-quickstart.md).**

## Configuration Workflow

To properly configure resilience for your pipeline, follow these steps:

### Step 1: Choose Your Execution Strategy

| Strategy | Use Case |
|----------|----------|
| Sequential | Simple processing, order matters |
| Parallel | CPU/IO-bound, high throughput |

Wrap with `ResilientExecutionStrategy`:

```csharp
var nodeHandle = builder
    .AddTransform<MyTransform, Input, Output>("myNode")
    .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
        new SequentialExecutionStrategy()
    ));
```

### Step 2: Configure Retry Options

```csharp
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxMaterializedItems: 1000
);

var context = PipelineContext.WithRetry(options);
```

**Decision Matrix:**

| Scenario | MaxItemRetries | MaxNodeRestartAttempts | MaxMaterializedItems |
|----------|---|---|---|
| Stable internal services | 1-2 | 1 | 500-1000 |
| Unstable external APIs | 3-5 | 3-5 | 1000-2000 |
| High-volume streaming | 1 | 2 | 5000-10000 |
| Critical business logic | 3-5 | 5 | 2000-5000 |

**⚠️ Critical Warning**: Setting `MaxMaterializedItems` to `null` (unbounded) silently disables node restart functionality. For detailed explanation of why unbounded buffers break resilience guarantees, see the [Node Restart Quick Start Checklist](./node-restart-quickstart.md#why-unbounded-memory-buffers-break-resilience-guarantees).

### Step 3: Configure Circuit Breaker (Optional)

Protect against cascading failures by tripping circuit breaker when failures exceed a threshold:

```csharp
// Basic configuration
builder.WithCircuitBreaker(
    failureThreshold: 5,
    openDuration: TimeSpan.FromMinutes(1),
    samplingWindow: TimeSpan.FromMinutes(5)
);

// For high-volume or long-running pipelines, tune memory cleanup
builder.ConfigureCircuitBreakerMemoryManagement(opts =>
    opts with
    {
        InactivityThreshold = TimeSpan.FromMinutes(10),
        MaxTrackedCircuitBreakers = 500
    }
);
```

See [Circuit Breaker Advanced Configuration](circuit-breaker-advanced-configuration.md) for detailed guidance on threshold types and memory management.

### Step 4: Configure Error Handling

```csharp
public class CustomErrorHandler : IPipelineErrorHandler
{
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId, Exception error, PipelineContext context, CancellationToken ct)
    {
        return error switch
        {
            TimeoutException => Task.FromResult(PipelineErrorDecision.RestartNode),
            _ => Task.FromResult(PipelineErrorDecision.FailPipeline)
        };
    }
}
```

### Step 5: Put It All Together

```csharp
public class MyPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var criticalHandle = builder
            .AddTransform<CriticalTransform, Input, Output>("criticalNode")
            .WithExecutionStrategy(builder, 
                new ResilientExecutionStrategy(new SequentialExecutionStrategy()));
        // ... add other nodes
        
        builder.AddPipelineErrorHandler<CustomErrorHandler>();
    }
}

var runner = new PipelineRunner();
await runner.RunAsync<MyPipelineDefinition>(context);
await runner.RunAsync(pipeline, context);
```

## Real-World Examples

See [Common Configuration Patterns](execution-with-resilience.md#common-configuration-patterns):

- E-commerce Order Processing
- High-Volume Data Analytics  
- Microservice Integration

## Common Mistakes

For avoiding configuration errors, see [Common Configuration Mistakes to Avoid](execution-with-resilience.md#common-configuration-mistakes-to-avoid).

## Next Steps

- **[Node Restart Quick Start Checklist](./node-restart-quickstart.md)**: Complete step-by-step configuration guide for node restart functionality
- [Resilient Execution Strategy](execution-with-resilience.md) - Learn how ResilientExecutionStrategy works
- [Materialization and Buffering](materialization-and-buffering.md) - Understand memory management
- [Troubleshooting](troubleshooting.md) - Diagnose configuration issues

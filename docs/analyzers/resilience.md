---
title: Resilience Analyzers
description: Detect incomplete resilience configuration that could lead to silent failures and unexpected behavior.
sidebar_position: 2
---

## Resilience Analyzers

Resilience analyzers protect against incomplete error handling configuration that can cause silent failures, where error recovery appears to succeed but silently fails at runtime.

### NP9002: Incomplete Resilient Configuration

**ID:** `NP9002`  
**Severity:** Warning  
**Category:** Resilience  

This analyzer detects when an error handler can return `PipelineErrorDecision.RestartNode` but is missing one or more of the three mandatory prerequisites:

1. **ResilientExecutionStrategy** wrapping the node
2. **MaxNodeRestartAttempts > 0** in PipelineRetryOptions
3. **MaxMaterializedItems != null** in PipelineRetryOptions

#### The Problem

Before this analyzer existed, developers could easily miss one of the three mandatory prerequisites for node restart:

```csharp
// ❌ User's Code - Looks correct but is missing prerequisites
public class MyErrorHandler : IPipelineErrorHandler
{
    public async Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        return error switch
        {
            TimeoutException => PipelineErrorDecision.RestartNode,  // ← Intent is clear
            _ => PipelineErrorDecision.FailPipeline
        };
    }
}

// But at runtime, restart silently fails because prerequisites are missing!
// The entire pipeline crashes instead of just restarting the node.
```

#### With the Analyzer

```text
CSC : warning NP9002: Error handler can return PipelineErrorDecision.RestartNode 
but the node may not have all three mandatory prerequisites configured...
```

This gets flagged immediately at build time, before deployment.

#### Solution: Complete Configuration

```csharp
// ✅ CORRECT: All three prerequisites configured

// 1. Wrap node with ResilientExecutionStrategy
var node = new MyTransformNode();
var resilientNode = new ResilientExecutionStrategy(node);

// 2. Configure retry options with MaxNodeRestartAttempts > 0
var retryOptions = new PipelineRetryOptions
{
    MaxNodeRestartAttempts = 3,
    MaxMaterializedItems = 10000
};

// 3. Provide error handler that can return RestartNode
var pipeline = new PipelineBuilder<string>()
    .AddNode(resilientNode)
    .WithErrorHandler(new MyErrorHandler())
    .WithRetryOptions(retryOptions)
    .Build();
```

#### Best Practices

1. **Always configure all three prerequisites together** - They work as a unit
2. **Use ResilientExecutionStrategy consistently** - Apply it to all nodes that need restart capability
3. **Set realistic MaxNodeRestartAttempts** - Usually 2-3 attempts is sufficient
4. **Configure MaxMaterializedItems appropriately** - Balance memory usage with retry capability

## See Also

- [Resilience Configuration Guide](../core-concepts/resilience/configuration-guide.md)
- [Error Handling Architecture](../architecture/error-handling-architecture.md)
- [Cancellation Model](../architecture/cancellation-model.md)

---
title: "Pipeline Validation"
description: "Catch configuration errors at build time with graph validation rules and analyzers."
order: 14
---

# Pipeline Validation

> **Prerequisites:** [Defining Pipelines](defining-pipelines.md)

NPipeline validates your pipeline graph when you call `Build()`. Validation catches structural errors - disconnected nodes, type mismatches, cycles - before execution begins.

## Validation Modes

Control validation behavior with `WithValidationMode`:

```csharp
// Default: throw PipelineValidationException on errors
builder.WithValidationMode(GraphValidationMode.Error);

// Log warnings but allow execution
builder.WithValidationMode(GraphValidationMode.Warn);

// Skip validation entirely (not recommended)
builder.WithValidationMode(GraphValidationMode.Off);
```

## Built-In Rules

### Core Rules (Always Run)

| Rule | Stops Build | What It Checks |
|------|:-----------:|----------------|
| `UniqueNodeNameRule` | Yes | No duplicate node names |
| `DuplicateNodeIdRule` | Yes | No duplicate node IDs |
| `EdgeReferenceRule` | Yes | All edges reference existing nodes |
| `SourceAndReachabilityRule` | No | At least one source; all nodes reachable |
| `CycleDetectionRule` | No | Graph is a DAG (no cycles) |

### Extended Rules (Opt-Out)

Extended rules add configuration and best-practice checks:

| Rule | What It Checks |
|------|----------------|
| `MissingSinkRule` | At least one sink node exists |
| `SelfLoopRule` | No edges from a node to itself |
| `DuplicateEdgeRule` | No duplicate edges between same node pair |
| `TypeCompatibilityRule` | Output type is assignable to downstream input type |
| `ResilienceConfigurationRule` | Resilient nodes have proper retry/materialization config |
| `ParallelConfigurationRule` | Parallel nodes have bounded queues and reasonable DOP |

Disable extended rules if needed:

```csharp
builder.WithoutExtendedValidation();
```

## Pre-Build Validation

Validate without building:

```csharp
PipelineValidationResult result = builder.Validate();

if (!result.IsValid)
{
    foreach (var error in result.Errors)
        Console.WriteLine($"Error: {error}");
    foreach (var warning in result.Warnings)
        Console.WriteLine($"Warning: {warning}");
}
```

Check if a specific connection is valid:

```csharp
if (builder.CanConnect(source, target, out string? reason))
    builder.Connect(source, target);
else
    Console.WriteLine($"Cannot connect: {reason}");
```

## Safe Build with TryBuild

`TryBuild` returns `false` instead of throwing on validation failures:

```csharp
if (builder.TryBuild(out var pipeline, out var validationResult))
{
    await runner.RunAsync(pipeline);
}
else
{
    foreach (var issue in validationResult.Issues)
        Console.WriteLine($"[{issue.Severity}] {issue.Message}");
}
```

## Custom Validation Rules

Add project-specific rules by implementing `IGraphRule`:

```csharp
public class RequireNamingConventionRule : IGraphRule
{
    public string Name => "Naming Convention";
    public bool StopOnError => false;

    public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
    {
        foreach (var node in context.Graph.Nodes)
        {
            if (!node.Name.Contains('-'))
            {
                yield return new ValidationIssue(
                    ValidationSeverity.Warning,
                    $"Node '{node.Name}' should use kebab-case naming",
                    "Convention");
            }
        }
    }
}

builder.WithValidationRule(new RequireNamingConventionRule());
```

## Pipeline Visualization

Inspect the pipeline graph structure:

```csharp
// Generate a Mermaid diagram
string mermaid = builder.ToMermaidDiagram();
Console.WriteLine(mermaid);

// Text description of the graph
string description = builder.Describe();
```

> 💡 **Tip:** Roslyn analyzers (NP9xxx) provide additional compile-time checks beyond graph validation. See [Analyzer Rules](../analyzers/index.md) for the full catalog.

## Next Steps

- [Defining Pipelines](defining-pipelines.md) - the builder API
- [Error Handling](../error-handling/index.md) - resilience configuration validation
- [Parallel Execution](parallel-execution.md) - parallel configuration validation

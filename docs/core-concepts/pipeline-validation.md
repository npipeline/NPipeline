# Pipeline Validation Extensions - Usage Guide

The `PipelineBuilderValidationExtensions` class provides fluent methods for validating and analyzing pipeline structures before building. This enables early error detection and helps with debugging complex pipelines.

## Available Methods

### 1. `Validate()` - Validate Pipeline Structure

Validates the current pipeline structure without building the complete pipeline.

```csharp
var builder = new PipelineBuilder()
    .AddSource<MySource, int>("source")
    .AddTransform<MyTransform, int, string>("transform")
    .AddSink<MySink, string>("sink");

builder.Connect("source", "transform");
builder.Connect("transform", "sink");

// Validate before building
var result = builder.Validate();
if (!result.IsValid)
{
    foreach (var error in result.Errors)
        Console.WriteLine($"Error: {error}");
}
else
{
    var pipeline = builder.Build(); // Safe to build
}
```

**Returns:** `PipelineValidationResult` containing:

- `IsValid`: Boolean indicating if all errors passed
- `Errors`: List of error messages
- `Warnings`: List of warning messages
- `Issues`: Complete list of validation issues with severity and category

### 2. `CanConnect<TData>()` - Pre-Check Connection Validity

Checks if a connection between two nodes is valid before adding it to the pipeline.

```csharp
var builder = new PipelineBuilder();
var source = builder.AddSource<MySource, int>("source");
var transform = builder.AddTransform<MyTransform, int, string>("transform");

// Check if connection is valid
if (builder.CanConnect(source, transform, out var reason))
{
    builder.Connect(source, transform);
}
else
{
    Console.WriteLine($"Cannot connect: {reason}");
}
```

**Checks:**

- Type compatibility between source output and target input
- Whether the connection would create a cycle
- Whether both nodes exist in the builder
- Self-loop detection

**Returns:** Boolean indicating validity, with optional reason string explaining why the connection is invalid.

### 3. `ToMermaidDiagram()` - Generate Visual Diagram

Generates a Mermaid flowchart diagram of the current pipeline structure.

```csharp
var builder = new PipelineBuilder()
    .AddSource<MySource, int>("source")
    .AddTransform<MyTransform, int, string>("transform")
    .AddSink<MySink, string>("sink");

builder.Connect("source", "transform");
builder.Connect("transform", "sink");

var mermaid = builder.ToMermaidDiagram();
Console.WriteLine(mermaid);

/* Output:
graph TD
    source["source : Source"]
    transform["transform : Transform"]
    sink["sink : Sink"]
    source --> transform
    transform --> sink
*/
```

**Use Cases:**

- Visualize complex pipelines in Mermaid Live Editor
- Generate documentation diagrams
- Debug pipeline structure visually
- Share pipeline designs in GitHub/documentation

### 4. `Describe()` - Get Text Description

Gets a human-readable textual description of the current pipeline structure.

```csharp
var builder = new PipelineBuilder()
    .AddSource<MySource, int>("source")
    .AddSink<MySink, string>("sink");

builder.Connect("source", "sink");

var description = builder.Describe();
Console.WriteLine(description);

/* Output:
Nodes:
  source | source | Source | MySource | In=-, Out=System.Int32
  sink | sink | Sink | MySink | In=System.String, Out=-

Edges:
  source --> sink
*/
```

**Use Cases:**

- Debugging and logging pipeline structures
- Text-based reports
- Console output for pipeline analysis

## Validation Results Structure

```csharp
public sealed record PipelineValidationResult(ImmutableList<ValidationIssue> Issues)
{
    // Built-in properties
    public bool IsValid => Issues.All(i => i.Severity != ValidationSeverity.Error);
    public ImmutableList<string> Errors => Issues
        .Where(i => i.Severity == ValidationSeverity.Error)
        .Select(i => i.Message)
        .ToImmutableList();
    public ImmutableList<string> Warnings => Issues
        .Where(i => i.Severity == ValidationSeverity.Warning)
        .Select(i => i.Message)
        .ToImmutableList();
}
```

## Validation Rules

### Core Rules (Always Applied)

- **UniqueNodeNameRule**: All node names must be unique
- **DuplicateNodeIdRule**: No duplicate node IDs
- **EdgeReferenceRule**: All edges reference valid nodes
- **SourceAndReachabilityRule**: Pipelines must have source nodes and all nodes must be reachable
- **CycleDetectionRule**: Pipeline graph must be acyclic (DAG)

### Extended Rules (Opt-In via `WithExtendedValidation()`)

- **MissingSinkRule**: Warnings if no sink nodes exist
- **SelfLoopRule**: Detects self-referencing nodes
- **DuplicateEdgeRule**: Prevents duplicate connections
- **MultiInboundNonJoinRule**: Warns if non-join nodes have multiple inputs
- **TypeCompatibilityRule**: Validates output/input type matching

## Complete Example

```csharp
public void BuildAndValidatePipeline()
{
    var builder = new PipelineBuilder();
    
    // Build pipeline
    var source = builder.AddSource<DataSource, int>("source");
    var transform = builder.AddTransform<Uppercase, int, string>("transform");
    var sink = builder.AddSink<ConsoleSink, string>("sink");
    
    // Validate before connecting
    if (!builder.CanConnect(source, transform, out var reason))
    {
        throw new InvalidOperationException($"Cannot connect: {reason}");
    }
    
    builder.Connect(source, transform);
    
    if (!builder.CanConnect(transform, sink, out reason))
    {
        throw new InvalidOperationException($"Cannot connect: {reason}");
    }
    
    builder.Connect(transform, sink);
    
    // Comprehensive validation
    var validationResult = builder.Validate();
    if (!validationResult.IsValid)
    {
        Console.WriteLine("Validation errors:");
        foreach (var error in validationResult.Errors)
            Console.WriteLine($"  - {error}");
        return;
    }
    
    // Generate visualization for documentation
    var diagram = builder.ToMermaidDiagram();
    File.WriteAllText("pipeline.mermaid", diagram);
    
    // Build and run
    var pipeline = builder.Build();
    // ... execute pipeline
}
```

## Error Handling Strategy

**Recommended Pattern:**

1. Use `CanConnect()` during pipeline construction to validate each connection
2. Use `Validate()` before final `Build()` for comprehensive validation
3. Use `ToMermaidDiagram()` to generate documentation
4. Use `Describe()` for logging and debugging

```csharp
try
{
    // Step 1: Build with connection validation
    if (!builder.CanConnect(source, transform, out var reason))
        throw new InvalidOperationException($"Invalid connection: {reason}");
    builder.Connect(source, transform);
    
    // Step 2: Final comprehensive validation
    var validation = builder.Validate();
    if (!validation.IsValid)
        throw new PipelineValidationException(validation);
    
    // Step 3: Safe to build
    var pipeline = builder.Build();
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Pipeline error: {ex.Message}");
    // Generate diagnostic report
    Console.Error.WriteLine(builder.Describe());
    throw;
}
```

## Performance Notes

- `Validate()` and `Describe()` construct a temporary `PipelineGraph` for analysis
- No graph freezing occurs - builder state remains mutable
- `CanConnect()` uses depth-first search for cycle detection (O(V+E) complexity)
- Suitable for development; consider caching results for repeated validations

## Integration with Build Validation

These extensions complement the standard `Build()` and `TryBuild()` methods:

- `Validate()` - Early, incremental validation
- `Build()` - Final validation with `GraphValidationMode` settings
- `TryBuild()` - Non-throwing validation wrapper

Use extensions during development; rely on `Build()` for final pipeline creation.

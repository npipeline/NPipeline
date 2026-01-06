# Nested Composition

## Overview

Nested composition allows composite nodes to contain other composite nodes, creating deep hierarchical pipeline structures. This enables building complex workflows from layers of simpler sub-pipelines.

## Basic Nesting

### Two-Level Nesting

The simplest form of nesting - a composite node within a parent pipeline:

```csharp
// Level 2: Inner sub-pipeline (multiplies by 2)
public class DoubleTransformPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<int>, int>("input");
        var transform = builder.AddTransform<DoubleTransform, int, int>("double");
        var output = builder.AddSink<PipelineOutputSink<int>, int>("output");
        
        builder.Connect(input, transform);
        builder.Connect(transform, output);
    }
}

// Level 1: Outer sub-pipeline (contains composite)
public class ProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<int>, int>("input");
        
        // Nested composite node
        var double = builder.AddComposite<int, int, DoubleTransformPipeline>("double");
        
        var output = builder.AddSink<PipelineOutputSink<int>, int>("output");
        
        builder.Connect(input, double);
        builder.Connect(double, output);
    }
}

// Level 0: Main pipeline
public class MainPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<IntSource, int>("source");
        
        // Top-level composite node
        var process = builder.AddComposite<int, int, ProcessingPipeline>("process");
        
        var sink = builder.AddSink<IntSink, int>("sink");
        
        builder.Connect(source, process);
        builder.Connect(process, sink);
    }
}
```

### Execution Flow

```
MainPipeline:
  [Source: 1, 2, 3] → [ProcessingPipeline] → [Sink]
                           ↓
                   ProcessingPipeline:
                     [Input] → [DoubleTransformPipeline] → [Output]
                                        ↓
                               DoubleTransformPipeline:
                                 [Input] → [Transform x2] → [Output]

Result: 2, 4, 6
```

## Deep Nesting

### Three or More Levels

Unlimited nesting depth is supported:

```csharp
// Level 3: Core processing
public class CorePipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Data>, Data>("input");
        var process = builder.AddTransform<CoreTransform, Data, Data>("process");
        var output = builder.AddSink<PipelineOutputSink<Data>, Data>("output");
        
        builder.Connect(input, process);
        builder.Connect(process, output);
    }
}

// Level 2: Validation + Core
public class ValidationProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Data>, Data>("input");
        
        var validate = builder.AddTransform<Validator, Data, Data>("validate");
        var core = builder.AddComposite<Data, Data, CorePipeline>("core");
        
        var output = builder.AddSink<PipelineOutputSink<Data>, Data>("output");
        
        builder.Connect(input, validate);
        builder.Connect(validate, core);
        builder.Connect(core, output);
    }
}

// Level 1: Enrichment + Validation + Core
public class EnrichmentPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Data>, Data>("input");
        
        var enrich = builder.AddTransform<Enricher, Data, Data>("enrich");
        var validateAndProcess = builder.AddComposite<Data, Data, ValidationProcessingPipeline>("validate-process");
        
        var output = builder.AddSink<PipelineOutputSink<Data>, Data>("output");
        
        builder.Connect(input, enrich);
        builder.Connect(enrich, validateAndProcess);
        builder.Connect(validateAndProcess, output);
    }
}

// Level 0: Main pipeline
public class MainPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DataSource, Data>("source");
        var process = builder.AddComposite<Data, Data, EnrichmentPipeline>("process");
        var sink = builder.AddSink<DataSink, Data>("sink");
        
        builder.Connect(source, process);
        builder.Connect(process, sink);
    }
}
```

## Context Propagation

### Inheritance Through Levels

Context inheritance can be configured at each level:

```csharp
// Level 2: Core - no inheritance
public class CorePipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Uses Default context - isolated
        var input = builder.AddSource<PipelineInputSource<T>, T>("input");
        var transform = builder.AddTransform<CoreTransform, T, T>("core");
        var output = builder.AddSink<PipelineOutputSink<T>, T>("output");
        
        builder.Connect(input, transform);
        builder.Connect(transform, output);
    }
}

// Level 1: Middle - inherits from Level 0
public class MiddlePipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<T>, T>("input");
        
        // Pass context to Level 2 with inheritance
        var core = builder.AddComposite<T, T, CorePipeline>(
            contextConfiguration: CompositeContextConfiguration.InheritAll);
        
        var output = builder.AddSink<PipelineOutputSink<T>, T>("output");
        
        builder.Connect(input, core);
        builder.Connect(core, output);
    }
}

// Level 0: Main - sets up initial context
public class MainPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DataSource, T>("source");
        
        // Inherit context to Level 1
        var middle = builder.AddComposite<T, T, MiddlePipeline>(
            contextConfiguration: CompositeContextConfiguration.InheritAll);
        
        var sink = builder.AddSink<DataSink, T>("sink");
        
        builder.Connect(source, middle);
        builder.Connect(middle, sink);
    }
}

// Usage
var context = new PipelineContext();
context.Parameters["Config"] = "value";

// Config propagates: Level 0 → Level 1 → Level 2
await runner.RunAsync<MainPipeline>(context);
```

### Selective Propagation

Different inheritance at each level:

```csharp
// Level 1: Inherits parameters only
builder.AddComposite<T, T, MiddlePipeline>(
    contextConfiguration: new CompositeContextConfiguration
    {
        InheritParentParameters = true,  // Pass params down
        InheritParentItems = false,      // Don't pass items
        InheritParentProperties = false
    });

// In MiddlePipeline, Level 2: Inherits everything from Level 1
builder.AddComposite<T, T, CorePipeline>(
    contextConfiguration: CompositeContextConfiguration.InheritAll);
```

## Common Patterns

### Pattern 1: Layered Architecture

Organize pipelines by responsibility layers:

```csharp
// Layer 3: Business Logic
public class BusinessLogicPipeline : IPipelineDefinition { }

// Layer 2: Validation + Business Logic
public class ValidationLayerPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<T>, T>("input");
        var validate = builder.AddTransform<Validator, T, T>("validate");
        var business = builder.AddComposite<T, T, BusinessLogicPipeline>("business");
        var output = builder.AddSink<PipelineOutputSink<T>, T>("output");
        
        builder.Connect(input, validate);
        builder.Connect(validate, business);
        builder.Connect(business, output);
    }
}

// Layer 1: Enrichment + Validation + Business Logic
public class EnrichmentLayerPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<T>, T>("input");
        var enrich = builder.AddTransform<Enricher, T, T>("enrich");
        var validateAndProcess = builder.AddComposite<T, T, ValidationLayerPipeline>("validate-process");
        var output = builder.AddSink<PipelineOutputSink<T>, T>("output");
        
        builder.Connect(input, enrich);
        builder.Connect(enrich, validateAndProcess);
        builder.Connect(validateAndProcess, output);
    }
}

// Layer 0: Orchestration
public class OrchestrationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DataSource, T>("source");
        var process = builder.AddComposite<T, T, EnrichmentLayerPipeline>("process");
        var sink = builder.AddSink<DataSink, T>("sink");
        
        builder.Connect(source, process);
        builder.Connect(process, sink);
    }
}
```

### Pattern 2: Recursive Processing

Process hierarchical data structures:

```csharp
// Process node and children
public class TreeProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<TreeNode>, TreeNode>("input");
        
        // Process current node
        var processNode = builder.AddTransform<NodeProcessor, TreeNode, TreeNode>("process-node");
        
        // Process children (recursive composite)
        var processChildren = builder.AddTransform<ChildrenProcessor, TreeNode, TreeNode>("process-children");
        
        var output = builder.AddSink<PipelineOutputSink<TreeNode>, TreeNode>("output");
        
        builder.Connect(input, processNode);
        builder.Connect(processNode, processChildren);
        builder.Connect(processChildren, output);
    }
}

// ChildrenProcessor creates composite nodes for each child
public class ChildrenProcessor : TransformNode<TreeNode, TreeNode>
{
    public override async Task<TreeNode> ExecuteAsync(TreeNode node, PipelineContext context, CancellationToken ct)
    {
        if (!node.HasChildren)
            return node;
            
        // Process each child with a nested pipeline
        foreach (var child in node.Children)
        {
            // Create and run sub-pipeline for child
            // (Implementation details omitted for brevity)
        }
        
        return node;
    }
}
```

### Pattern 3: Pipeline Templates

Reusable pipeline templates with different core logic:

```csharp
// Template structure
public class PipelineTemplate<TCoreLogic> : IPipelineDefinition
    where TCoreLogic : IPipelineDefinition, new()
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Data>, Data>("input");
        
        // Common preprocessing
        var preprocess = builder.AddTransform<Preprocessor, Data, Data>("preprocess");
        
        // Core logic (varies)
        var core = builder.AddComposite<Data, Data, TCoreLogic>("core");
        
        // Common postprocessing
        var postprocess = builder.AddTransform<Postprocessor, Data, Data>("postprocess");
        
        var output = builder.AddSink<PipelineOutputSink<Data>, Data>("output");
        
        builder.Connect(input, preprocess);
        builder.Connect(preprocess, core);
        builder.Connect(core, postprocess);
        builder.Connect(postprocess, output);
    }
}

// Different core implementations
public class ValidationCorePipeline : IPipelineDefinition { }
public class TransformationCorePipeline : IPipelineDefinition { }

// Usage
builder.AddComposite<Data, Data, PipelineTemplate<ValidationCorePipeline>>("validate");
builder.AddComposite<Data, Data, PipelineTemplate<TransformationCorePipeline>>("transform");
```

## Performance Considerations

### Nesting Depth Impact

Each nesting level adds overhead:

| Aspect | Impact per Level |
|--------|------------------|
| Context Creation | ~1-2μs |
| Memory | ~1-5KB (depending on context) |
| Stack Depth | Minimal (async execution) |

**Practical Limit:** 5-10 levels before performance degrades noticeably.

### Optimization Strategies

#### 1. Flatten When Possible

```csharp
❌ Avoid excessive nesting:
[Composite A] → [Composite B] → [Composite C] → [Transform]

✅ Prefer flatter structure:
[Composite ABC] → [Transform]
```

#### 2. Cache Composite Definitions

Reuse sub-pipeline definitions:

```csharp
// Define once, use many times
var validationConfig = CompositeContextConfiguration.Default;

builder.AddComposite<T, T, ValidationPipeline>("validate-1", validationConfig);
builder.AddComposite<T, T, ValidationPipeline>("validate-2", validationConfig);
```

#### 3. Minimize Context Inheritance

Only inherit at necessary levels:

```csharp
✅ Good: Selective inheritance
// Level 0 → Level 1: InheritAll
// Level 1 → Level 2: Default (no inheritance)

❌ Bad: Unnecessary inheritance
// Level 0 → Level 1: InheritAll
// Level 1 → Level 2: InheritAll
// Level 2 → Level 3: InheritAll
```

## Testing Nested Pipelines

### Test Each Level Independently

```csharp
[Fact]
public async Task Level3_CorePipeline_ShouldProcess()
{
    // Test deepest level first
    var context = new PipelineContext();
    await runner.RunAsync<CorePipeline>(context);
    // Assert
}

[Fact]
public async Task Level2_MiddlePipeline_ShouldProcess()
{
    // Test middle level
    var context = new PipelineContext();
    await runner.RunAsync<MiddlePipeline>(context);
    // Assert
}

[Fact]
public async Task Level1_MainPipeline_ShouldProcess()
{
    // Test full hierarchy
    var context = new PipelineContext();
    await runner.RunAsync<MainPipeline>(context);
    // Assert
}
```

### Test Integration Between Levels

```csharp
[Fact]
public async Task NestedPipelines_ShouldPassDataCorrectly()
{
    var context = new PipelineContext();
    context.Parameters["Input"] = "test";
    
    await runner.RunAsync<MainPipeline>(context);
    
    // Verify data flowed through all levels
    var result = context.Parameters["Output"];
    Assert.Equal("expected", result);
}
```

## Best Practices

### 1. Limit Nesting Depth

Keep nesting to 2-3 levels for most use cases:

```csharp
✅ Good: 2-3 levels
Main → Processing → Validation

❌ Excessive: 5+ levels
Main → Orchestration → Processing → Transformation → Validation → Core
```

### 2. Name Levels Clearly

Use clear, hierarchical naming:

```csharp
✅ Good names:
"order-processing"
  ↳ "validation"
    ↳ "schema-validation"

❌ Bad names:
"pipeline1"
  ↳ "pipeline2"
    ↳ "pipeline3"
```

### 3. Document Hierarchy

Document the pipeline structure:

```csharp
/// <summary>
/// Order processing pipeline with nested validation.
/// </summary>
/// <remarks>
/// Pipeline Structure:
/// - Level 0: OrderProcessingPipeline
///   - Level 1: ValidationPipeline
///     - Level 2: SchemaValidationPipeline
///   - Level 1: TransformationPipeline
/// </remarks>
public class OrderProcessingPipeline : IPipelineDefinition
{
    // ...
}
```

### 4. Use Composition for Reusability

Nest when reusing sub-pipelines:

```csharp
✅ Good: Reusable validation
public class OrderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var validate = builder.AddComposite<Order, ValidatedOrder, OrderValidationPipeline>("validate");
        // Use same validation in multiple pipelines
    }
}

public class InvoicePipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var validate = builder.AddComposite<Invoice, ValidatedInvoice, InvoiceValidationPipeline>("validate");
        // Reuse validation logic
    }
}
```

### 5. Balance Isolation and Integration

Choose appropriate inheritance at each level:

```csharp
// Level 0: Set up shared config
context.Parameters["ApiKey"] = key;

// Level 1: Inherit config, isolate processing
builder.AddComposite<T, T, ProcessingPipeline>(
    contextConfiguration: new CompositeContextConfiguration
    {
        InheritParentParameters = true  // Get config
    });

// In ProcessingPipeline, Level 2: Full isolation for core logic
builder.AddComposite<T, T, CorePipeline>(
    contextConfiguration: CompositeContextConfiguration.Default);
```

## Summary

Nested composition enables building complex pipelines from simple building blocks:

- **Unlimited depth** supported (practical limit 5-10 levels)
- **Context propagation** configurable at each level
- **Independent testing** of each level
- **Performance impact** increases with depth
- **Best practices** favor 2-3 levels for most use cases

Use nesting when it improves modularity, reusability, and maintainability - but avoid excessive depth that adds complexity without benefit.

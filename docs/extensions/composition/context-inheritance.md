# Context Inheritance

## Overview

Context inheritance controls what data from the parent pipeline's context is available to sub-pipelines. This is a critical design decision that affects isolation, testability, and behavior of your composite pipelines.

## Context Components

The PipelineContext has three main dictionaries:

### Parameters

Used for pipeline input parameters and configuration:

```csharp
context.Parameters["DatabaseConnection"] = connectionString;
context.Parameters["BatchSize"] = 100;
```

**Typical Use Cases:**
- Configuration values
- Connection strings
- Processing parameters
- Input data for composite nodes

### Items

Used for request-scoped state and services:

```csharp
context.Items["Logger"] = myLogger;
context.Items["RequestId"] = Guid.NewGuid();
```

**Typical Use Cases:**
- Request-scoped services
- Temporary state
- Request identifiers
- Cross-cutting concerns

### Properties

Used for metadata and pipeline-level configuration:

```csharp
context.Properties["Environment"] = "Production";
context.Properties["Version"] = "1.0.0";
```

**Typical Use Cases:**
- Pipeline metadata
- Environment settings
- Feature flags
- Global configuration

## Inheritance Strategies

### No Inheritance (Default)

**Configuration:**
```csharp
builder.AddComposite<TIn, TOut, SubPipeline>(
    contextConfiguration: CompositeContextConfiguration.Default);
```

**When to Use:**
- Sub-pipeline should be completely isolated
- Testing sub-pipelines independently
- Avoiding unintended dependencies
- Maximum modularity

**Characteristics:**
- Sub-pipeline has empty context dictionaries
- No parent data accessible
- Complete isolation
- Easiest to test

**Example:**
```csharp
public class StandaloneValidationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // This pipeline doesn't need any parent context
        var input = builder.AddSource<PipelineInputSource<Customer>, Customer>("input");
        var validate = builder.AddTransform<BasicValidator, Customer, ValidatedCustomer>("validate");
        var output = builder.AddSink<PipelineOutputSink<ValidatedCustomer>, ValidatedCustomer>("output");
        
        builder.Connect(input, validate);
        builder.Connect(validate, output);
    }
}
```

### Full Inheritance

**Configuration:**
```csharp
builder.AddComposite<TIn, TOut, SubPipeline>(
    contextConfiguration: CompositeContextConfiguration.InheritAll);
```

**When to Use:**
- Sub-pipeline needs access to parent configuration
- Sharing services across pipeline hierarchy
- Consistent environment settings
- Logging and tracing integration

**Characteristics:**
- All parent context data copied to sub-context
- Parent context remains isolated from changes
- Sub-pipeline can read parent values
- More complex testing requirements

**Example:**
```csharp
public class ConfigAwareEnrichmentPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Access parent configuration
        var apiKey = context.Parameters["ApiKey"]?.ToString() ?? "";
        var logger = context.Items["Logger"] as ILogger;
        
        var input = builder.AddSource<PipelineInputSource<Customer>, Customer>("input");
        var enrich = builder.AddTransform<ApiEnricher, Customer, EnrichedCustomer>("enrich");
        var output = builder.AddSink<PipelineOutputSink<EnrichedCustomer>, EnrichedCustomer>("output");
        
        builder.Connect(input, enrich);
        builder.Connect(enrich, output);
    }
}

// Usage in parent
var context = new PipelineContext();
context.Parameters["ApiKey"] = "secret-key";
context.Items["Logger"] = myLogger;

builder.AddComposite<Customer, EnrichedCustomer, ConfigAwareEnrichmentPipeline>(
    contextConfiguration: CompositeContextConfiguration.InheritAll);
```

### Selective Inheritance

**Configuration:**
```csharp
builder.AddComposite<TIn, TOut, SubPipeline>(
    contextConfiguration: new CompositeContextConfiguration
    {
        InheritParentParameters = true,
        InheritParentItems = false,
        InheritParentProperties = true
    });
```

**When to Use:**
- Need specific parent data only
- Balance between isolation and access
- Fine-grained control over dependencies
- Performance optimization

**Example:**
```csharp
// Sub-pipeline needs config but not services
builder.AddComposite<Order, ProcessedOrder, OrderProcessingPipeline>(
    contextConfiguration: new CompositeContextConfiguration
    {
        InheritParentParameters = true,  // Config values
        InheritParentItems = false,      // No services
        InheritParentProperties = true   // Metadata
    });
```

### Custom Configuration with Action

**Configuration:**
```csharp
builder.AddComposite<TIn, TOut, SubPipeline>(
    configureContext: config =>
    {
        config.InheritParentParameters = shouldInheritParams;
        config.InheritParentItems = shouldInheritItems;
        config.InheritParentProperties = shouldInheritProps;
    });
```

**When to Use:**
- Dynamic configuration based on conditions
- Configuration from external sources
- Complex inheritance logic

**Example:**
```csharp
var isDevelopment = Environment.GetEnvironmentVariable("ENVIRONMENT") == "Development";

builder.AddComposite<TIn, TOut, SubPipeline>(
    configureContext: config =>
    {
        config.InheritParentParameters = true;
        config.InheritParentItems = isDevelopment;  // Only in dev
        config.InheritParentProperties = true;
    });
```

## Isolation and Safety

### Parent Context is Always Isolated

Changes in sub-pipeline context **never** affect parent context:

```csharp
// Parent pipeline
var context = new PipelineContext();
context.Parameters["SharedValue"] = "Original";

// Run composite with inheritance
await runner.RunAsync<ParentPipeline>(context);

// Parent value unchanged, even if sub-pipeline modified it
Assert.Equal("Original", context.Parameters["SharedValue"]);
```

### Sub-Pipeline Gets Copies

When inheritance is enabled, sub-pipeline receives **copies** of the dictionaries:

```csharp
// In sub-pipeline transform
public override Task<T> ExecuteAsync(T input, PipelineContext context, ...)
{
    // This modifies the sub-pipeline's copy only
    context.Parameters["SharedValue"] = "Modified";
    
    // Parent's value remains unchanged
    return Task.FromResult(input);
}
```

## Performance Considerations

### Memory Overhead

Inheritance involves copying dictionaries:

| Configuration | Memory Impact |
|--------------|---------------|
| Default (no inheritance) | Minimal - empty dictionaries |
| InheritAll | Moderate - copies all three dictionaries |
| Selective | Low to moderate - copies selected dictionaries |

**Recommendation:** Only inherit what you need.

### Copy Timing

Dictionaries are copied once per item when the composite node processes it:

```csharp
// For each item from source:
//   1. Create sub-context (with copies if inheritance enabled)
//   2. Execute sub-pipeline
//   3. Retrieve output
//   4. Discard sub-context
```

**Recommendation:** For high-throughput scenarios, prefer no inheritance.

## Common Patterns

### Pattern 1: Configuration Inheritance

Pass configuration to sub-pipelines:

```csharp
// Parent sets up config
var context = new PipelineContext();
context.Parameters["ApiEndpoint"] = "https://api.example.com";
context.Parameters["Timeout"] = TimeSpan.FromSeconds(30);

// Sub-pipeline reads config
builder.AddComposite<TIn, TOut, ApiCallPipeline>(
    contextConfiguration: new CompositeContextConfiguration
    {
        InheritParentParameters = true
    });
```

### Pattern 2: Service Injection

Share services across pipeline hierarchy:

```csharp
// Parent sets up services
var context = new PipelineContext();
context.Items["DatabaseConnection"] = dbConnection;
context.Items["Cache"] = cache;

// Sub-pipeline uses services
builder.AddComposite<TIn, TOut, DatabasePipeline>(
    contextConfiguration: new CompositeContextConfiguration
    {
        InheritParentItems = true
    });
```

### Pattern 3: Environment Context

Share environment settings:

```csharp
// Parent sets environment
var context = new PipelineContext();
context.Properties["Environment"] = "Production";
context.Properties["Region"] = "US-West";

// Sub-pipeline adapts to environment
builder.AddComposite<TIn, TOut, AdaptivePipeline>(
    contextConfiguration: new CompositeContextConfiguration
    {
        InheritParentProperties = true
    });
```

### Pattern 4: Isolated Testing

Test sub-pipelines independently:

```csharp
[Fact]
public async Task SubPipeline_WithTestData_ShouldProcess()
{
    // Test sub-pipeline directly with test context
    var context = new PipelineContext();
    context.Parameters["TestMode"] = true;
    
    var runner = PipelineRunner.Create();
    await runner.RunAsync<MySubPipeline>(context);
    
    // Verify behavior
}
```

## Best Practices

### 1. Default to No Inheritance

Start with no inheritance and add it only when needed:

```csharp
✅ Good: Start simple
builder.AddComposite<TIn, TOut, SubPipeline>();  // Uses Default

// Add inheritance only if needed
builder.AddComposite<TIn, TOut, SubPipeline>(
    contextConfiguration: new CompositeContextConfiguration
    {
        InheritParentParameters = true  // Only what's needed
    });
```

### 2. Document Dependencies

Clearly document what context data a sub-pipeline needs:

```csharp
/// <summary>
/// Processes orders using external API.
/// </summary>
/// <remarks>
/// Required Parameters:
/// - "ApiKey" (string): API authentication key
/// - "Timeout" (TimeSpan): Request timeout
/// 
/// Required Items:
/// - "Logger" (ILogger): Logging service
/// </remarks>
public class ApiOrderProcessingPipeline : IPipelineDefinition
{
    // ...
}
```

### 3. Use Type-Safe Accessors

Create helper methods for accessing context:

```csharp
public static class ContextExtensions
{
    public static string GetApiKey(this PipelineContext context)
    {
        return context.Parameters.TryGetValue("ApiKey", out var value)
            ? value?.ToString() ?? throw new InvalidOperationException("ApiKey not found")
            : throw new InvalidOperationException("ApiKey not found");
    }
    
    public static ILogger GetLogger(this PipelineContext context)
    {
        return context.Items.TryGetValue("Logger", out var value)
            ? value as ILogger ?? throw new InvalidOperationException("Logger not found")
            : throw new InvalidOperationException("Logger not found");
    }
}

// Usage
var apiKey = context.GetApiKey();
var logger = context.GetLogger();
```

### 4. Test Both With and Without Inheritance

Test sub-pipelines in both modes:

```csharp
[Fact]
public async Task SubPipeline_Standalone_ShouldWork()
{
    // Test without parent context
    var context = new PipelineContext();
    await runner.RunAsync<SubPipeline>(context);
}

[Fact]
public async Task SubPipeline_WithParentContext_ShouldInherit()
{
    // Test with parent context
    var parentContext = new PipelineContext();
    parentContext.Parameters["Config"] = "value";
    
    await runner.RunAsync<ParentPipeline>(parentContext);
}
```

### 5. Avoid Implicit Dependencies

Make dependencies explicit through parameters or constructor injection:

```csharp
❌ Bad: Hidden dependency
public class MyTransform : TransformNode<T, T>
{
    public override Task<T> ExecuteAsync(T input, PipelineContext context, ...)
    {
        // Implicitly requires "Config" in context
        var config = context.Parameters["Config"];
        // ...
    }
}

✅ Good: Explicit dependency
public class MyTransform : TransformNode<T, T>
{
    private readonly string _config;
    
    public MyTransform(PipelineContext context)
    {
        if (!context.Parameters.TryGetValue("Config", out var value))
            throw new ArgumentException("Config parameter is required");
        _config = value.ToString() ?? throw new ArgumentException("Config cannot be null");
    }
    
    public override Task<T> ExecuteAsync(T input, PipelineContext context, ...)
    {
        // Use _config
    }
}
```

## Summary

| Strategy | Parameters | Items | Properties | Use Case |
|----------|-----------|-------|------------|----------|
| **Default** | ❌ | ❌ | ❌ | Isolated, testable sub-pipelines |
| **InheritAll** | ✅ | ✅ | ✅ | Full integration with parent |
| **Parameters Only** | ✅ | ❌ | ❌ | Configuration inheritance |
| **Items Only** | ❌ | ✅ | ❌ | Service sharing |
| **Properties Only** | ❌ | ❌ | ✅ | Metadata/environment |
| **Custom** | 🔧 | 🔧 | 🔧 | Fine-grained control |

Choose the strategy that best balances isolation, functionality, and testability for your specific use case.

---
title: Node Instantiation
description: How NPipeline instantiates nodes and optimization techniques.
sidebar_position: 5.5
---

# Node Instantiation

NPipeline uses an optimized factory pattern to instantiate nodes with minimal overhead. This page explains how node creation works and how to structure your nodes for best performance.

## Overview

Node instantiation happens when a pipeline is built. NPipeline provides multiple instantiation strategies to support different scenarios:

1. **Compiled factory (default)** - Fast path for nodes with parameterless constructors
2. **Dependency injection** - For nodes requiring constructor dependencies
3. **Pre-configured instances** - For complex or custom initialization

## DefaultNodeFactory (Optimized)

The default node factory uses **compiled expression delegates** to achieve 3-5x faster instantiation than reflection-based approaches.

### How It Works

For nodes with public parameterless constructors, NPipeline compiles an optimized factory:

```csharp
// First instantiation of a node type:
var node = factory.Create(nodeDefinition, graph);
// Internally: Compiles () => new MyNode() and caches it

// Subsequent instantiations of same type:
var node2 = factory.Create(nodeDefinition2, graph);
// Uses cached compiled delegate (no reflection!)
```

### Performance Impact

- **Optimized path**: ~50-100μs per instantiation (compiled delegate)
- **Fallback path**: ~200-300μs per instantiation (reflection)
- **Savings per pipeline**: 200-300μs on average
- **Relative improvement**: 3-5x faster for parameterless constructors

### Implementation

The factory implementation is in `NPipeline.Execution.Factories.DefaultNodeFactory`:

```csharp
private static Func<INode>? BuildCompiledFactory(Type nodeType)
{
    var constructor = nodeType.GetConstructor(Type.EmptyTypes);
    
    if (constructor == null)
        return null; // Fallback to Activator.CreateInstance
    
    // Compile: () => (INode)new TNode()
    var newExpression = Expression.New(constructor);
    var castExpression = Expression.Convert(newExpression, typeof(INode));
    var lambda = Expression.Lambda<Func<INode>>(castExpression);
    
    return lambda.Compile(); // Cached per type
}
```

## Node Constructor Patterns

### Pattern 1: Parameterless Constructor (Recommended)

✅ **Optimized** - Uses compiled factory for maximum performance

```csharp
public class SimpleTransform : TransformNode<string, int>
{
    // Implicit or explicit parameterless constructor
    // Both are optimized by the compiled factory
    
    public override Task<int> ExecuteAsync(
        string item, 
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        return Task.FromResult(item.Length);
    }
}
```

**Best for**: Stateless transforms, utility nodes, simple processing logic

### Pattern 2: Mixed Constructors (DI + Default)

✅ **Optimized** - Uses parameterless constructor path, but supports DI

```csharp
public class DependencyAwareNode : TransformNode<Order, ProcessedOrder>
{
    private IPaymentService? _paymentService;

    // Default constructor for DefaultNodeFactory (optimized)
    public DependencyAwareNode()
    {
    }

    // DI constructor for DIContainerNodeFactory
    public DependencyAwareNode(IPaymentService paymentService)
    {
        _paymentService = paymentService;
    }

    public override Task<ProcessedOrder> ExecuteAsync(
        Order item, 
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        // Handle both cases: DI-provided or null
        if (_paymentService != null)
        {
            return ProcessWithServiceAsync(item, cancellationToken);
        }
        else
        {
            return ProcessWithoutServiceAsync(item, cancellationToken);
        }
    }

    private async Task<ProcessedOrder> ProcessWithServiceAsync(
        Order item, 
        CancellationToken cancellationToken)
    {
        var result = await _paymentService.ChargeAsync(item.Amount, cancellationToken);
        return new ProcessedOrder { /* ... */ };
    }

    private Task<ProcessedOrder> ProcessWithoutServiceAsync(Order item, CancellationToken cancellationToken)
    {
        return Task.FromResult(new ProcessedOrder { /* ... */ });
    }
}
```

**Best for**: Nodes that optionally use dependencies, testing scenarios

### Pattern 3: Dependency-Only Constructor

⚠️ **Not optimized** - Falls back to reflection, but fully supports DI

```csharp
public class DIOnlyNode : TransformNode<string, int>
{
    private readonly ILogger _logger;

    // Only parameterized constructor - uses reflection fallback
    public DIOnlyNode(ILogger logger)
    {
        _logger = logger;
    }

    public override Task<int> ExecuteAsync(
        string item, 
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        _logger.LogInformation($"Processing: {item}");
        return Task.FromResult(item.Length);
    }
}
```

**Best for**: Nodes that require external services, when DI flexibility is more important than the ~200-300μs overhead

**Note**: Use `DIContainerNodeFactory` for these nodes to ensure proper instantiation.

## Factory Selection

### DefaultNodeFactory

Used by default when you don't configure DI. Optimized for parameterless constructors.

```csharp
var builder = new PipelineBuilder();
var source = builder.AddSource<SimpleSource, int>();
var transform = builder.AddTransform<OptimizedTransform, int, string>();
builder.Connect(source, transform);

var pipeline = builder.Build(); // Uses DefaultNodeFactory
```

### DIContainerNodeFactory

Use when you need dependency injection. Supports parameterized constructors.

```csharp
var services = new ServiceCollection();
services.AddSingleton<ILogger, ConsoleLogger>();
services.AddNPipeline(builder => 
{
    builder.UseDIContainerNodeFactory();
});

var pipeline = PipelineFactory.Create<MyPipeline>(context);
```

### Pre-configured Instances

For complex initialization or testing:

```csharp
var builder = new PipelineBuilder();
var customInstance = new MyNode(complexConfig);

var nodeHandle = builder.AddTransform<MyNode, string, int>();
builder.AddPreconfiguredNodeInstance(nodeHandle.Id, customInstance);

var pipeline = builder.Build();
```

## Analyzer Support

NPipeline includes analyzer **NP9505** that detects nodes without parameterless constructors:

### Warning Detection

The analyzer warns when a node has parameterized constructors but no parameterless fallback:

```csharp
public class ProblematicNode : TransformNode<string, int>
{
    private readonly ILogger _logger;

    // ⚠️ Warning NP9505: No parameterless constructor
    public ProblematicNode(ILogger logger)
    {
        _logger = logger;
    }
}
```

**Message**: "Node 'ProblematicNode' does not have a public parameterless constructor and requires DI or pre-configured instances"

### Automatic Code Fix

The analyzer provides a one-click code fix that adds a parameterless constructor:

```csharp
public class ProblematicNode : TransformNode<string, int>
{
    private ILogger? _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ProblematicNode" /> class.
    /// </summary>
    public ProblematicNode()
    {
    }

    public ProblematicNode(ILogger logger)
    {
        _logger = logger;
    }
}
```

**Note**: Implicit parameterless constructors (when no constructors are defined) don't trigger warnings - they're already optimized.

## Best Practices

1. **Add explicit parameterless constructors** for clarity and to prevent accidental removal
   ```csharp
   public MyNode() { } // Explicit, won't be accidentally removed
   ```

2. **Use both constructors** when you need flexibility
   ```csharp
   public MyNode() { }
   public MyNode(IService service) { _service = service; }
   ```

3. **Keep constructors simple** - Complex initialization belongs in a factory method or builder pattern
   ```csharp
   // Good
   public MyNode() { }
   public MyNode(ILogger logger) { _logger = logger; }

   // Consider refactoring
   public MyNode(ILogger logger, IConfig config, ICache cache, 
                 INotification notif, ISecurity sec) { /* complex */ }
   ```

4. **Don't worry about implicit constructors** - They're already optimized
   ```csharp
   // This is fine - it has an implicit parameterless constructor
   public MyNode : TransformNode<string, int>
   {
       public override Task<string> ExecuteAsync(...) { ... }
   }
   ```

5. **Use DIContainerNodeFactory for pure DI scenarios** if performance isn't critical
   ```csharp
   // When you always use DI and don't care about ~200μs overhead
   builder.UseDIContainerNodeFactory();
   ```

## Performance Comparison

| Pattern | Factory | Instantiation Time | Use Case |
|---------|---------|-------------------|----------|
| Parameterless | Compiled (optimized) | ~50-100μs | Stateless, simple nodes |
| Mixed constructors | Compiled (optimized) | ~50-100μs | Optional DI, flexible |
| Parameterized only | Reflection (fallback) | ~200-300μs | Requires dependencies |

## Related Documentation

- [Component Architecture](./component-architecture.md) - Overview of node factory role
- [Dependency Injection Integration](./dependency-injection.md) - DI setup and patterns
- [PipelineBuilder](../core-concepts/pipelinebuilder.md) - How nodes are added to pipelines
- [Performance Characteristics](./performance-characteristics.md) - Overall framework performance

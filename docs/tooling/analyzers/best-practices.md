---
title: Best Practice Analyzers
description: Validate dependency injection, resource management, and framework contract compliance.
sidebar_position: 5
---

## Best Practice Analyzers

Best practice analyzers enforce architectural patterns that ensure your code is testable, maintainable, and follows the framework's design principles.

### NP9303: Unsafe PipelineContext Access

**ID:** `NP9303`
**Severity:** Warning  
**Category:** Best Practice  

This analyzer detects unsafe access patterns to nullable properties on PipelineContext. Accessing potentially null properties without null-safety checks can cause null reference exceptions at runtime.

#### Problematic Patterns

```csharp
// ❌ PROBLEM: Direct access to potentially null property
public async Task HandleErrorAsync(PipelineContext context, Exception error)
{
    // NP9303: PipelineErrorHandler might be null
    await context.PipelineErrorHandler.HandleNodeFailureAsync(
        "nodeId", error, context, cancellationToken);
}

// ❌ PROBLEM: Direct dictionary access without existence check
public string GetParameter(PipelineContext context, string key)
{
    // NP9303: Dictionary might not contain key, and Parameters might be null
    return context.Parameters[key].ToString();
}

// ❌ PROBLEM: Unsafe cast
public void ProcessConfig(PipelineContext context)
{
    // NP9303: Configuration might be null or wrong type
    var config = (MyConfig)context.Configuration;
    ProcessConfig(config);
}
```

#### Solution: Use Safe Access Patterns

```csharp
// ✅ CORRECT: Use null-conditional operator
public async Task HandleErrorAsync(PipelineContext context, Exception error, CancellationToken cancellationToken)
{
    // Safe access - operation only occurs if PipelineErrorHandler is not null
    await context.PipelineErrorHandler?.HandleNodeFailureAsync(
        "nodeId", error, context, cancellationToken);
}

// ✅ CORRECT: Explicit null check with comment
public async Task HandleErrorAsync(PipelineContext context, Exception error, CancellationToken cancellationToken)
{
    if (context.PipelineErrorHandler == null)
    {
        throw new InvalidOperationException("PipelineErrorHandler must be configured for this operation");
    }
    
    await context.PipelineErrorHandler.HandleNodeFailureAsync(
        "nodeId", error, context, cancellationToken);
}

// ✅ CORRECT: Use TryGetValue pattern for dictionary access
public bool TryGetParameter(PipelineContext context, string key, out object value)
{
    value = null;
    return context.Parameters?.TryGetValue(key, out value) == true;
}

// ✅ CORRECT: Use pattern matching for type-safe access
public void ProcessConfig(PipelineContext context)
{
    if (context.Configuration is MyConfig config)
    {
        ProcessConfig(config);
    }
}

// ✅ CORRECT: Use pattern matching with property access
public string GetValue(PipelineContext context)
{
    if (context.PipelineErrorHandler is { } handler)
    {
        return handler.GetType().Name;
    }
    
    return "No error handler configured";
}
```

#### Safe Access Patterns Reference

| Pattern | Example | When to Use |
|---------|---------|-------------|
| Null-conditional operator | `context.Property?.Method()` | When you can safely continue if the property is null |
| Explicit null check | `if (context.Property != null)` | When you need to handle the null case explicitly |
| TryGetValue pattern | `context.Parameters?.TryGetValue("key", out var value)` | For dictionary access with null safety |
| Pattern matching | `if (context.Property is { } value)` | For type-safe access with null checking |
| Combined null and key check | `context.Parameters?.ContainsKey("key") == true` | When you need to verify both container and key existence |

#### Why Safe Access Matters

1. **Runtime Safety**: Prevents null reference exceptions from reaching production
2. **Graceful Degradation**: Code continues functioning with missing optional properties
3. **Explicit Intent**: Null checks make it clear which properties are optional
4. **Testability**: Easier to test code that handles null cases explicitly
5. **Maintainability**: Future developers understand property optionality

### NP9401: Missing Dependency Injection for Services

**ID:** `NP9401`
**Severity:** Warning  
**Category:** Best Practice  

This analyzer detects dependency injection anti-patterns in node implementations that can lead to tightly coupled code that is difficult to test and maintain. The analyzer identifies the following problematic patterns:

1. **Direct service instantiation** using the `new` keyword
2. **Static singleton field assignments** that create tightly coupled dependencies
3. **Service locator pattern usage** through GetService or GetRequiredService calls

#### The Problem

When node implementations directly instantiate their dependencies or use service locator patterns, they create tight coupling that makes the code difficult to test, maintain, and configure. This violates the Dependency Inversion Principle and makes it challenging to swap implementations or mock dependencies for testing.

```csharp
// ❌ PROBLEM: Direct service instantiation
public class BadTransformNode : TransformNode<string, string>
{
    private readonly BadService _badService = new BadService(); // NP9401: Direct instantiation

    public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult(_badService.Process(item));
    }
}

// ❌ PROBLEM: Static singleton assignment
public class BadSourceNode : SourceNode<int>
{
    private static BadService _service; // Static field

    public BadSourceNode()
    {
        _service = new BadService(); // NP9401: Static singleton assignment
    }
}

// ❌ PROBLEM: Service locator pattern
public class BadSinkNode : SinkNode<string>
{
    private readonly IServiceProvider _serviceProvider;

    public BadSinkNode(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var badService = _serviceProvider.GetService(typeof(BadService)) as BadService; // NP9401: Service locator
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            // Process item
        }
    }
}
```

#### Problematic Patterns

##### Direct Service Instantiation

```csharp
// ❌ PROBLEM: Direct instantiation with 'new'
public class TransformNodeWithDirectInstantiation : TransformNode<string, string>
{
    private readonly EmailService _emailService = new EmailService(); // NP9401
    private readonly LoggingService _loggingService = new LoggingService(); // NP9401

    public override async Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        await _emailService.SendEmailAsync(item);
        _loggingService.Log(item);
        return item.ToUpper();
    }
}
```

##### Static Singleton Field Assignments

```csharp
// ❌ PROBLEM: Static singleton pattern
public class NodeWithStaticSingleton : TransformNode<int, int>
{
    private static DataService _dataService; // Static field

    public NodeWithStaticSingleton()
    {
        _dataService = new DataService(); // NP9401: Static singleton assignment
    }

    public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult(_dataService.Process(item));
    }
}

// ❌ PROBLEM: Static property assignment
public class NodeWithStaticProperty : TransformNode<string, bool>
{
    public static CacheService Cache { get; private set; }

    static NodeWithStaticProperty()
    {
        Cache = new CacheService(); // NP9401: Static singleton assignment
    }

    public override Task<bool> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult(Cache.Contains(item));
    }
}
```

##### Service Locator Pattern Usage

```csharp
// ❌ PROBLEM: Service locator with GetService
public class NodeWithServiceLocator : TransformNode<string, string>
{
    private readonly IServiceProvider _provider;

    public NodeWithServiceLocator(IServiceProvider provider)
    {
        _provider = provider;
    }

    public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        var processor = _provider.GetService(typeof(DataProcessor)) as DataProcessor; // NP9401
        var validator = _provider.GetService<IValidator>(); // NP9401
        return Task.FromResult(processor.Process(item));
    }
}

// ❌ PROBLEM: Service locator with GetRequiredService
public class NodeWithRequiredServiceLocator : TransformNode<double, double>
{
    private readonly IServiceProvider _provider;

    public NodeWithRequiredServiceLocator(IServiceProvider provider)
    {
        _provider = provider;
    }

    public override Task<double> ExecuteAsync(double item, PipelineContext context, CancellationToken cancellationToken)
    {
        var calculator = _provider.GetRequiredService<ICalculator>(); // NP9401
        return Task.FromResult(calculator.Calculate(item));
    }
}
```

#### Solution: Use Constructor Injection

The recommended approach is to use constructor injection, which makes dependencies explicit, improves testability, and follows the Dependency Inversion Principle.

```csharp
// ✅ CORRECT: Constructor injection
public class GoodTransformNode : TransformNode<string, string>
{
    private readonly BadService _badService;

    public GoodTransformNode(BadService badService) // Constructor injection
    {
        _badService = badService;
    }

    public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult(_badService.Process(item));
    }
}

// ✅ CORRECT: Multiple dependencies via constructor
public class GoodSinkNode : SinkNode<string>
{
    private readonly IEmailService _emailService;
    private readonly ILoggingService _loggingService;
    private readonly IDataRepository _repository;

    public GoodSinkNode(
        IEmailService emailService,
        ILoggingService loggingService,
        IDataRepository repository) // Multiple dependencies
    {
        _emailService = emailService;
        _loggingService = loggingService;
        _repository = repository;
    }

    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            var processed = await _repository.SaveAsync(item);
            await _emailService.SendNotificationAsync(processed);
            _loggingService.Log(processed);
        }
    }
}
```

#### Advanced Patterns

##### Dependency Injection with Factory Pattern

```csharp
// ✅ GOOD: Using factory for complex dependencies
public class NodeWithFactory : TransformNode<string, string>
{
    private readonly IServiceFactory _serviceFactory;

    public NodeWithFactory(IServiceFactory serviceFactory)
    {
        _serviceFactory = serviceFactory;
    }

    public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        var processor = _serviceFactory.CreateProcessor(item.Type); // Factory creates appropriate service
        return Task.FromResult(processor.Process(item));
    }
}
```

##### Optional Dependencies

```csharp
// ✅ GOOD: Optional dependencies with null checks
public class NodeWithOptionalDependency : TransformNode<string, string>
{
    private readonly ICacheService _cacheService;

    public NodeWithOptionalDependency(ICacheService cacheService = null) // Optional dependency
    {
        _cacheService = cacheService;
    }

    public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        if (_cacheService != null)
        {
            var cached = _cacheService.Get(item);
            if (cached != null)
                return Task.FromResult(cached);
        }

        // Process without cache
        return Task.FromResult(item.ToUpper());
    }
}
```

#### Why This Matters

##### Testability

Constructor injection makes it easy to mock dependencies for unit testing:

```csharp
// Easy to test with mocked dependencies
[Fact]
public async Task ShouldProcessDataCorrectly()
{
    // Arrange
    var mockService = new Mock<IBadService>();
    mockService.Setup(s => s.Process("input")).Returns("OUTPUT");
    
    var node = new GoodTransformNode(mockService.Object);
    
    // Act
    var result = await node.ExecuteAsync("input", new PipelineContext(), CancellationToken.None);
    
    // Assert
    Assert.Equal("OUTPUT", result);
}
```

##### Flexibility and Configuration

With dependency injection, you can easily swap implementations:

```csharp
// Development
services.AddSingleton<IBadService, DevBadService>();

// Production
services.AddSingleton<IBadService, ProdBadService>();

// Testing
services.AddSingleton<IBadService, MockBadService>();
```

##### Dependency Inversion Principle

Constructor injection helps follow SOLID principles by depending on abstractions rather than concretions:

```csharp
// ✅ GOOD: Depends on abstraction
public class GoodNode : TransformNode<string, string>
{
    private readonly IDataProcessor _processor; // Interface, not concrete class

    public GoodNode(IDataProcessor processor)
    {
        _processor = processor;
    }

    public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult(_processor.Process(item));
    }
}

// ❌ BAD: Depends on concrete implementation
public class BadNode : TransformNode<string, string>
{
    private readonly SpecificDataProcessor _processor = new SpecificDataProcessor(); // Concrete class
    
    public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult(_processor.Process(item));
    }
}
```

#### Detection Strategy

The analyzer uses semantic analysis to detect dependency injection anti-patterns:

1. **Node Type Detection**: Identifies classes that inherit from node base types
2. **Service Type Recognition**: Distinguishes between services and DTOs
3. **Anti-Pattern Detection**: Scans for direct instantiation, static assignments, and service locator calls
4. **Context Awareness**: Only analyzes code within node implementations

#### Service vs DTO Detection

The analyzer intelligently distinguishes between services and DTOs:

**Services are identified by:**
- Namespaces containing "Service", "Repository", "Provider", "Handler", "Manager"
- Class names containing "Service", "Repository", "Provider", "Handler", "Manager"
- Having non-static methods beyond the basic object methods

**DTOs are identified by:**
- Being record types
- Names containing "Dto", "Model", "ViewModel"
- Namespaces containing "Model", "Dto", "ViewModel"
- Having only properties and static methods
- Being value types

```csharp
// This is OK - DTO instantiation is allowed
public record DataDto(string Value);

public class GoodNode : TransformNode<string, string>
{
    public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        var dto = new DataDto(item); // No warning - DTO instantiation is fine
        return Task.FromResult(dto.Value);
    }
}
```

#### Best Practices

1. **Always use constructor injection** for services in node implementations
2. **Depend on abstractions** (interfaces) rather than concrete implementations
3. **Keep constructors focused** on dependency injection, not business logic
4. **Use factories** for complex dependency creation scenarios
5. **Make dependencies explicit** - avoid optional dependencies when possible
6. **Register dependencies** in your DI container at application startup

#### Implementation Guide

To implement proper dependency injection instead of anti-patterns:

1. **Identify direct instantiations** using the NP9401 analyzer
2. **Extract dependencies** to constructor parameters
3. **Register dependencies** in your DI container
4. **Update tests** to use mocked dependencies
5. **Remove static singletons** and replace with proper DI
6. **Eliminate service locator** usage in favor of constructor injection

## Best Practices Summary

| Pattern | Recommendation | Reason |
|---------|-----------------|--------|
| Accessing nullable properties | Use null-conditional operators or explicit checks | Prevents null reference exceptions |
| Injecting services | Always use constructor injection | Improves testability and maintainability |
| Accessing context data | Use safe patterns like TryGetValue or pattern matching | Prevents runtime errors |
| Handling optional properties | Document and handle null cases explicitly | Makes code intent clear |
| Testing node implementations | Use mocked dependencies via constructor | Ensures good test isolation |

## Configuration

Adjust analyzer severity in `.editorconfig`:

```ini
# Treat unsafe context access as errors
dotnet_diagnostic.NP9303.severity = error

# Treat DI anti-patterns as errors
dotnet_diagnostic.NP9401.severity = error
```

## See Also

- [Dependency Injection Guide](../extensions/dependency-injection.md)
- [Testing Pipelines](../extensions/testing/advanced-testing.md)
- [SOLID Principles](../architecture/design-principles.md)

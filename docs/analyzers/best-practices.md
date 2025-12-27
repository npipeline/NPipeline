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
// PROBLEM: Direct access to potentially null property
public async Task HandleErrorAsync(PipelineContext context, Exception error)
{
    // NP9303: PipelineErrorHandler might be null
    await context.PipelineErrorHandler.HandleNodeFailureAsync(
        "nodeId", error, context, cancellationToken);
}

// PROBLEM: Direct dictionary access without existence check
public string GetParameter(PipelineContext context, string key)
{
    // NP9303: Dictionary might not contain key, and Parameters might be null
    return context.Parameters[key].ToString();
}

// PROBLEM: Unsafe cast
public void ProcessConfig(PipelineContext context)
{
    // NP9303: Configuration might be null or wrong type
    var config = (MyConfig)context.Configuration;
    ProcessConfig(config);
}
```

#### Solution: Use Safe Access Patterns

```csharp
// CORRECT: Use null-conditional operator
public async Task HandleErrorAsync(PipelineContext context, Exception error, CancellationToken cancellationToken)
{
    // Safe access - operation only occurs if PipelineErrorHandler is not null
    await context.PipelineErrorHandler?.HandleNodeFailureAsync(
        "nodeId", error, context, cancellationToken);
}

// CORRECT: Explicit null check with comment
public async Task HandleErrorAsync(PipelineContext context, Exception error, CancellationToken cancellationToken)
{
    if (context.PipelineErrorHandler == null)
    {
        throw new InvalidOperationException("PipelineErrorHandler must be configured for this operation");
    }
    
    await context.PipelineErrorHandler.HandleNodeFailureAsync(
        "nodeId", error, context, cancellationToken);
}

// CORRECT: Use TryGetValue pattern for dictionary access
public bool TryGetParameter(PipelineContext context, string key, out object value)
{
    value = null;
    return context.Parameters?.TryGetValue(key, out value) == true;
}

// CORRECT: Use pattern matching for type-safe access
public void ProcessConfig(PipelineContext context)
{
    if (context.Configuration is MyConfig config)
    {
        ProcessConfig(config);
    }
}

// CORRECT: Use pattern matching with property access
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
// PROBLEM: Direct service instantiation
public class BadTransformNode : TransformNode<string, string>
{
    private readonly BadService _badService = new BadService(); // NP9401: Direct instantiation

    public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult(_badService.Process(item));
    }
}

// PROBLEM: Static singleton assignment
public class BadSourceNode : SourceNode<int>
{
    private static BadService _service; // Static field

    public BadSourceNode()
    {
        _service = new BadService(); // NP9401: Static singleton assignment
    }
}

// PROBLEM: Service locator pattern
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

#### Solution: Use Constructor Injection

```csharp
// CORRECT: Constructor injection
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

// CORRECT: Multiple dependencies via constructor
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

#### Best Practices

1. **Always use constructor injection** for services in node implementations
2. **Depend on abstractions** (interfaces) rather than concrete implementations
3. **Keep constructors focused** on dependency injection, not business logic
4. **Use factories** for complex dependency creation scenarios
5. **Make dependencies explicit** - avoid optional dependencies when possible
6. **Register dependencies** in your DI container at application startup

## Configuration

Adjust analyzer severity in `.editorconfig`:

```ini
# Treat unsafe context access as errors
dotnet_diagnostic.NP9303.severity = error

# Treat DI anti-patterns as errors
dotnet_diagnostic.NP9401.severity = error
```

## See Also

- [Dependency Injection Guide](../../extensions/dependency-injection)
- [Testing Pipelines](../../extensions/testing/advanced-testing)
- [SOLID Principles](../../architecture/design-principles)

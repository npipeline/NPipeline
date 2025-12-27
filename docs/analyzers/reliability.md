---
title: Reliability Analyzers
description: Identify inefficient exception handling patterns and unsafe access patterns that can harm reliability.
sidebar_position: 3
---

## Reliability Analyzers

Reliability analyzers detect inefficient exception handling patterns and unsafe access patterns that can harm performance and reliability in NPipeline pipelines.

### NP9301: Inefficient Exception Handling

**ID:** `NP9301`
**Severity:** Warning  
**Category:** Reliability  

This analyzer detects inefficient exception handling patterns that can harm performance and reliability in NPipeline pipelines. The analyzer identifies overly broad exception catches, missing specific exception handling, and performance-impacting exception usage in hot paths.

#### Why This Matters

Inefficient exception handling causes:

1. **Performance Degradation**: Exception throwing and catching is expensive
2. **Masked Errors**: Overly broad catches hide real issues
3. **Poor Debugging**: Generic exception handling obscures root causes
4. **Resource Leaks**: Improper exception handling can prevent cleanup

#### Problematic Patterns

```csharp
// PROBLEM: Catching Exception instead of specific exceptions
public async Task ProcessAsync(Input input, CancellationToken cancellationToken)
{
    try
    {
        await ProcessItemAsync(input, cancellationToken);
    }
    catch (Exception ex) // NP9301: Too broad exception handling
    {
        _logger.LogError(ex, "Processing failed");
        throw;
    }
}

// PROBLEM: Using exceptions for control flow
public Item GetItem(string id)
{
    if (!_cache.TryGetValue(id, out var item))
    {
        throw new KeyNotFoundException($"Item {id} not found"); // NP9301: Exception for control flow
    }
    return item;
}

// PROBLEM: Exception handling in hot paths
public async Task ProcessBatchAsync(IEnumerable<Item> items, CancellationToken cancellationToken)
{
    foreach (var item in items)
    {
        try
        {
            await ProcessItemAsync(item, cancellationToken);
        }
        catch (Exception ex) // NP9301: Exception handling in performance-critical loop
        {
            _logger.LogError(ex, "Failed to process item {ItemId}", item.Id);
        }
    }
}
```

#### Solution: Use Specific Exception Handling and Alternative Patterns

```csharp
// CORRECT: Catch specific exceptions
public async Task ProcessAsync(Input input, CancellationToken cancellationToken)
{
    try
    {
        await ProcessItemAsync(input, cancellationToken);
    }
    catch (InvalidOperationException ex)
    {
        _logger.LogError(ex, "Invalid operation during processing");
        throw;
    }
    catch (TimeoutException ex)
    {
        _logger.LogError(ex, "Processing timeout");
        throw new ProcessingTimeoutException("Item processing timed out", ex);
    }
}

// CORRECT: Use result pattern instead of exceptions for control flow
public Result<Item> TryGetItem(string id)
{
    if (!_cache.TryGetValue(id, out var item))
    {
        return Result<Item>.Failure($"Item {id} not found");
    }
    return Result<Item>.Success(item);
}

// CORRECT: Filter invalid items before processing
public async Task ProcessBatchAsync(IEnumerable<Item> items, CancellationToken cancellationToken)
{
    var validItems = new List<Item>();
    var failedItems = new List<(Item Item, Exception Error)>();
    
    // Validate first to avoid exceptions in hot path
    foreach (var item in items)
    {
        if (IsValid(item))
        {
            validItems.Add(item);
        }
        else
        {
            failedItems.Add((item, new ValidationException("Invalid item")));
        }
    }
    
    // Process valid items
    foreach (var item in validItems)
    {
        try
        {
            await ProcessItemAsync(item, cancellationToken);
        }
        catch (Exception ex)
        {
            failedItems.Add((item, ex));
        }
    }
    
    if (failedItems.Any())
    {
        _logger.LogWarning("Failed to process {Count} items", failedItems.Count);
    }
}
```

#### Exception Handling Guidelines

| Scenario | Recommended Approach | Benefit |
|----------|----------------------|----------|
| Expected failures | Result pattern or TryXXX methods | No exception overhead |
| Validation failures | Input validation before processing | Prevent exceptions |
| External dependencies | Specific exception types | Precise error handling |
| Hot paths | Avoid exceptions for control flow | Better performance |
| Cleanup operations | finally blocks or using statements | Guaranteed cleanup |

## Best Practices for Reliability

1. **Catch specific exceptions** - Avoid catching Exception when possible
2. **Use result patterns** - For expected failures that don't need exceptions
3. **Validate inputs early** - Prevent exceptions before they occur
4. **Avoid exceptions in hot paths** - Use alternative control flow mechanisms
5. **Always clean up resources** - Use finally blocks or using statements

## Configuration

Adjust analyzer severity in `.editorconfig`:

```ini
# Treat inefficient exception handling as warnings
dotnet_diagnostic.NP9301.severity = warning
```

## See Also

- [Error Handling Architecture](../../architecture/error-handling-architecture)
- [Performance Characteristics](../../architecture/performance-characteristics)

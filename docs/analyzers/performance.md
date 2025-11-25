---
title: Performance Analyzers
description: Identify blocking operations, non-streaming patterns, and async/await anti-patterns that harm performance.
sidebar_position: 3
---

## Performance Analyzers

Performance analyzers detect patterns that harm throughput, increase latency, cause thread starvation, or prevent proper streaming of data. These violations directly contradict NPipeline's core mission of high-performance, non-blocking I/O.

### NP9102: Blocking Operations in Async Methods

**ID:** `NP9102`  
**Severity:** Warning  
**Category:** Performance  

This analyzer detects blocking operations in async methods that can lead to deadlocks, thread pool starvation, and reduced performance. The analyzer identifies the following blocking patterns:

1. **Task.Result and Task.Wait()** calls that block the current thread
2. **GetAwaiter().GetResult()** patterns that synchronously wait for task completion
3. **Thread.Sleep()** in async methods (should use Task.Delay instead)
4. **Synchronous file I/O operations** (File.ReadAllText, File.WriteAllBytes, etc.)
5. **Synchronous network I/O operations** (WebClient.DownloadString, unawaited HttpClient calls)
6. **Unawaited StreamReader/Writer operations** (ReadToEnd, WriteLine without await)

#### Why This Matters

Blocking operations in async code:

- **Cause deadlocks** in certain synchronization contexts (UI threads, ASP.NET Classic)
- **Starve the thread pool** by blocking threads that should be available for other work
- **Reduce scalability** because you can only handle as many concurrent operations as you have threads
- **Increase latency** because blocked threads can't process other work
- **Contradict async design** and defeat the purpose of asynchronous I/O

#### Problematic Patterns

```csharp
// ❌ PROBLEM: Blocking on Task.Result
public async Task<string> ProcessDataAsync()
{
    var task = SomeOperationAsync();
    return task.Result; // NP9102: Blocks until task completes
}

// ❌ PROBLEM: Blocking on Task.Wait()
public async Task ProcessDataAsync()
{
    var task = SomeOperationAsync();
    task.Wait(); // NP9102: Blocks until task completes
}

// ❌ PROBLEM: Using GetAwaiter().GetResult()
public async Task<string> ProcessDataAsync()
{
    var task = SomeOperationAsync();
    return task.GetAwaiter().GetResult(); // NP9102: Synchronous blocking
}

// ❌ PROBLEM: Synchronous I/O in async method
public async Task ProcessFileAsync()
{
    var content = File.ReadAllText("file.txt"); // NP9102: Synchronous I/O
    await ProcessAsync(content);
}

// ❌ PROBLEM: Thread.Sleep instead of Task.Delay
public async Task WaitAsync()
{
    Thread.Sleep(1000); // NP9102: Blocks the thread
    await ContinueAsync();
}
```

#### Solution: Use await

```csharp
// ✅ CORRECT: Use await
public async Task<string> ProcessDataAsync()
{
    var task = SomeOperationAsync();
    return await task; // Properly awaits without blocking
}

// ✅ CORRECT: Use async I/O
public async Task ProcessFileAsync()
{
    var content = await File.ReadAllTextAsync("file.txt"); // Async I/O
    await ProcessAsync(content);
}

// ✅ CORRECT: Use Task.Delay
public async Task WaitAsync()
{
    await Task.Delay(1000); // Non-blocking delay
    await ContinueAsync();
}
```

### NP9103: Swallowed OperationCanceledException

**ID:** `NP9103`  
**Severity:** Warning  
**Category:** Performance  

This analyzer detects when `OperationCanceledException` is caught but not re-thrown. Swallowing cancellation exceptions breaks the cancellation contract and causes the pipeline to continue processing when it should stop.

#### Problematic Pattern

```csharp
// ❌ PROBLEM: Swallowing OperationCanceledException
public async Task ProcessAsync(CancellationToken cancellationToken)
{
    try
    {
        await SomeOperationAsync(cancellationToken);
    }
    catch (OperationCanceledException)
    {
        // NP9103: Silently swallowing cancellation
        Console.WriteLine("Operation cancelled");
    }
}
```

#### Solution: Re-throw Cancellation

```csharp
// ✅ CORRECT: Re-throw cancellation exception
public async Task ProcessAsync(CancellationToken cancellationToken)
{
    try
    {
        await SomeOperationAsync(cancellationToken);
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw; // Re-throw to propagate cancellation
    }
    catch (OperationCanceledException)
    {
        // Handle other cancellation scenarios
        throw;
    }
}

// ✅ ALTERNATIVE: Handle other exceptions only
public async Task ProcessAsync(CancellationToken cancellationToken)
{
    try
    {
        await SomeOperationAsync(cancellationToken);
    }
    catch (Exception ex) when (!(ex is OperationCanceledException))
    {
        // Handle non-cancellation exceptions
    }
}
```

### NP9104: Synchronous over Async Patterns

**ID:** `NP9104`  
**Severity:** Warning  
**Category:** Performance  

This analyzer detects "sync-over-async" patterns like unawaited async method calls or fire-and-forget operations. These patterns create unpredictable behavior and make it impossible to wait for completion or handle errors.

#### Problematic Sync-Over-Async Patterns

```csharp
// ❌ PROBLEM: Fire-and-forget async call (unawaited)
public async Task ProcessDataAsync()
{
    SomeOperationAsync(); // NP9104: Async method not awaited
    DoSomethingElse();
}

// ❌ PROBLEM: Async method called from sync method
public void ProcessData()
{
    var result = SomeOperationAsync(); // NP9104: Async method not awaited
}

// ❌ PROBLEM: Task.Run wrapping sync work
public async Task ProcessDataAsync()
{
    var result = await Task.Run(() => 
    {
        return SomeSynchronousOperation(); // NP9104: Unnecessary Task.Run
    });
}
```

#### Solution: Always Await

```csharp
// ✅ CORRECT: Await the async call
public async Task ProcessDataAsync()
{
    await SomeOperationAsync(); // Properly awaited
    DoSomethingElse();
}

// ✅ CORRECT: Make calling method async
public async Task ProcessDataAsync()
{
    var result = await SomeOperationAsync(); // Properly awaited
}

// ✅ CORRECT: Call sync methods directly
public async Task ProcessDataAsync()
{
    var result = SomeSynchronousOperation(); // Direct call, no Task.Run
    await ProcessResultAsync(result);
}
```

### NP9105: Cancellation Token Not Respected

**ID:** `NP9105`  
**Severity:** Warning  
**Category:** Performance  

This analyzer detects when a cancellation token is not checked or respected in long-running operations. When you receive a cancellation token, you must check it periodically and propagate cancellation requests.

#### Problematic Cancellation Patterns

```csharp
// ❌ PROBLEM: Not checking cancellation token in loop
public async Task ProcessItemsAsync(IEnumerable<Item> items, CancellationToken cancellationToken)
{
    foreach (var item in items)
    {
        // NP9105: Not checking cancellation token
        await ProcessItemAsync(item);
    }
}
```

#### Solution: Check and Respect Cancellation

```csharp
// ✅ CORRECT: Check cancellation token before processing
public async Task ProcessItemsAsync(IEnumerable<Item> items, CancellationToken cancellationToken)
{
    foreach (var item in items)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await ProcessItemAsync(item, cancellationToken);
    }
}

// ✅ CORRECT: Pass token to async operations
public async Task ProcessItemsAsync(IEnumerable<Item> items, CancellationToken cancellationToken)
{
    await foreach (var item in GetItemsAsync(cancellationToken))
    {
        await ProcessItemAsync(item, cancellationToken);
    }
}

private async IAsyncEnumerable<Item> GetItemsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
{
    foreach (var item in _items)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return item;
    }
}
```

### NP9209: Missing ValueTask Optimization

**ID:** `NP9209`  
**Severity:** Warning  
**Category:** Performance  

This analyzer detects cases where a method frequently completes synchronously but returns `Task` instead of `ValueTask`. Using `ValueTask` avoids heap allocations when the result is available synchronously, which is critical for high-throughput pipeline performance.

#### Problem

```csharp
// ❌ PROBLEM: Allocates heap object even for synchronous completions
public async Task<string> GetDataAsync(string id)
{
    var cached = _cache.Get(id);
    if (cached != null)
    {
        return cached; // Allocates Task on heap
    }
    
    return await FetchFromDatabaseAsync(id);
}
```

#### Solution: Use ValueTask

```csharp
// ✅ CORRECT: No allocation for synchronous returns
public async ValueTask<string> GetDataAsync(string id)
{
    var cached = _cache.Get(id);
    if (cached != null)
    {
        return cached; // No allocation - synchronous completion
    }
    
    return await FetchFromDatabaseAsync(id);
}
```

**Important:** ValueTask comes with critical constraints that you must understand to avoid subtle bugs. For complete implementation guidance, including dangerous constraints and real-world examples, see [**Synchronous Fast Paths and ValueTask Optimization**](../advanced-topics/synchronous-fast-paths.md)—the dedicated deep-dive guide that covers the complete pattern and critical safety considerations.

### NP9211: Non-Streaming Patterns in SourceNode

**ID:** `NP9211`  
**Severity:** Warning  
**Category:** Performance  

This analyzer detects non-streaming patterns in SourceNode implementations that can lead to memory issues and poor performance. See the [Data Processing Analyzers](./data-processing.md) section for detailed information about this analyzer.

## Best Practices for Performance

1. **Always use await** - Never block on async code with .Result, .Wait(), or .GetResult()
2. **Respect cancellation tokens** - Check them frequently and pass them to all async operations
3. **Never swallow OperationCanceledException** - Always re-throw or handle it appropriately
4. **Use ValueTask for sync-heavy paths** - Avoid unnecessary allocations
5. **Use async all the way down** - Don't mix sync and async code
6. **Use ConfigureAwait(false) in library code** - Improves performance and prevents deadlocks

## Configuration

Adjust analyzer severity in `.editorconfig`:

```ini
# Treat blocking operations as errors
dotnet_diagnostic.NP9102.severity = error

# Treat swallowed cancellation as errors
dotnet_diagnostic.NP9103.severity = error

# Treat fire-and-forget async as errors
dotnet_diagnostic.NP9104.severity = error

# Treat ignored cancellation tokens as errors
dotnet_diagnostic.NP9105.severity = error

# Treat missing ValueTask optimization as warnings
dotnet_diagnostic.NP9209.severity = warning

# Treat non-streaming patterns as errors
dotnet_diagnostic.NP9211.severity = error
```

## See Also

- [Performance Hygiene](../advanced-topics/performance-hygiene.md)
- [Synchronous Fast Paths](../advanced-topics/synchronous-fast-paths.md)

# NPipeline Error Codes Reference

This guide explains NPipeline error codes, their causes, and solutions.

Error codes are organized by category:

- **NP01xx**: Graph Validation Errors
- **NP02xx**: Type Mismatch and Conversion Errors
- **NP03xx**: Execution Errors
- **NP04xx**: Configuration Errors
- **NP05xx**: Resource and Capacity Errors
- **NP90xx-NP94xx**: Analyzer Diagnostics

---

## NP01xx - Graph Validation Errors

### NP0101: Pipeline Requires At Least One Node

**Message:** `[NP0101] A pipeline must have at least one node. Add at least one node (source, transform, or sink) before building.`

**Cause:** You attempted to build a pipeline with no nodes.

**Solution:** Add at least one node using methods like `AddSource()`, `AddTransform()`, `AddSink()`, `AddJoin()`, or `AddAggregate()`.

**Example:**

```csharp
var builder = new PipelineBuilder();
// builder.Build(); // ❌ Would throw NP0101

builder.AddSource<MySourceNode>();
var pipeline = builder.Build(); // ✅ Works
```

---

### NP0102: Node Missing Input Connection

**Message:** `[NP0102] Node '{nodeId}' ({nodeName}, {nodeKind}) is missing a required input connection...`**

**Cause:** A transform or sink node has no incoming edges from other nodes. These node types require at least one input connection.

**Solution:** Connect an output node to this node using `Connect()`.

**Example:**

```csharp
builder.AddSource<SourceNode>("src");
builder.AddTransform<MyTransform>("transform");
// Missing: builder.Connect("src", "transform");

builder.Build(); // ❌ Would throw NP0102
```

---

### NP0103: Cyclic Dependency Detected

**Message:** `[NP0103] Cyclic dependency detected in pipeline graph. Pipelines must be directed acyclic graphs (DAGs)...`**

**Cause:** Your pipeline connections form a cycle (circular reference). This is not allowed as pipelines must be directed acyclic graphs.

**Solution:** Review your connections and ensure no circular dependencies exist.

**Example:**

```csharp
builder.AddTransform<Transform1>("t1");
builder.AddTransform<Transform2>("t2");
builder.Connect("t1", "t2");
builder.Connect("t2", "t1"); // ❌ Creates cycle

builder.Build(); // Would throw NP0103
```

---

### NP0104: Node Already Added

**Message:** `[NP0104] A node with ID '{nodeId}' has already been added...`**

**Cause:** You attempted to add a node with an ID that's already registered in the pipeline.

**Solution:** Use a different ID or don't add the same node twice.

**Example:**

```csharp
builder.AddTransform<MyTransform>("transform");
builder.AddTransform<MyTransform>("transform"); // ❌ Duplicate ID

builder.Build(); // Would throw NP0104
```

---

### NP0105: Node Name Not Unique

**Message:** `[NP0105] A node with name '{name}' has already been added...`**

**Cause:** Node names must be unique within a pipeline. You used a duplicate name.

**Solution:** Either provide a different name or let the framework auto-generate one by not specifying a name.

**Example:**

```csharp
builder.AddTransform<Transform1>("step");
builder.AddTransform<Transform2>("step"); // ❌ Duplicate name

builder.Build(); // Would throw NP0105
```

---

## NP02xx - Type Mismatch and Conversion Errors

### NP0201: Type Mismatch in Connection

**Message:** `[NP0201] Type mismatch in connection between nodes...`**

**Cause:** You connected two nodes with incompatible types. The output type of one doesn't match the input type of the other.

**Solution:** Add an appropriate transformation node between them to handle the type conversion, or adjust your node types to be compatible.

**Example:**

```csharp
builder.AddSource<StringSource>("src");     // Outputs: string
builder.AddTransform<IntTransform>("trans"); // Inputs: int
builder.Connect("src", "trans"); // ❌ Type mismatch

// Solution: Add type converter
builder.AddTransform<StringToIntConverter>("converter");
builder.Connect("src", "converter");
builder.Connect("converter", "trans"); // ✅ Works
```

---

### NP0202: Input Data Pipe Wrong Type

**Message:** `[NP0202] Input data pipe is not of the expected type...`**

**Cause:** An internal framework error where the input pipe type doesn't match expected type. This usually indicates a graph construction error.

**Solution:** Review your pipeline definition and ensure all node types and connections are correct.

---

### NP0203: Cannot Register Mappings After Execution

**Message:** `[NP0203] Cannot register type mappings after execution has begun...`**

**Cause:** Attempted to configure type mappings after the pipeline has started executing.

**Solution:** Configure all mappings before the pipeline starts executing or when building.

**Example:**

```csharp
// Configure all type mappings BEFORE pipeline execution
var builder = new PipelineBuilder();
// Add your nodes and configure mappings here
var pipeline = builder.Build();

// Then execute the pipeline
await pipeline.ExecuteAsync(source, context);
```

---

### NP0204: Record Type Has No Public Constructor

**Message:** `[NP0204] Record type '{recordTypeName}' has no public constructors...`**

**Cause:** You're trying to construct a record type that doesn't have any public constructors.

**Solution:** Either add a public constructor to the record or use a different conversion method.

**Example:**

```csharp
// ❌ Bad: private constructor
public record Order(int Id)
{
    private Order() { }
}

// ✅ Good: public constructor
public record Order(int Id);
```

---

### NP0205-2008: Member Access and Setter Errors

**Messages:**

- `[NP0205]` Invalid member access expression
- `[NP0206]` Member not writable
- `[NP0207]` Setter creation failed
- `[NP0208]` ValueTuple constructor not found

**Cause:** Issues with property/field mapping in type conversions. The member either doesn't exist, isn't writable, or has other issues.

**Solution:** Review your selector expressions and ensure they target valid, writable properties.

---

### NP0210: Cannot Concatenate Streams Type Mismatch

**Message:** `[NP0210] Cannot concatenate streams due to type mismatch...`**

**Cause:** You're trying to merge multiple input streams with incompatible types.

**Solution:** Ensure all inputs to a merge point have the same type.

---

## NP03xx - Execution Errors

### NP0301: Node Kind Not Supported

**Message:** `[NP0301] Node kind '{nodeKind}' is not supported...`**

**Cause:** An internal framework error where a node type is not properly registered.

**Solution:** This is usually an NPipeline bug. Please report it on GitHub.

---

### NP0302: Output Not Found for Source Node

**Message:** `[NP0302] Could not find output for source node '{sourceNodeId}'...`**

**Cause:** A downstream node is trying to consume output from a source node that hasn't executed or didn't produce output.

**Solution:** Ensure source nodes execute successfully before consuming their output.

---

### NP0303-3004: Pipeline Execution Failed

**Messages:**

- `[NP0303]` Pipeline execution failed at node
- `[NP0304]` Pipeline execution failed

**Cause:** An exception occurred during pipeline execution.

**Solution:** Check the inner exception details and review the node's implementation. The error message will include details about what went wrong.

---

### NP0305: Item Failed After Max Retries

**Message:** `[NP0305] An item failed to process after {attempts} attempts...`**

**Cause:** An item failed to process and exceeded the configured retry limit.

**Solution:**

1. Increase retry limit if transient errors are expected
2. Fix the underlying issue causing the error
3. Implement better error handling

**Example:**

```csharp
builder.WithRetryOptions(options =>
    options with { MaxItemRetries = 5 } // Increase from default (typically 0)
);
```

---

### NP0306: Error Handling Failed

**Message:** `[NP0306] Error handling failed for node '{nodeId}'...`**

**Cause:** Your custom error handler threw an exception.

**Solution:** Review your error handler implementation and ensure it doesn't throw exceptions.

---

### NP0307: Lineage Cardinality Mismatch

**Message:** `[NP0307] Lineage cardinality mismatch in node...`**

**Cause:** The number of lineage mappings doesn't match the node's declared input/output cardinality.

**Solution:** Ensure your lineage configuration matches your node's actual cardinality (OneToOne, OneToMany, etc.).

---

### NP0310: Circuit Breaker Tripped

**Message:** `[NP0310] Circuit breaker tripped for node '{nodeId}'...`**

**Cause:** A node exceeded the failure threshold and the circuit breaker activated.

**Solution:**

1. Fix the underlying issue causing failures
2. Increase the failure threshold if needed
3. Wait for the circuit breaker to reset (default: 1 minute)

---

### NP0311: Retry Limit Exhausted

**Message:** `[NP0311] Retry limit exhausted for node '{nodeId}'...`**

**Cause:** A node failed repeatedly and exceeded both item-level and node-restart retry limits.

**Solution:**

1. Review error logs to understand why retries are failing
2. Increase retry limits if failures are transient
3. Fix the underlying issue

---

### NP9302: SinkNode Input Not Consumed

**Message:** `[NP9302] SinkNode '{0}' overrides ExecuteAsync but doesn't consume input parameter. Sink nodes should process all items from input data pipe.`**

**Category:** Node Execution Error

**Severity:** Error

**Cause:** Your SinkNode implementation overrides ExecuteAsync but doesn't consume the input parameter. Sink nodes are designed to process all items from their input data pipe, but your implementation ignores the input.

This can cause:
- Data loss when items in the input pipe are not processed
- Incomplete pipeline execution
- Unexpected behavior when downstream nodes depend on sink's side effects

**Example of Problem Code:**

```csharp
// ❌ PROBLEM: SinkNode ignores input
public class MySinkNode : SinkNode<string>
{
    public override Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Input parameter is never used
        Console.WriteLine("Sink executed but ignoring input");
        return Task.CompletedTask;
    }
}
```

**Solution:**

Always consume input in SinkNode implementations:

```csharp
// ✅ SOLUTION: Process all items from input
public class MySinkNode : SinkNode<string>
{
    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Process all items from input
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Processing: {item}");
            // Save to database, write to file, etc.
        }
    }
}
```

**Common Input Consumption Patterns:**

| Pattern | Example | Use Case |
|---------|---------|----------|
| Process all items | `await foreach (var item in input.WithCancellation(cancellationToken)) { ... }` | Standard processing of each item |
| Count items | `var count = await input.CountAsync(cancellationToken);` | When you only need the count |
| Collect to list | `var items = await input.ToListAsync(cancellationToken);` | When you need all items in memory |
| First item only | `var first = await input.FirstAsync(cancellationToken);` | When you only need the first item |
| Any items check | `var hasItems = await input.AnyAsync(cancellationToken);` | When you just need to check if input is non-empty |

**Best Practices for SinkNode Implementation:**

1. **Always consume input** - Use `await foreach` or other data pipe operations
2. **Pass cancellation token** - Use `WithCancellation(cancellationToken)` for proper cancellation support
3. **Handle empty input** - Your code should work correctly even if the input pipe is empty
4. **Consider performance** - For large datasets, process items in a streaming fashion rather than collecting all items
5. **Don't silently ignore input** - Even if you don't need to process items, consume them to acknowledge receipt

---

## NP9303: Unsafe PipelineContext Access

**Message:** `[NP9303] Unsafe access pattern '{0}' detected on PipelineContext. This can lead to NullReferenceException at runtime. Use null-conditional operators (?.) or check for null before accessing these properties.`**

**Category:** Node Execution Error

**Severity:** Warning

**Cause:** Your code is accessing PipelineContext properties or dictionaries in a way that can lead to NullReferenceException at runtime. The analyzer detects these unsafe patterns:

1. **Direct access to nullable properties** without null checks
2. **Dictionary access without type checking** (Items, Parameters, Properties dictionaries)
3. **Unsafe casting from dictionary values**
4. **Method calls on nullable properties** without null verification

These unsafe patterns can cause:
- Runtime NullReferenceException when properties are null
- Pipeline failures that are difficult to debug
- Inconsistent behavior in production environments

**Example of Problem Code:**

```csharp
// ❌ PROBLEM: Unsafe access patterns
public async Task ProcessContext(PipelineContext context)
{
    // NP9303: Direct access to nullable property
    var handler = context.PipelineErrorHandler;
    
    // NP9303: Dictionary access without null check
    var configValue = context.Parameters["retryCount"];
    
    // NP9303: Method call on nullable property
    var stateManager = context.StateManager;
    stateManager.SaveState("key", data);
}
```

**Solution - Use Safe Access Patterns:**

```csharp
// ✅ SOLUTION: Safe access patterns
public async Task ProcessContext(PipelineContext context)
{
    // Use null-conditional operator
    await context.PipelineErrorHandler?.HandleNodeFailureAsync("nodeId", new Exception(), context, CancellationToken.None);
    
    // Use TryGetValue pattern for dictionary access
    if (context.Parameters?.TryGetValue("retryCount", out var configValue) == true)
    {
        var retryCount = Convert.ToInt32(configValue);
    }
    
    // Check for null before using
    if (context.StateManager != null)
    {
        context.StateManager.SaveState("key", data);
    }
}
```

**Common Safe Access Patterns:**

| Pattern | Example | When to Use |
|---------|---------|-------------|
| Null-conditional operator | `context.PipelineErrorHandler?.HandleError()` | When you can safely continue if property is null |
| Explicit null check | `if (context.PipelineErrorHandler != null)` | When you need to handle null case explicitly |
| TryGetValue pattern | `context.Parameters?.TryGetValue("key", out var value)` | For dictionary access with null safety |
| Pattern matching | `if (context.PipelineErrorHandler is { } handler)` | For type-safe access with null checking |

---

## NP04xx - Configuration Errors

### NP9001: Incomplete Resilient Configuration

**Message:** `[NP9001] Error handler can return PipelineErrorDecision.RestartNode but may not have all three mandatory prerequisites configured. Missing prerequisites will silently disable restart, causing the entire pipeline to fail instead of recovering the failed node.`

**Category:** Configuration Error

**Severity:** Warning

**Cause:** Your error handler can return `PipelineErrorDecision.RestartNode`, but the configuration is incomplete. Restart functionality requires **all three** of the following mandatory prerequisites:

1. **ResilientExecutionStrategy** must wrap the node
2. **MaxNodeRestartAttempts** must be > 0 in PipelineRetryOptions
3. **MaxMaterializedItems** must be non-null (not unbounded)

Missing even one of these prerequisites will **silently disable restart**, causing the entire pipeline to fail instead of recovering gracefully.

**Example of Problem Code:**

```csharp
// ❌ PROBLEM: RestartNode is possible, but prerequisites are missing

var myErrorHandler = new MyErrorHandler();

var pipeline = builder
    .AddTransform<MyTransform>("myNode")
    // Missing: .WithExecutionStrategy(builder, new ResilientExecutionStrategy(...))
    .Build();

// If MyErrorHandler.HandleNodeFailureAsync() can return RestartNode,
// but the node doesn't have ResilientExecutionStrategy, restart will fail silently!
```

**Solution - Complete Configuration Checklist:**

**For detailed step-by-step configuration instructions, see the [Getting Started with Resilience](../core-concepts/resilience/getting-started.md) guide.**

```csharp
// ✅ SOLUTION: All three prerequisites configured

// STEP 1: Apply ResilientExecutionStrategy
var nodeHandle = builder
    .AddTransform<MyTransform, Input, Output>("myNode")
    .WithExecutionStrategy(
        builder,
        new ResilientExecutionStrategy(new SequentialExecutionStrategy())
    );

// STEP 2: Configure retry options with MaxNodeRestartAttempts > 0
var retryOptions = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,           // ← STEP 2: Set to > 0
    MaxSequentialNodeAttempts: 5,
    MaxMaterializedItems: 1000            // ← STEP 3: Set to non-null value
);

// STEP 3: Create context with retry options
var context = PipelineContext.WithRetry(retryOptions);
var pipeline = builder.Build();

await pipeline.ExecuteAsync(source, context);
```

**Critical Warning:** Never set `MaxMaterializedItems` to `null` (unbounded). This silently disables restart functionality and can cause OutOfMemoryException. See the [Getting Started with Resilience](../core-concepts/resilience/getting-started.md) guide for detailed explanation of why unbounded buffers break resilience guarantees.

**Read More:**

- **[Getting Started with Resilience](../core-concepts/resilience/getting-started.md)** - Complete quick-start and step-by-step configuration guide
- [Build-Time Resilience Analyzer Guide](../tooling/analyzers/resilience.md)
- [Error Handling Guide](../core-concepts/resilience/error-handling.md)
- [Materialization & Buffering](../core-concepts/resilience/materialization.md)

---

## NP90xx-NP94xx - Analyzer Diagnostics

### NP9205: Non-Streaming Patterns in SourceNode

**Message:** `[NP9205] Non-streaming patterns detected in SourceNode implementation. Consider using IAsyncEnumerable with yield return for better performance and memory efficiency.`

**Category:** Performance

**Severity:** Warning

**Cause:** Your SourceNode implementation contains patterns that can lead to memory issues and poor performance when processing large datasets. The analyzer detects these problematic patterns:

1. **`List<T>` or `Array` allocation and population** in ExecuteAsync methods
2. **.ToAsyncEnumerable()** calls on materialized collections
3. **Synchronous I/O operations** like File.ReadAllText, File.WriteAllBytes, etc.
4. **.ToList() and .ToArray()** calls that materialize collections in memory

These patterns can cause:
- High memory usage when processing large datasets
- Poor startup performance as all data must be loaded before processing begins
- Increased GC pressure from large collections
- Thread blocking from synchronous I/O operations
- Reduced scalability as memory requirements grow with data size

**Example of Problem Code:**

```csharp
// ❌ PROBLEM: Materializing all data in memory
public class BadSourceNode : SourceNode<string>
{
    protected override async Task ExecuteAsync(IDataPipe<string> output, PipelineContext context, CancellationToken cancellationToken)
    {
        // NP9205: Allocating List<T> and populating it
        var items = new List<string>();
        
        // Read all lines from file into memory
        var lines = File.ReadAllLines("large-file.txt"); // NP9205: Synchronous I/O
        
        foreach (var line in lines)
        {
            items.Add(line);
        }
        
        // NP9205: Materializing collection with ToList()
        foreach (var item in items.ToList())
        {
            await output.ProduceAsync(item, cancellationToken);
        }
    }
}
```

**Solution - Use Streaming Patterns:**

```csharp
// ✅ CORRECT: Using IAsyncEnumerable with yield return
public class GoodSourceNode : SourceNode<string>
{
    protected override async Task ExecuteAsync(IDataPipe<string> output, PipelineContext context, CancellationToken cancellationToken)
    {
        // Stream data line by line without materializing in memory
        await foreach (var line in ReadLinesAsync("large-file.txt", cancellationToken))
        {
            await output.ProduceAsync(line, cancellationToken);
        }
    }
    
    // Helper method that yields lines one at a time
    private async IAsyncEnumerable<string> ReadLinesAsync(string filePath, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(
            new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true));
        
        string? line;
        while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
        {
            yield return line; // Stream one line at a time
        }
    }
}
```

**Common Streaming Patterns:**

| Pattern | Example | Use Case |
|---------|---------|----------|
| Process all items | `await foreach (var item in GetItemsAsync(cancellationToken)) { ... }` | Standard processing of each item |
| Generate data | `for (int i = 0; i < count; i++) { yield return GenerateItem(i); }` | Data generation without materialization |
| Stream from database | `while (await reader.ReadAsync(cancellationToken)) { ... }` | Database cursor processing |
| Transform while streaming | `await foreach (var item in GetSourceAsync()) { yield return Transform(item); }` | Transformation without buffering |

**Benefits of Streaming Patterns:**

1. **Memory Efficiency**: Constant memory usage regardless of data size
2. **Better Performance**: Processing begins immediately without waiting for all data to load
3. **Scalability**: Can handle arbitrarily large datasets without running out of memory
4. **Responsiveness**: Async I/O doesn't block threads, improving overall throughput
5. **Resource Utilization**: Lower GC pressure and better cache locality

**Read More:**

- [Build-Time Resilience Analyzer Guide](../tooling/analyzers/resilience.md)
- [Source Node Best Practices](../core-concepts/nodes/source-nodes.md)
- [Performance Optimization Guide](../advanced-topics/performance-hygiene.md)

---

### NP9401: Missing Dependency Injection

**Message:** `[NP9401] Avoid dependency injection anti-patterns in node implementations. Use constructor injection instead.`

**Category:** Best Practice

**Severity:** Warning

**Cause:** Your node implementation contains dependency injection anti-patterns that can lead to tightly coupled code that is difficult to test and maintain. The analyzer detects these problematic patterns:

1. **Direct service instantiation** using `new` keyword
2. **Static singleton field assignments** that create tightly coupled dependencies
3. **Service locator pattern usage** through GetService or GetRequiredService calls

These anti-patterns violate the Dependency Inversion Principle and make code difficult to test, maintain, and configure.

**Example of Problem Code:**

```csharp
// ❌ PROBLEM: Dependency injection anti-patterns

public class BadTransformNode : TransformNode<string, string>
{
    private readonly BadService _badService = new BadService(); // NP9401: Direct instantiation

    public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult(_badService.Process(item));
    }
}

public class BadSourceNode : SourceNode<int>
{
    private static BadService _service; // Static field

    public BadSourceNode()
    {
        _service = new BadService(); // NP9401: Static singleton assignment
    }
}

public class BadSinkNode : SinkNode<string>
{
    private readonly IServiceProvider _serviceProvider;

    public BadSinkNode(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public override Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var badService = _serviceProvider.GetService(typeof(BadService)) as BadService; // NP9401: Service locator
        return Task.CompletedTask;
    }
}
```

**Solution - Use Constructor Injection:**

```csharp
// ✅ SOLUTION: Constructor injection

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

// ✅ SOLUTION: Multiple dependencies via constructor

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

**Benefits of Constructor Injection:**

1. **Testability**: Easy to mock dependencies for unit testing
2. **Flexibility**: Can swap implementations easily
3. **Dependency Inversion**: Depends on abstractions rather than concretions
4. **Explicit Dependencies**: All dependencies are visible in the constructor
5. **Better Configuration**: Works well with DI containers

**Read More:**

- [Build-Time Resilience Analyzer Guide](../tooling/analyzers/resilience.md)
- [Dependency Injection Best Practices](../architecture/dependency-injection.md)
- [Testing Pipeline Components](../extensions/testing/index.md)

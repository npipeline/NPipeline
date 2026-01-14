---
title: Lambda-Based Node Syntax
description: Using lambda functions to define simple nodes without creating separate classes.
sidebar_position: 4
---

# Lambda-Based Node Syntax

NPipeline provides a simplified syntax for creating nodes using lambda functions instead of defining separate classes. This approach reduces boilerplate code for simple, stateless transformations and is ideal for quick prototyping and simple operations.

## Overview

The lambda-based syntax consists of three main patterns:

- **Transform nodes**: Convert input data to output data
- **Source nodes**: Produce data into the pipeline
- **Sink nodes**: Consume data from the pipeline

For each pattern, NPipeline provides a single, unified method that supports both synchronous and asynchronous operations. The compiler automatically selects the correct overload based on the delegate signature you provide.

## When to Use Lambda Nodes

### ✅ Use Lambda Nodes For

- **Simple, stateless transformations**: String manipulation, type conversion, basic calculations
- **Quick prototyping**: Getting a pipeline working quickly during development
- **Single-use operations**: Logic that won't be reused across pipelines
- **Testing and debugging**: Temporary nodes for diagnostic purposes
- **External service calls**: Short HTTP requests or API calls

### ❌ Use Class-Based Nodes For

- **Complex business logic**: Multi-step transformations requiring comprehensive testing
- **Stateful operations**: Nodes that maintain internal state across executions
- **Reusable components**: Logic used across multiple pipelines
- **Configuration**: Nodes with complex initialization or configuration
- **Error handling**: Operations requiring sophisticated error recovery

## Synchronous Transform Nodes

Use `AddTransform()` with a synchronous delegate for simple, CPU-bound transformations:

```csharp
// Simple transformation
var transform = builder.AddTransform(
    (int x) => x * 2,
    "double");

// With complex objects
var transform = builder.AddTransform(
    (Customer c) => new CustomerDto
    {
        Id = c.Id,
        Name = c.Name.Trim()
    },
    "customerToDto");

// String operations
var transform = builder.AddTransform(
    (string s) => s.ToUpperInvariant(),
    "uppercase");
```

## Asynchronous Transform Nodes

Use `AddTransform()` with an asynchronous delegate for I/O-bound transformations like HTTP requests or database lookups. The method name is the same—the compiler automatically selects the correct overload:

```csharp
// HTTP request transformation
var transform = builder.AddTransform(
    async (string url, ct) =>
    {
        using var client = new HttpClient();
        var response = await client.GetStringAsync(url, ct);
        return response;
    },
    "fetchUrl");

// Database lookup
var transform = builder.AddTransform(
    async (int customerId, ct) =>
    {
        return await _db.GetCustomerAsync(customerId, ct);
    },
    "lookupCustomer");

// File I/O
var transform = builder.AddTransform(
    async (string path, ct) =>
    {
        return await File.ReadAllTextAsync(path, ct);
    },
    "readFile");
```

## Source Nodes

### Synchronous Source (from collections)

Use `AddSource()` with a synchronous delegate that returns an `IEnumerable<T>`:

```csharp
// From array
var source = builder.AddSource(
    () => new[] { 1, 2, 3, 4, 5 },
    "numbers");

// From list
var source = builder.AddSource(
    () => _customers.ToList(),
    "customers");

// From LINQ query
var source = builder.AddSource(
    () => File.ReadAllLines("/data/input.txt"),
    "fileLines");
```

### Asynchronous Source (from async operations)

Use `AddSource()` with an asynchronous delegate that returns an `IAsyncEnumerable<T>`. The method name is the same—the compiler automatically selects the correct overload:

```csharp
// Async database query
var source = builder.AddSource(
    async ct =>
    {
        return await _db.GetAllOrdersAsync(ct);
    },
    "orders");

// Async file reading
var source = builder.AddSource(
    async ct =>
    {
        var lines = await File.ReadAllLinesAsync("/data/input.txt", ct);
        foreach (var line in lines)
        {
            yield return line;
        }
    },
    "fileLines");

// Async HTTP request
var source = builder.AddSource(
    async ct =>
    {
        using var client = new HttpClient();
        var json = await client.GetStringAsync("https://api.example.com/items", ct);
        var items = JsonSerializer.Deserialize<List<Item>>(json);
        foreach (var item in items!)
        {
            yield return item;
        }
    },
    "apiItems");
```

## Sink Nodes

### Synchronous Sink (immediate processing)

Use `AddSink()` with a synchronous delegate that accepts a single argument:

```csharp
// Console output
var sink = builder.AddSink(
    (string line) => Console.WriteLine(line),
    "console");

// Collection accumulation
var results = new List<int>();
var sink = builder.AddSink(
    (int item) => results.Add(item),
    "collect");

// File appending
var sink = builder.AddSink(
    (string line) => File.AppendAllText("/log.txt", line + Environment.NewLine),
    "fileLog");
```

### Asynchronous Sink (async I/O)

Use `AddSink()` with an asynchronous delegate that accepts a value and `CancellationToken`. The method name is the same—the compiler automatically selects the correct overload:

```csharp
// Database insert
var sink = builder.AddSink(
    async (Customer customer, ct) =>
    {
        await _db.InsertCustomerAsync(customer, ct);
    },
    "dbInsert");

// File writing with async
var sink = builder.AddSink(
    async (string line, ct) =>
    {
        await File.AppendAllTextAsync("/log.txt", line + Environment.NewLine, ct);
    },
    "asyncFileLog");

// HTTP POST
var sink = builder.AddSink(
    async (Order order, ct) =>
    {
        using var client = new HttpClient();
        var json = JsonSerializer.Serialize(order);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        await client.PostAsync("https://api.example.com/orders", content, ct);
    },
    "apiPost");
```

## Complete Example: Simple ETL Pipeline

```csharp
public class SimpleCsvEtlPipeline : IPipelineDefinition
{
    private readonly string _inputFile;
    private readonly string _outputFile;

    public SimpleCsvEtlPipeline(string inputFile, string outputFile)
    {
        _inputFile = inputFile;
        _outputFile = outputFile;
    }

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Source: Read CSV file (async variant)
        var source = builder.AddSource(
            async ct =>
            {
                var lines = await File.ReadAllLinesAsync(_inputFile, ct);
                foreach (var line in lines.Skip(1)) // Skip header
                {
                    yield return line;
                }
            },
            "csvRead");

        // Transform: Parse CSV and convert (sync variant)
        var parse = builder.AddTransform(
            (string line) =>
            {
                var parts = line.Split(',');
                return new Customer
                {
                    Id = int.Parse(parts[0]),
                    Name = parts[1].Trim(),
                    Email = parts[2].Trim()
                };
            },
            "csvParse");

        // Transform: Validate and clean (async variant)
        var validate = builder.AddTransform(
            async (Customer customer, ct) =>
            {
                // Async validation (e.g., check against database)
                var isValid = !string.IsNullOrWhiteSpace(customer.Email);
                await Task.Delay(10, ct); // Simulate async validation

                return isValid ? customer : null;
            },
            "validateCustomer");

        // Sink: Write results (async variant)
        var sink = builder.AddSink(
            async (Customer? customer, ct) =>
            {
                if (customer != null)
                {
                    var csv = $"{customer.Id},{customer.Name},{customer.Email}";
                    await File.AppendAllTextAsync(_outputFile, csv + Environment.NewLine, ct);
                }
            },
            "csvWrite");

        // Connect nodes
        builder.Connect(source, parse);
        builder.Connect(parse, validate);
        builder.Connect(validate, sink);
    }
}
```

## Testing Lambda-Based Transformations

Since lambda transformations are embedded in the pipeline definition, you can test them by extracting the logic into separate, testable methods:

```csharp
public static class CustomerTransformations
{
    // Testable logic extracted from lambda
    public static Customer ParseCsvLine(string line)
    {
        var parts = line.Split(',');
        return new Customer
        {
            Id = int.Parse(parts[0]),
            Name = parts[1].Trim(),
            Email = parts[2].Trim()
        };
    }

    // Use in pipeline
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var parse = builder.AddTransform(
            CustomerTransformations.ParseCsvLine,
            "csvParse");
    }
}

// Tested separately
[TestClass]
public class CustomerTransformationTests
{
    [TestMethod]
    public void ParseCsvLine_WithValidInput_ReturnsCustomer()
    {
        var result = CustomerTransformations.ParseCsvLine("1,John Doe,john@example.com");
        Assert.AreEqual(1, result.Id);
        Assert.AreEqual("John Doe", result.Name);
        Assert.AreEqual("john@example.com", result.Email);
    }
}
```

## Hybrid Approach: Named Delegates

For better testability, you can assign lambda functions to named variables that can be tested independently:

```csharp
public class HybridApproachPipeline : IPipelineDefinition
{
    // Testable delegate
    private static readonly Func<int, int> DoubleValue = x => x * 2;

    private static readonly Func<int, CancellationToken, ValueTask<int>> AsyncEnrich = async (x, ct) =>
    {
        await Task.Delay(10, ct);
        return x + 100;
    };

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(() => Enumerable.Range(1, 10), "range");
        var double_val = builder.AddTransform(DoubleValue, "double");
        var enrich = builder.AddTransformAsync(AsyncEnrich, "enrich");
        var sink = builder.AddSink(Console.WriteLine, "console");

        builder.Connect(source, double_val);
        builder.Connect(double_val, enrich);
        builder.Connect(enrich, sink);
    }
}

// Test the delegates
[TestClass]
public class HybridApproachTests
{
    [TestMethod]
    public void DoubleValue_WithInput5_Returns10()
    {
        var result = HybridApproachPipeline.DoubleValue(5);
        Assert.AreEqual(10, result);
    }

    [TestMethod]
    public async Task AsyncEnrich_WithInput5_Returns105()
    {
        var result = await HybridApproachPipeline.AsyncEnrich(5, CancellationToken.None);
        Assert.AreEqual(105, result);
    }
}
```

## Cancellation Token Support

All asynchronous lambda nodes respect the cancellation token, allowing graceful shutdown:

```csharp
var transformAsync = builder.AddTransformAsync(
    async (item, cancellationToken) =>
    {
        // Check cancellation token
        cancellationToken.ThrowIfCancellationRequested();

        // Perform async work with cancellation support
        var result = await SomeAsyncOperation(item, cancellationToken);
        return result;
    },
    "asyncWork");
```

## Performance Considerations

- **Synchronous lambdas**: Zero overhead compared to class-based nodes
- **Asynchronous lambdas**: Minimal overhead; naturally produces `ValueTask` for synchronous completions
- **Value types**: Lambda nodes support value types efficiently without boxing overhead

## Limitations and Best Practices

1. **State Management**: Lambda nodes are stateless by design. For stateful operations, use class-based nodes.
2. **Error Handling**: Simple exceptions are propagated; use error handler middleware for complex recovery logic.
3. **Logging**: Use dependency injection or static loggers within lambdas for diagnostic purposes.
4. **Documentation**: Include XML comments above extracted methods/delegates for clarity.
5. **Reusability**: Extract commonly-used transformations into extension methods or utility classes.

## See Also

- [Node Definition](./node-definition) - Understanding node structure
- [Custom Nodes](./nodes/transform-nodes) - Creating class-based nodes for complex scenarios
- [Defining Pipelines](./defining-pipelines) - Full pipeline builder API reference

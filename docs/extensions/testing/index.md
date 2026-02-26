---
title: Testing Extensions
description: Learn about NPipeline's testing utilities and helpers for writing comprehensive tests.
sidebar_position: 7
slug: /extensions/testing
---

# Testing Extensions

NPipeline provides comprehensive testing support through dedicated extensions that make it easy to write unit and integration tests for your pipelines. These testing utilities help you verify the behavior of your pipelines, mock external dependencies, and ensure data flows correctly through your transformations.

> **Need to test complex scenarios?** See [Advanced Testing](./advanced-testing.md) for strategies with dependencies, mocking, error handling, and integration tests.

> **This guide** covers: **Installation, packages, basic patterns, and simple unit testing**  
> **Advanced Testing** covers: **Mocking services, dependency injection, error handling, and complex scenarios**

## Available Testing Packages

### NPipeline.Extensions.Testing

The core testing package provides essential utilities for testing NPipeline pipelines:

- **In-memory source and sink nodes** for testing data flows without external dependencies
- **Pipeline builder extensions** for setting up test pipelines with fluent syntax
- **Test context extensions** for setting up pipeline execution environments
- **Mock node implementations** for isolating components under test

### NPipeline.Extensions.Testing.AwesomeAssertions

Provides assertion extensions using the AwesomeAssertions library for fluent, readable test assertions:

```csharp
// Example using AwesomeAssertions
sink.ShouldHaveReceived(5);
sink.ShouldContain(expectedItem);
```

### NPipeline.Extensions.Testing.FluentAssertions

Provides assertion extensions using the FluentAssertions library for expressive test assertions:

```csharp
// Example using FluentAssertions
sink.Items.Should().HaveCount(5);
sink.Items.Should().Contain(expectedItem);
```

## Quick Start

### Testing with PipelineTestHarness

The easiest way to test pipelines is using the `PipelineTestHarness<TPipeline>` class, which provides a fluent API for configuring and executing pipelines with automatic error capturing and assertions:

```csharp
using NPipeline.Extensions.Testing;
using NPipeline.Pipeline;

public class MyPipelineTests
{
    [Fact]
    public async Task Pipeline_ShouldTransformDataCorrectly()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<MyPipeline>()
            .WithParameter("input", testData)
            .RunAsync();

        // Assert
        result.AssertSuccess();
        result.AssertNoErrors();
        result.AssertCompletedWithin(TimeSpan.FromSeconds(5));
    }
}
```

### Basic Testing Pattern

Alternatively, you can build pipelines manually for more control by defining a pipeline class that implements `IPipelineDefinition`:

```csharp
using NPipeline.Extensions.Testing;
using NPipeline.Extensions.Testing.AwesomeAssertions; // or FluentAssertions
using NPipeline.Pipeline;
using NPipeline.Execution;

// Define your pipeline as a class implementing IPipelineDefinition
public sealed class MyTestPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<InMemorySourceNode<string>, string>();
        var transform = builder.AddTransform<MyTransform, string, string>();
        var sink = builder.AddSink<InMemorySinkNode<string>, string>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public class MyPipelineTests
{
    [Fact]
    public async Task Pipeline_ShouldTransformDataCorrectly()
    {
        // Arrange
        var testData = new[] { "input1", "input2", "input3" };
        var context = new PipelineContext();
        context.SetSourceData(testData);

        // Act
        await PipelineRunner.Create().RunAsync<MyTestPipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.ShouldHaveReceived(3);
        sink.Items.Should().BeEquivalentTo(expectedData);
    }
}
```

## Testing Strategies

### Unit Testing Individual Nodes

Test individual nodes in isolation:

```csharp
[Fact]
public async Task Transform_ShouldApplyCorrectLogic()
{
    // Arrange
    var transform = new MyTransform();
    var input = new TestData { Value = "test" };
    var context = new PipelineContext();

    // Act
    var result = await transform.ExecuteAsync(input, context, CancellationToken.None);
    
    // Assert
    result.Should().Be("processed_test");
}
```

### Integration Testing Full Pipelines

Test entire pipelines to verify end-to-end behavior:

```csharp
    [Fact]
    public async Task FullPipeline_ShouldProcessDataFlow()
    {
        // Arrange
        var testData = new[] { "test1", "test2", "test3" };
        var context = new PipelineContext();
        context.SetSourceData(testData);

        // Act
        await PipelineRunner.Create().RunAsync<FullTestPipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.ShouldHaveReceived(3);
        sink.Items.Should().NotBeEmpty();
    }
```

### Error Handling Testing

Test error scenarios and recovery:

```csharp
    [Fact]
    public async Task Pipeline_ShouldHandleErrorsGracefully()
    {
        // Arrange
        var testData = new[] { "valid", "invalid", "valid" };
        var context = new PipelineContext();
        context.SetSourceData(testData);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            PipelineRunner.Create().RunAsync<ErrorHandlingTestPipeline>(context));
    }
```

## Pipeline Builder Testing Extensions

The [`PipelineBuilderTestingExtensions`](../../../src/NPipeline.Extensions.Testing/PipelineBuilderTestingExtensions.cs) class provides convenient methods for setting up test pipelines:

### In-Memory Sources

```csharp
// Add empty source
var source = builder.AddInMemorySource<int>();

// Add source with data
var source = builder.AddInMemorySource(new[] { 1, 2, 3 });

// Add named source
var source = builder.AddInMemorySource<int>("MySource");

// Add source with context-backed data
var source = builder.AddInMemorySourceWithDataFromContext<int>(context, new[] { 1, 2, 3 });
```

### In-Memory Sinks

```csharp
// Add sink and register in context
var sink = builder.AddInMemorySink<string>(context);

// Add named sink
var sink = builder.AddInMemorySink<string>("MySink");

// Add sink without context registration
var sink = builder.AddInMemorySink<string>();
```

### Pass-Through Transforms

```csharp
// Add transform that casts from int to string
var transform = builder.AddPassThroughTransform<int, string>();

// Add named transform
var transform = builder.AddPassThroughTransform<string, int>("ToInt");
```

## Context Extensions for Testing

The [`TestingContextExtensions`](../../../src/NPipeline.Extensions.Testing/TestingContextExtensions.cs) provides methods for managing test data:

```csharp
// Set source data for a specific node type
context.SetSourceData(new[] { 1, 2, 3 });

// Set source data for a specific node instance
context.SetSourceData(new[] { 1, 2, 3 }, nodeId: "MyNodeId");

// Retrieve sink from context
var sink = context.GetSink<InMemorySinkNode<string>>();
```

## Best Practices

1. **Use descriptive test names** that clearly indicate what scenario is being tested
2. **Arrange-Act-Assert pattern** for clear test structure
3. **Test both success and failure scenarios** to ensure robust error handling
4. **Mock external dependencies** to isolate the code under test
5. **Use test data builders** for creating complex test scenarios
6. **Assert on both the process and the results** for comprehensive validation
7. **Use context.SetSourceData()** to provide test data to in-memory sources
8. **Register sinks in context** to easily access results after pipeline execution

For more advanced testing scenarios, see the [Dependency Injection](../dependency-injection.md) documentation for patterns involving dependency injection, error handling, and mock services.

---
title: AwesomeAssertions
description: Write expressive, readable assertions for your pipeline tests with the AwesomeAssertions integration.
sidebar_position: 2
---

# AwesomeAssertions Integration

While standard `Assert` statements work perfectly well, assertion libraries can make tests more readable and expressive. `NPipeline.Extensions.Testing.AwesomeAssertions` is a small extension that integrates with the [AwesomeAssertions](https://github.com/Awesome-Assertions/Awesome-Assertions) library, providing a fluent, human-readable syntax for validating the output of your pipelines.

## Installation

To use this integration, you need to add both the `AwesomeAssertions` and the NPipeline extension package to your test project:

```bash
dotnet add package AwesomeAssertions
dotnet add package NPipeline.Extensions.Testing.AwesomeAssertions
```

For the core NPipeline package and other available testing extensions, see the [Installation Guide](../../getting-started/installation.md).

## Pipeline Execution Result Assertions

With `NPipeline.Extensions.Testing`, you can use `PipelineTestHarness<T>` to test your pipelines with fluent assertions on the execution results:

```csharp
using NPipeline.Extensions.Testing;
using NPipeline.Extensions.Testing.AwesomeAssertions;
using AwesomeAssertions;
using Xunit;

public class MyPipelineTests
{
    [Fact]
    public async Task Pipeline_ShouldCompleteSuccessfully()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<MyPipeline>()
            .WithParameter("input", testData)
            .RunAsync();

        // Assert - fluent API with AwesomeAssertions
        result
            .ShouldBeSuccessful()
            .ShouldHaveNoErrors()
            .ShouldCompleteWithin(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Pipeline_ShouldCaptureErrorsGracefully()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<MyPipeline>()
            .WithParameter("input", invalidData)
            .CaptureErrors()
            .RunAsync();

        // Assert
        result
            .ShouldFail()
            .ShouldHaveErrorOfType<InvalidOperationException>()
            .ShouldHaveErrorCount(1);
    }
}
```

### Pipeline Result Assertions

The `AwesomeAssertions` integration provides these assertion extension methods:

| Method | Description |
|--------|-------------|
| `ShouldBeSuccessful()` | Assert pipeline executed successfully (no uncaught exceptions) |
| `ShouldFail()` | Assert pipeline execution failed |
| `ShouldHaveNoErrors()` | Assert no errors were captured |
| `ShouldHaveErrorCount(int)` | Assert specific number of errors were captured |
| `ShouldHaveErrorOfType<TException>()` | Assert at least one error of a specific type |
| `ShouldCompleteWithin(TimeSpan)` | Assert execution completed within duration |

All methods return the result for fluent chaining.

## Sink Node Assertions

For asserting on individual sink nodes, use the existing sink extensions:

```csharp
using NPipeline.Extensions.Testing;
using NPipeline.Extensions.Testing.AwesomeAssertions;
using AwesomeAssertions;

public class InMemorySinkTests
{
    [Fact]
    public async Task Sink_Should_Receive_Data()
    {
        // Arrange
        var inputData = new[] { "a", "b" };
        var context = new PipelineContext();
        context.SetSourceData(inputData);

        var sink = new InMemorySinkNode<string>();
        var builder = new PipelineBuilder();
        var source = builder.AddInMemorySource<string>();
        var transform = builder.AddTransform<ToUpperTransform, string, string>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        var pipeline = builder.Build();

        // Act
        await PipelineRunner.Create().RunAsync(pipeline, context);

        // Assert
        sink.ShouldContain("A");
        sink.ShouldContain("B");
        sink.ShouldHaveReceived(2);
    }
}
```

## Available Extension Methods

The [`InMemorySinkExtensions`](../../../src/NPipeline.Extensions.Testing.AwesomeAssertions/InMemorySinkExtensions.cs) class provides these assertion methods for sink nodes:

### Count Assertions

```csharp
// Assert that sink has received a specific number of items
sink.ShouldHaveReceived(expectedCount);

// Equivalent to:
sink.Items.Count.Should().Be(expectedCount);
```

### Content Assertions

```csharp
// Assert that sink contains a specific item
sink.ShouldContain(expectedItem);

// Assert that sink contains an item matching a predicate
sink.ShouldContain(item => item.StartsWith("A"));

// Assert that sink does not contain a specific item
sink.ShouldNotContain(unexpectedItem);

// Assert that all items in sink satisfy a predicate
sink.ShouldOnlyContain(item => item.Length > 0);
```

## Complete Example: Pipeline with Error Handling

```csharp
using NPipeline.Extensions.Testing;
using NPipeline.Extensions.Testing.AwesomeAssertions;
using AwesomeAssertions;
using Xunit;

public class PipelineWithErrorHandlingTests
{
    [Fact]
    public async Task Pipeline_Should_Filter_And_Transform_Data()
    {
        // Arrange
        var inputData = new[] { "apple", "banana", "cherry", "date" };
        var context = new PipelineContext();
        context.SetSourceData(inputData);

        var builder = new PipelineBuilder();
        var source = builder.AddInMemorySource<string>();
        var filter = builder.AddTransform<FruitFilter, string, string>();
        var transform = builder.AddTransform<ToUpperTransform, string, string>();
        var sink = builder.AddInMemorySink<string>(context);

        builder.Connect(source, filter);
        builder.Connect(filter, transform);
        builder.Connect(transform, sink);

        var pipeline = builder.Build();

        // Act
        await PipelineRunner.Create().RunAsync(pipeline, context);

        // Assert
        sink.ShouldHaveReceived(2); // Only "apple" and "banana" should pass filter
        sink.ShouldContain("APPLE");
        sink.ShouldContain("BANANA");
        sink.ShouldNotContain("CHERRY"); // "cherry" and "date" should be filtered out
        sink.ShouldOnlyContain(item => item.Length >= 5); // All results should be 5+ characters
    }

    [Fact]
    public async Task Pipeline_With_Error_Capturing_Should_Report_Errors()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<PipelineThatCanFail>()
            .WithParameter("input", problemData)
            .CaptureErrors(PipelineErrorDecision.ContinueWithoutNode)
            .RunAsync();

        // Assert - fluent assertions make this very readable
        result
            .ShouldHaveErrorCount(2)
            .ShouldHaveErrorOfType<ValidationException>()
            .ShouldCompleteWithin(TimeSpan.FromSeconds(10));
        
        // Can also access the sink
        var sink = result.GetSink<InMemorySinkNode<ProcessedData>>();
        sink.ShouldHaveReceived(expectedSuccessfulCount);
    }

    [Fact]
    public async Task Pipeline_Should_Preserve_Custom_Error_Handlers()
    {
        // Arrange - create a context with custom error handler
        var errorLog = new List<string>();
        var customHandler = new LoggingErrorHandler(errorLog);
        
        var context = new PipelineContext();
        context.PipelineErrorHandler = customHandler;

        // Act - test harness will chain custom handler with capturing handler
        var result = await new PipelineTestHarness<MyPipeline>(context)
            .WithParameter("input", testData)
            .CaptureErrors()
            .RunAsync();

        // Assert - both error capturing AND custom logging happened
        errorLog.Should().NotBeEmpty("custom handler should have logged");
        result.Errors.Should().NotBeEmpty("errors should be captured");
    }
}
```

## Benefits of AwesomeAssertions

- **Readability:** `sink.ShouldHaveReceived(5)` reads more like a natural sentence than `Assert.Equal(5, sink.Items.Count)`.
- **Expressiveness:** AwesomeAssertions provides a rich set of assertions for collections, objects, exceptions, and more. For example, you can assert that a collection contains a specific item, that all items match a predicate, or that the output is in a specific order.
- **Detailed Error Messages:** When an assertion fails, AwesomeAssertions provides detailed output highlighting exactly what was different between the actual and expected values, which can significantly speed up debugging.
- **Fluent Chaining:** You can chain multiple assertions together for complex validation scenarios.

## Next Steps

- **[FluentAssertions](./fluent-assertions.md)**: If you prefer FluentAssertions, NPipeline also provides a similar integration for that library.
- **[Testing Extensions](./index.md)**: Return to the main testing documentation for more testing patterns and utilities.


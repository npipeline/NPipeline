---
title: FluentAssertions
description: Use the popular FluentAssertions library to write strongly-typed, descriptive, and readable tests for your NPipelines.
sidebar_position: 3
---

# FluentAssertions Integration

For developers who prefer the widely-used [FluentAssertions](https://fluentassertions.com/) library, `NPipeline.Extensions.Testing.FluentAssertions` provides a seamless integration. This extension allows you to use the rich, fluent, and highly readable assertion syntax of FluentAssertions to validate your pipeline's output and execution results.

## Installation

To use this integration, add both the `FluentAssertions` and the NPipeline extension package to your test project:

```bash
dotnet add package FluentAssertions
dotnet add package NPipeline.Extensions.Testing.FluentAssertions
```

For the core NPipeline package and other available testing extensions, see the [Installation Guide](../../getting-started/installation.md).

## Pipeline Execution Result Assertions

With `NPipeline.Extensions.Testing`, you can use `PipelineTestHarness<T>` to test your pipelines with fluent assertions on the execution results:

```csharp
using NPipeline.Extensions.Testing;
using NPipeline.Extensions.Testing.FluentAssertions;
using Xunit;
using FluentAssertions;

public class MyPipelineTests
{
    [Fact]
    public async Task Pipeline_ShouldCompleteSuccessfully()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<MyPipeline>()
            .WithParameter("input", testData)
            .RunAsync();

        // Assert - fluent API with FluentAssertions
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

The `FluentAssertions` integration provides these assertion extension methods:

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

For asserting on individual sink nodes, the existing sink extensions still apply:

```csharp
using NPipeline.Extensions.Testing;
using NPipeline.Extensions.Testing.FluentAssertions;
using FluentAssertions;

public sealed class SinkTestPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<InMemorySourceNode<string>, string>();
        var sink = builder.AddSink<InMemorySinkNode<string>, string>();

        builder.Connect(source, sink);
    }
}

public class InMemorySinkTests
{
    [Fact]
    public async Task Sink_Should_Receive_Data()
    {
        // Arrange
        var inputData = new[] { "a", "b", "c" };
        var context = new PipelineContext();
        context.SetSourceData(inputData);

        // Act
        await PipelineRunner.Create().RunAsync<SinkTestPipeline>(context);

        // Assert
        var sink = context.GetSink<InMemorySinkNode<string>>();
        sink.ShouldHaveReceived(3);
        sink.ShouldContain("a");
        sink.ShouldContain(x => x.Length == 1);
        sink.ShouldOnlyContain(x => !string.IsNullOrEmpty(x));
    }
}
```

## Complete Example: Testing with Error Handling

```csharp
using NPipeline.Extensions.Testing;
using NPipeline.Extensions.Testing.FluentAssertions;
using Xunit;
using FluentAssertions;

public class PipelineWithErrorHandlingTests
{
    [Fact]
    public async Task Pipeline_With_Error_Capturing_Should_Report_Errors()
    {
        // Arrange
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

## Why Use FluentAssertions?

- **Strongly-Typed and Discoverable:** The fluent API guides you to the right assertions, with excellent IntelliSense support.
- **Rich Collection Support:** FluentAssertions has an extensive set of assertions for collections, allowing you to check for order, equivalency, subsets, and more.
- **Clear Failure Messages:** When a test fails, FluentAssertions provides exceptionally clear and detailed error messages that pinpoint the exact cause of the failure, making debugging much faster.
- **Chainable Assertions:** Pipeline result assertions chain together, allowing elegant multi-assertion test patterns.

This integration allows you to leverage the full power of FluentAssertions to write robust and maintainable tests for your NPipelines.

## Next Steps

- **[AwesomeAssertions](./awesome-assertions.md)**: If you prefer AwesomeAssertions, NPipeline also provides a similar integration for that library.
- **[Testing Extensions](./index.md)**: Return to the main testing documentation for more testing patterns and utilities.
- **[Connectors](../../connectors/index.md)**: Explore the available connectors for reading from and writing to external systems like CSV files.

# NPipeline Extensions Testing

NPipeline Extensions Testing is a comprehensive testing utilities package designed to help developers test their NPipeline pipelines effectively. This package
provides in-memory test nodes, a test harness framework, assertion helpers, and pipeline testing utilities to simplify unit testing, integration testing, and
performance testing of data processing pipelines.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Extensions.Testing
```

## Requirements

- **.NET 8.0**, **9.0**, or **10.0**
- **NPipeline** core package
- **xUnit**, **NUnit**, or **MSTest** for test framework integration

## Key Features

- **In-Memory Test Nodes**: Source and sink nodes that operate entirely in memory for fast, isolated testing
- **Test Harness Framework**: Fluent API for setting up and executing pipeline tests with assertion capabilities
- **Assertion Helpers**: Utilities to verify pipeline behavior, output, and performance characteristics
- **Pipeline Testing Utilities**: Specialized classes for running pipelines in test scenarios and capturing results
- **Error Simulation**: Components to simulate errors and test error handling behavior
- **Performance Testing**: Built-in timing and metrics collection for performance validation

## Usage Examples

### Basic Pipeline Testing

```csharp
using NPipeline.Extensions.Testing;
using Xunit;

public class MyPipelineTests
{
    [Fact]
    public async Task MyPipeline_ShouldProcessDataCorrectly()
    {
        // Arrange
        var testData = new List<string> { "item1", "item2", "item3" };

        // Act
        var result = await new PipelineTestHarness<MyProcessingPipeline>()
            .WithParameter("inputData", testData)
            .CaptureErrors()
            .RunAsync();

        // Assert
        result.Success.Should().BeTrue();
        result.Errors.Should().BeEmpty();
        result.Duration.Should().BeLessThan(TimeSpan.FromSeconds(1));
    }
}
```

### In-Memory Node Usage

```csharp
// Using InMemorySourceNode for test data
var sourceData = new List<int> { 1, 2, 3, 4, 5 };
var source = new InMemorySourceNode<int>(sourceData);

// Using InMemorySinkNode to capture results
var sink = new InMemorySinkNode<string>();

// Build pipeline with test nodes
var pipeline = PipelineBuilder
    .Start.With(source)
    .Then.Transform<int, string>(item => $"Processed: {item}")
    .End.With(sink)
    .Build();

// Execute pipeline
await PipelineRunner.Create().RunAsync(pipeline);

// Verify results
sink.Items.Should().HaveCount(5);
sink.Items.Should().Contain("Processed: 3");
```

### Test Harness Setup

```csharp
// Advanced test harness configuration
var result = await new PipelineTestHarness<ComplexPipeline>()
    .WithParameter("batchSize", 100)
    .WithParameter("timeout", TimeSpan.FromSeconds(30))
    .WithContextItem("testMode", true)
    .WithExecutionObserver(new TestExecutionObserver())
    .CaptureErrors(PipelineErrorDecision.ContinueWithoutNode)
    .RunAsync();

// Detailed assertions
result.Success.Should().BeTrue();
result.Context.Items["processedCount"].Should().Be(42);
result.Context.Metrics["totalDuration"].Should().BeLessThan(5000);
```

### Assertion Examples

```csharp
// Using PipelineExecutionResultExtensions for assertions
result.Should().Succeed();
result.Should().CompleteWithin(TimeSpan.FromSeconds(5));
result.Should().HaveNoErrors();

// Accessing captured errors for detailed validation
if (!result.Success)
{
    result.Errors.Should().Contain(e => e.Message.Contains("expected error"));
    result.Errors.Should().HaveCount(1);
}

// Verifying pipeline metrics
result.Context.Metrics.Should().ContainKey("itemsProcessed");
result.Context.Metrics["itemsProcessed"].Should().BeGreaterThan(0);
```

## Testing Patterns

### Unit Testing Pipelines

For unit testing individual pipeline components:

```csharp
public class TransformNodeTests
{
    [Fact]
    public async Task CustomTransform_ShouldApplyLogicCorrectly()
    {
        // Arrange
        var mockTransform = new MockNode<string, int>((input, context, token) =>
        {
            return Task.FromResult(input.Length);
        });

        var source = new InMemorySourceNode<string>(new[] { "short", "medium", "very long" });
        var sink = new InMemorySinkNode<int>();

        // Act
        var pipeline = PipelineBuilder
            .Start.With(source)
            .Then.With(mockTransform)
            .End.With(sink)
            .Build();

        await PipelineRunner.Create().RunAsync(pipeline);

        // Assert
        sink.Items.Should().BeEquivalentTo(new[] { 5, 6, 9 });
    }
}
```

### Integration Testing

For testing complete pipeline flows:

```csharp
public class PipelineIntegrationTests
{
    [Fact]
    public async Task EndToEndPipeline_ShouldProcessRealWorldData()
    {
        // Arrange
        var testData = LoadTestData();
        var pipeline = new DataProcessingPipeline();

        // Act
        var result = await new PipelineTestHarness<DataProcessingPipeline>()
            .WithParameter("dataSource", testData)
            .CaptureErrors()
            .RunAsync();

        // Assert
        result.Should().Succeed();
        result.Context.GetSink<ProcessedDataSink>().Items.Should().HaveCount(testData.Count);
        result.Context.Metrics["processingRate"].Should().BeGreaterThan(100); // items per second
    }
}
```

### Performance Testing

For validating performance characteristics:

```csharp
public class PerformanceTests
{
    [Fact]
    public async Task Pipeline_ShouldHandleLargeVolumeEfficiently()
    {
        // Arrange
        var largeDataSet = GenerateTestData(10000);

        // Act
        var result = await new PipelineTestHarness<HighVolumePipeline>()
            .WithParameter("inputData", largeDataSet)
            .RunAsync();

        // Assert
        result.Should().Succeed();
        result.Duration.Should().BeLessThan(TimeSpan.FromSeconds(10));
        result.Context.Metrics["throughput"].Should().BeGreaterThan(1000); // items per second
        result.Context.Metrics["memoryUsage"].Should().BeLessThan(100 * 1024 * 1024); // 100MB
    }
}
```

## License

MIT License - see LICENSE file for details.

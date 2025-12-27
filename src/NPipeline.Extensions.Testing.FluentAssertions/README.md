# NPipeline.Extensions.Testing.FluentAssertions

NPipeline.Extensions.Testing.FluentAssertions provides FluentAssertions extensions for testing NPipeline pipelines. This package adds fluent assertion methods
specifically designed for pipeline execution results and in-memory sink nodes, making it easier to write expressive and readable tests for your data processing
pipelines.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Extensions.Testing.FluentAssertions
```

## Features

- **Fluent Assertion Methods**: Provides expressive assertion methods for pipeline testing
- **Pipeline Execution Assertions**: Assert on pipeline success, failure, errors, and execution time
- **In-Memory Sink Assertions**: Validate data captured by in-memory sink nodes
- **FluentAssertions Integration**: Seamlessly integrates with the popular FluentAssertions library
- **Chainable Assertions**: All methods return the original object for fluent chaining

## Usage

### Basic Pipeline Execution Assertions

```csharp
using NPipeline.Extensions.Testing.FluentAssertions;
using FluentAssertions;

// Execute a pipeline
var result = await pipeline.ExecuteAsync();

// Assert successful execution
result.ShouldBeSuccessful()
      .ShouldHaveNoErrors()
      .ShouldCompleteWithin(TimeSpan.FromSeconds(5));

// Assert failed execution
result.ShouldFail()
      .ShouldHaveErrorCount(1)
      .ShouldHaveErrorOfType<InvalidOperationException>();
```

### In-Memory Sink Assertions

```csharp
using NPipeline.Extensions.Testing.FluentAssertions;

// Create an in-memory sink for testing
var sink = new InMemorySinkNode<int>();

// Assert on received items
sink.ShouldHaveReceived(5);
sink.ShouldContain(42);
sink.ShouldNotContain(99);

// Assert with predicates
sink.ShouldContain(x => x > 10);
sink.ShouldOnlyContain(x => x >= 0);
```

### Complete Test Example

```csharp
using NPipeline.Extensions.Testing.FluentAssertions;
using FluentAssertions;

[Fact]
public async Task Pipeline_Should_Process_Data_Correctly()
{
    // Arrange
    var source = new InMemorySourceNode<int>([1, 2, 3, 4, 5]);
    var transform = new TransformNode<int, int>(x => x * 2);
    var sink = new InMemorySinkNode<int>();

    var pipeline = PipelineBuilder.Create()
        .AddSource(source)
        .AddNode(transform)
        .AddSink(sink)
        .Build();

    // Act
    var result = await pipeline.ExecuteAsync();

    // Assert
    result.ShouldBeSuccessful()
          .ShouldHaveNoErrors()
          .ShouldCompleteWithin(TimeSpan.FromSeconds(1));

    sink.ShouldHaveReceived(5)
        .ShouldContain(2)
        .ShouldContain(10)
        .ShouldOnlyContain(x => x > 0);
}
```

## Requirements

- **.NET 8.0** or later
- **FluentAssertions 8.8.0** or later
- **NPipeline.Extensions.Testing** (automatically included as a dependency)

## License

MIT License - see LICENSE file for details.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Extensions.Testing](https://www.nuget.org/packages/NPipeline.Extensions.Testing)** - Core testing utilities for NPipeline
- **[NPipeline.Extensions.Testing.AwesomeAssertions](https://www.nuget.org/packages/NPipeline.Extensions.Testing.AwesomeAssertions)** - Alternative assertion
  library support

## Support

- **Documentation**: [https://npipeline.readthedocs.io](https://npipeline.readthedocs.io)
- **Issues**: [GitHub Issues](https://github.com/npipeline/NPipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/npipeline/NPipeline/discussions)
- **Discord**: [NPipeline Community](https://discord.gg/npipeline)

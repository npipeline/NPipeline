# NPipeline.Extensions.Testing.AwesomeAssertions

NPipeline.Extensions.Testing.AwesomeAssertions provides AwesomeAssertions extensions for testing NPipeline pipelines. This package adds assertion methods
specifically designed for pipeline execution results and in-memory sink nodes, making it easier to write expressive and readable tests for your data processing
pipelines using the AwesomeAssertions library.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Extensions.Testing.AwesomeAssertions
```

## Features

- **Pipeline Execution Assertions**: Assert on pipeline success, failure, errors, and execution time
- **AwesomeAssertions Integration**: Seamlessly integrates with the AwesomeAssertions library
- **In-Memory Sink Assertions**: Validate data captured by in-memory sink nodes
- **Test Result Validation**: Comprehensive validation of pipeline execution results
- **Chainable Assertions**: All methods support fluent chaining for readable test code

## Usage

### Basic Pipeline Execution Assertions

```csharp
using NPipeline.Extensions.Testing.AwesomeAssertions;
using AwesomeAssertions;

// Execute a pipeline
var result = await pipeline.ExecuteAsync();

// Assert successful execution
Expect(result).ToBeSuccessful()
    .ToHaveNoErrors()
    .ToCompleteWithin(TimeSpan.FromSeconds(5));

// Assert failed execution
Expect(result).ToFail()
    .ToHaveErrorCount(1)
    .ToHaveErrorOfType<InvalidOperationException>();
```

### In-Memory Sink Assertions

```csharp
using NPipeline.Extensions.Testing.AwesomeAssertions;

// Create an in-memory sink for testing
var sink = new InMemorySinkNode<int>();

// Assert on received items
Expect(sink).ToHaveReceived(5);
Expect(sink).ToContain(42);
Expect(sink).Not.ToContain(99);

// Assert with predicates
Expect(sink).ToContain(x => x > 10);
Expect(sink).ToOnlyContain(x => x >= 0);
```

### Complete Test Example

```csharp
using NPipeline.Extensions.Testing.AwesomeAssertions;
using AwesomeAssertions;

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
    Expect(result).ToBeSuccessful()
        .ToHaveNoErrors()
        .ToCompleteWithin(TimeSpan.FromSeconds(1));

    Expect(sink).ToHaveReceived(5)
        .ToContain(2)
        .ToContain(10)
        .ToOnlyContain(x => x > 0);
}
```

## Requirements

- **.NET 8.0** or later
- **AwesomeAssertions 1.0.0** or later
- **NPipeline.Extensions.Testing** (automatically included as a dependency)

## License

MIT License - see LICENSE file for details.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Extensions.Testing](https://www.nuget.org/packages/NPipeline.Extensions.Testing)** - Core testing utilities for NPipeline
- **[NPipeline.Extensions.Testing.FluentAssertions](https://www.nuget.org/packages/NPipeline.Extensions.Testing.FluentAssertions)** - FluentAssertions support
  for NPipeline testing

## Support

- **Documentation**: [https://npipeline.readthedocs.io](https://npipeline.readthedocs.io)
- **Issues**: [GitHub Issues](https://github.com/npipeline/NPipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/npipeline/NPipeline/discussions)
- **Discord**: [NPipeline Community](https://discord.gg/npipeline)

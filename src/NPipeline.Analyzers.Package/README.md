# NPipeline Analyzers

NPipeline Analyzers is a comprehensive Roslyn analyzer package designed to help developers build efficient, robust, and performant data processing pipelines
using the NPipeline framework. This package provides real-time diagnostics and code fixes to detect and resolve common pipeline configuration issues,
performance bottlenecks, and anti-patterns.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Analyzers
```

## Features

- **Real-time Diagnostics**: Detects common pipeline configuration issues and performance anti-patterns as you code
- **Automated Code Fixes**: Provides one-click fixes for most detected issues
- **Performance Optimization**: Identifies bottlenecks in hot paths and suggests optimizations
- **Resilience Patterns**: Ensures proper error handling and cancellation token usage
- **Configuration Validation**: Validates pipeline configuration parameters for optimal performance
- **Async/Await Best Practices**: Enforces proper async patterns in pipeline implementations

## Supported Analyzers (18)

The package includes 18 comprehensive analyzers covering different aspects of pipeline development:

### Performance Analyzers

1. **AnonymousObjectAllocationAnalyzer** - Detects anonymous object allocations in hot paths that can cause GC pressure
2. **InefficientStringOperationsAnalyzer** - Identifies inefficient string concatenation and manipulation in performance-critical code
3. **LinqInHotPathsAnalyzer** - Detects LINQ operations in high-frequency execution paths that cause unnecessary allocations
4. **ValueTaskOptimizationAnalyzer** - Identifies opportunities to optimize synchronous completions with ValueTask

### Configuration Analyzers

5. **BatchingConfigurationMismatchAnalyzer** - Detects mismatched batch size and timeout configurations
6. **InappropriateParallelismConfigurationAnalyzer** - Identifies inappropriate parallelism settings that can cause resource contention
7. **TimeoutConfigurationAnalyzer** - Detects timeout values that are too short or too long for the workload type
8. **UnboundedMaterializationConfigurationAnalyzer** - Identifies potential memory leaks from unbounded materialization

### Async/Cancellation Analyzers

9. **BlockingAsyncOperationAnalyzer** - Detects blocking calls on async operations that can cause deadlocks
10. **CancellationTokenRespectAnalyzer** - Ensures proper cancellation token propagation and usage
11. **SynchronousOverAsyncAnalyzer** - Identifies synchronous-over-asynchronous anti-patterns

### Error Handling Analyzers

12. **InefficientExceptionHandlingAnalyzer** - Detects inefficient exception handling patterns in hot paths
13. **OperationCanceledExceptionAnalyzer** - Ensures proper handling of OperationCanceledException

### Pipeline-Specific Analyzers

14. **DependencyInjectionAnalyzer** - Detects dependency injection anti-patterns in node implementations
15. **PipelineContextAccessAnalyzer** - Identifies unsafe access patterns on PipelineContext properties
16. **ResilientExecutionConfigurationAnalyzer** - Validates resilient execution strategy configurations
17. **SinkNodeInputConsumptionAnalyzer** - Ensures proper consumption of input in sink nodes
18. **SourceNodeStreamingAnalyzer** - Detects non-streaming patterns in source nodes

## Supported Code Fix Providers (18)

Each analyzer has a corresponding code fix provider that can automatically resolve detected issues:

1. **AnonymousObjectAllocationCodeFixProvider** - Converts anonymous objects to named types or ValueTuples
2. **BatchingConfigurationMismatchCodeFixProvider** - Adjusts batch size and timeout configurations
3. **BlockingAsyncOperationCodeFixProvider** - Replaces blocking calls with async alternatives
4. **CancellationTokenRespectCodeFixProvider** - Adds cancellation tokens to method signatures and calls
5. **DependencyInjectionCodeFixProvider** - Refactors to proper dependency injection patterns
6. **InappropriateParallelismConfigurationCodeFixProvider** - Optimizes parallelism settings based on workload
7. **InefficientExceptionHandlingCodeFixProvider** - Refactors exception handling for better performance
8. **InefficientStringOperationsCodeFixProvider** - Replaces inefficient string operations with optimized alternatives
9. **LinqInHotPathsCodeFixProvider** - Converts LINQ operations to more efficient foreach loops
10. **OperationCanceledExceptionCodeFixProvider** - Adds proper OperationCanceledException handling
11. **PipelineContextAccessCodeFixProvider** - Adds null checks and conditional operators for safe access
12. **ResilientExecutionConfigurationCodeFixProvider** - Configures resilient execution strategies
13. **SinkNodeInputConsumptionCodeFixProvider** - Adds proper async enumeration for input consumption
14. **SourceNodeStreamingCodeFixProvider** - Converts to streaming patterns for source nodes
15. **SynchronousOverAsyncCodeFixProvider** - Replaces sync-over-async patterns with proper async
16. **TimeoutConfigurationCodeFixProvider** - Optimizes timeout values based on workload characteristics
17. **UnboundedMaterializationConfigurationCodeFixProvider** - Adds bounds to materialization operations
18. **ValueTaskOptimizationCodeFixProvider** - Converts Task.FromResult patterns to ValueTask

## Example Diagnostics

### Performance Issues

```csharp
// Inefficient string concatenation in a hot path
public string ProcessItem(Item item)
{
    string result = "Processing: " + item.Name + " at " + DateTime.Now;
    return result;
}
// Diagnostic: NPIPE001: Use StringBuilder or string interpolation for efficient string concatenation
```

```csharp
// LINQ in hot path
public List<Result> TransformItems(List<Item> items)
{
    return items.Where(x => x.IsValid)
                 .Select(x => new Result(x))
                 .ToList();
}
// Diagnostic: NPIPE002: Avoid LINQ operations in hot paths
```

### Configuration Issues

```csharp
// Inappropriate parallelism for I/O-bound work
var strategy = new ParallelExecutionStrategy
{
    DegreeOfParallelism = Environment.ProcessorCount
};
// Diagnostic: NPIPE003: High parallelism for I/O-bound work may cause resource contention
```

```csharp
// Timeout too short for complex processing
var resilientStrategy = new ResilientExecutionStrategy
{
    Timeout = TimeSpan.FromMilliseconds(100)
};
// Diagnostic: NPIPE004: Timeout may be too short for the workload type
```

### Async/Await Issues

```csharp
// Blocking on async code
public void ProcessData()
{
    var result = GetDataAsync().Result;
}
// Diagnostic: NPIPE005: Avoid blocking on async operations
```

```csharp
// Not respecting cancellation token
public async Task ProcessAsync(CancellationToken cancellationToken)
{
    await ProcessItemAsync(); // Missing cancellationToken parameter
}
// Diagnostic: NPIPE006: Async method should respect cancellation token
```

## Troubleshooting

### Analyzers Not Running

1. **Ensure the package is installed** in the project you're working on
2. **Restart Visual Studio** after installing the package
3. **Check that analyzers are enabled** in your project settings:
   ```xml
   <PropertyGroup>
     <EnableNETAnalyzers>true</EnableNETAnalyzers>
     <AnalysisMode>AllEnabledByDefault</AnalysisMode>
   </PropertyGroup>
   ```

### Code Fixes Not Available

1. **Make sure the diagnostic is active** (not suppressed)
2. **Check that the file is saved** - some fixes require saved files
3. **Verify the fix is applicable** - not all issues have automatic fixes

### Performance Impact

The analyzers are designed to have minimal impact on build performance:

- Most analysis is incremental and only runs on changed files
- Complex analyses are limited to specific contexts (hot paths, pipeline nodes)
- Caching is used to avoid redundant analysis

If you experience performance issues:

1. **Update to the latest version** of the package
2. **Exclude test projects** from analysis if not needed
3. **Consider disabling specific analyzers** that aren't relevant to your project

## Configuration

You can configure the behavior of the analyzers using an `.editorconfig` file:

```ini
# Severity levels
dotnet_diagnostic.NPIPE001.severity = warning
dotnet_diagnostic.NPIPE002.severity = suggestion

# Disable specific analyzers
dotnet_diagnostic.NPIPE003.severity = none

# Configure hot path detection
dotnet_code_quality.maximum_hot_path_complexity = 20
```

## Integration with Build Process

The analyzers integrate seamlessly with your build process:

```bash
# Build with warnings as errors
dotnet build --warnaserror

# Run specific analyzers
dotnet build -p:RunAnalyzers=true -p:AnalyzerPlugins=NPipeline.Analyzers
```

## Contributing

We welcome contributions to the NPipeline Analyzers package! Here's how you can help:

### Reporting Issues

1. **Check existing issues** to avoid duplicates
2. **Provide a minimal reproduction** when reporting bugs
3. **Include diagnostic IDs** and code examples when possible
4. **Describe the expected vs. actual behavior**

### Submitting Pull Requests

1. **Fork the repository** and create a feature branch
2. **Follow the existing code style** and patterns
3. **Add tests** for new analyzers or fixes
4. **Update documentation** for any new features
5. **Ensure all tests pass** before submitting

### Development Setup

```bash
# Clone the repository
git clone https://github.com/npipeline/NPipeline.git
cd NPipeline

# Build the solution
dotnet build

# Run tests
dotnet test

# Pack the analyzer package
dotnet pack src/NPipeline.Analyzers.Package
```

### Adding New Analyzers

When adding a new analyzer:

1. **Inherit from DiagnosticAnalyzer** and use the `[DiagnosticAnalyzer]` attribute
2. **Follow the naming convention**: `[Name]Analyzer.cs`
3. **Provide clear diagnostic messages** with actionable advice
4. **Implement a corresponding code fix provider** when possible
5. **Add comprehensive tests** covering various scenarios
6. **Update the documentation** with the new analyzer's purpose and usage

### Code Style

- **Follow C# conventions** as defined in `.editorconfig`
- **Use XML documentation** for public APIs
- **Keep methods small and focused**
- **Add unit tests** for all functionality
- **Use meaningful variable and method names**

## Requirements

- **.NET Standard 2.0** compatible IDE
- **Visual Studio 2017+** or **JetBrains Rider** or **VS Code** with C# extension
- **C# 8.0** or later for full feature support

## License

MIT License - see LICENSE file for details.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Extensions](https://www.nuget.org/packages/NPipeline.Extensions)** - Additional pipeline components
- **[NPipeline.Benchmarks](https://www.nuget.org/packages/NPipeline.Benchmarks)** - Performance benchmarks

## Support

- **Documentation**: [https://npipeline.readthedocs.io](https://npipeline.readthedocs.io)
- **Issues**: [GitHub Issues](https://github.com/npipeline/NPipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/npipeline/NPipeline/discussions)
- **Discord**: [NPipeline Community](https://discord.gg/npipeline)

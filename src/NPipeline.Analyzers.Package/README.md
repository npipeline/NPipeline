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

## Requirements

- **.NET Standard 2.0** compatible IDE
- **Visual Studio 2017+** or **JetBrains Rider** or **VS Code** with C# extension
- **C# 8.0** or later for full feature support

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

### Configuration Analyzers

1. **BatchingConfigurationMismatchAnalyzer** - Detects mismatched batch size and timeout configurations
2. **InappropriateParallelismConfigurationAnalyzer** - Identifies inappropriate parallelism settings that can cause resource contention
3. **TimeoutConfigurationAnalyzer** - Detects circuit breaker timings that cannot work (non-positive durations, or a `MaxPause` shorter than `OpenDuration` with `WhenOpen = Pause`)

### Async/Cancellation Analyzers

1. **BlockingAsyncOperationAnalyzer** - Detects blocking calls on async operations that can cause deadlocks
2. **CancellationTokenRespectAnalyzer** - Ensures proper cancellation token propagation and usage
3. **SynchronousOverAsyncAnalyzer** - Identifies synchronous-over-asynchronous anti-patterns

### Error Handling Analyzers

1. **InefficientExceptionHandlingAnalyzer** - Detects inefficient exception handling patterns in hot paths
2. **OperationCanceledExceptionAnalyzer** - Ensures proper handling of OperationCanceledException
3. **NodeKindResilienceMisuseAnalyzer** - Detects `ItemRetry`, `NodeRestart`, or `CircuitBreaker` configured for a source, sink, aggregate, or join node
4. **UnconditionalRetryDecisionAnalyzer** - Detects a resilience policy that returns `Retry` without consulting `failure.CanRetry`

### Pipeline-Specific Analyzers

1. **DependencyInjectionAnalyzer** - Detects dependency injection anti-patterns in node implementations
2. **PipelineContextAccessAnalyzer** - Identifies unsafe access patterns on PipelineContext properties
3. **ResilientExecutionConfigurationAnalyzer** - Detects `RestartNode` returned where it cannot restart a node, or where `NodeRestart` is never enabled
4. **SinkNodeInputConsumptionAnalyzer** - Ensures proper consumption of input in sink nodes
5. **SourceNodeStreamingAnalyzer** - Detects non-streaming patterns in source nodes

## Supported Code Fix Providers (14)

These code fix providers can automatically resolve detected issues:

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
12. **SinkNodeInputConsumptionCodeFixProvider** - Adds proper async enumeration for input consumption
13. **SourceNodeStreamingCodeFixProvider** - Converts to streaming patterns for source nodes
14. **SynchronousOverAsyncCodeFixProvider** - Replaces sync-over-async patterns with proper async

## Example Diagnostics

### Performance Issues

```csharp
// Inefficient string concatenation in a hot path
public string ProcessItem(Item item)
{
    string result = "Processing: " + item.Name + " at " + DateTime.Now;
    return result;
}
// Diagnostic: NP9104: Inefficient string operation detected
```

```csharp
// LINQ in hot path
public List<Result> TransformItems(List<Item> items)
{
    return items.Where(x => x.IsValid)
                 .Select(x => new Result(x))
                 .ToList();
}
// Diagnostic: NP9103: LINQ operation detected in hot path
```

### Configuration Issues

```csharp
// Inappropriate parallelism for I/O-bound work
var strategy = new ParallelExecutionStrategy
{
    DegreeOfParallelism = Environment.ProcessorCount
};
// Diagnostic: NP9003: Inappropriate parallelism configuration detected
```

```csharp
// Item retry on a sink: only transform nodes retry items
builder.WithResilience(sink, o => o with { ItemRetry = ItemRetryOptions.Default });
// Diagnostic: NP9204: 'sink' is a sink node, but its resilience options set ItemRetry, which only transform nodes use; building the pipeline will fail
```

### Async/Await Issues

```csharp
// Blocking on async code
public void ProcessData()
{
    var result = GetDataAsync().Result;
}
// Diagnostic: NP9101: Avoid blocking calls in async methods
```

```csharp
// Not respecting cancellation token
public async Task ProcessAsync(CancellationToken cancellationToken)
{
    await ProcessItemAsync(); // Missing cancellationToken parameter
}
// Diagnostic: NP9203: Method should respect cancellation token
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
dotnet_diagnostic.NP9104.severity = warning
dotnet_diagnostic.NP9103.severity = suggestion

# Disable specific analyzers
dotnet_diagnostic.NP9003.severity = none

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

## License

This package is licensed under the [MIT License](LICENSE.txt). You are free to use, modify, and distribute it in personal, open-source, and commercial projects without restriction.

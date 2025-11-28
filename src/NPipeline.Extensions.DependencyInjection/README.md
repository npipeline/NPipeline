# NPipeline Extensions for DependencyInjection

NPipeline.Extensions.DependencyInjection provides seamless integration between NPipeline and Microsoft.Extensions.DependencyInjection, enabling automatic node discovery, service lifetime management, and dependency injection support for pipeline components.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Extensions.DependencyInjection
```

## Features

- **Automatic Node Discovery**: Scan assemblies and automatically register pipeline components
- **Service Lifetime Management**: Control the lifetime of nodes and handlers (Transient, Scoped, Singleton)
- **Dependency Injection Support**: Inject dependencies into nodes, error handlers, and other components
- **Assembly Scanning**: Automatically discover and register pipeline components from specified assemblies
- **Fluent Configuration API**: Intuitive builder pattern for configuring NPipeline services
- **Pipeline Execution from Service Provider**: Execute pipelines directly from your DI container
- **Error Handler Registration**: Register custom error handlers with DI support
- **Safe Type Loading**: Robust assembly scanning that handles reflection exceptions gracefully

## Usage

### Basic Registration

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

// Register NPipeline with basic configuration
var services = new ServiceCollection();

services.AddNPipeline(builder =>
{
    builder.AddNode<MyTransformNode>()
           .AddNode<MySourceNode>()
           .AddPipeline<MyDataProcessingPipeline>()
           .AddErrorHandler<MyCustomErrorHandler>();
});

var serviceProvider = services.BuildServiceProvider();
```

### Assembly Scanning

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

var services = new ServiceCollection();

// Scan the current assembly for pipeline components
services.AddNPipeline(Assembly.GetExecutingAssembly());

// Or scan multiple assemblies
services.AddNPipeline(
    Assembly.GetExecutingAssembly(),
    typeof(MyOtherNode).Assembly,
    typeof(ExternalPipeline).Assembly);

// Or use the builder with assembly scanning
services.AddNPipeline(builder =>
{
    builder.ScanAssemblies(
        Assembly.GetExecutingAssembly(),
        typeof(MyOtherNode).Assembly);
});
```

### Custom Service Lifetimes

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

var services = new ServiceCollection();

services.AddNPipeline(builder =>
{
    // Register nodes with specific lifetimes
    builder.AddNode<SingletonNode>(ServiceLifetime.Singleton)
           .AddNode<ScopedNode>(ServiceLifetime.Scoped)
           .AddNode<TransientNode>(ServiceLifetime.Transient);
    
    // Register pipelines with specific lifetimes
    builder.AddPipeline<MyPipeline>(ServiceLifetime.Scoped);
    
    // Register error handlers with specific lifetimes
    builder.AddErrorHandler<MyErrorHandler>(ServiceLifetime.Singleton)
           .AddPipelineErrorHandler<MyPipelineErrorHandler>(ServiceLifetime.Transient);
});
```

### Pipeline Execution from Service Provider

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

// Set up services
var services = new ServiceCollection();
services.AddNPipeline(builder =>
{
    builder.AddNode<MySourceNode>()
           .AddNode<MyTransformNode>()
           .AddNode<MySinkNode>()
           .AddPipeline<MyDataPipeline>();
});

var serviceProvider = services.BuildServiceProvider();

// Execute pipeline without parameters
await serviceProvider.RunPipelineAsync<MyDataPipeline>();

// Execute pipeline with parameters
var parameters = new Dictionary<string, object>
{
    ["BatchSize"] = 1000,
    ["ProcessingMode"] = "Fast"
};

await serviceProvider.RunPipelineAsync<MyDataPipeline>(parameters);
```

### Error Handler Registration

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

var services = new ServiceCollection();

services.AddNPipeline(builder =>
{
    // Register node-specific error handlers
    builder.AddErrorHandler<MyNodeErrorHandler>()
           .AddErrorHandler<RetryErrorHandler>(ServiceLifetime.Singleton);
    
    // Register pipeline-level error handlers
    builder.AddPipelineErrorHandler<MyPipelineErrorHandler>();
    
    // Register dead letter sinks for failed items
    builder.AddDeadLetterSink<MyDeadLetterSink>();
    
    // Register lineage sinks for tracking
    builder.AddLineageSink<MyLineageSink>()
           .AddPipelineLineageSink<MyPipelineLineageSink>();
    
    // Register lineage sink providers
    builder.AddLineageSinkProvider<MyLineageSinkProvider>();
});
```

## Configuration

### Service Registration Options

The NPipeline service builder provides several options for registering components:

```csharp
services.AddNPipeline(builder =>
{
    // Nodes
    builder.AddNode<TNode>()                    // Transient lifetime
           .AddNode<TNode>(lifetime);           // Specific lifetime
    
    // Pipeline Definitions
    builder.AddPipeline<TPipeline>()             // Transient lifetime
           .AddPipeline<TPipeline>(lifetime);   // Specific lifetime
    
    // Error Handlers
    builder.AddErrorHandler<THandler>()          // Transient lifetime
           .AddErrorHandler<THandler>(lifetime) // Specific lifetime
           .AddPipelineErrorHandler<THandler>()  // Transient lifetime
           .AddPipelineErrorHandler<THandler>(lifetime); // Specific lifetime
    
    // Sinks
    builder.AddDeadLetterSink<TSink>()           // Transient lifetime
           .AddDeadLetterSink<TSink>(lifetime)  // Specific lifetime
           .AddLineageSink<TSink>()              // Transient lifetime
           .AddLineageSink<TSink>(lifetime)     // Specific lifetime
           .AddPipelineLineageSink<TSink>()      // Transient lifetime
           .AddPipelineLineageSink<TSink>(lifetime); // Specific lifetime
    
    // Providers
    builder.AddLineageSinkProvider<TProvider>()  // Transient lifetime
           .AddLineageSinkProvider<TProvider>(lifetime); // Specific lifetime
});
```

### Assembly Scanning Configuration

Assembly scanning automatically discovers and registers these component types:

- **Nodes**: Classes implementing `INode`
- **Pipeline Definitions**: Classes implementing `IPipelineDefinition`
- **Error Handlers**: Classes implementing `INodeErrorHandler` or `IPipelineErrorHandler`
- **Sinks**: Classes implementing `IDeadLetterSink`, `ILineageSink`, or `IPipelineLineageSink`
- **Providers**: Classes implementing `IPipelineLineageSinkProvider`

```csharp
// Scan specific assemblies
services.AddNPipeline(builder =>
{
    builder.ScanAssemblies(
        Assembly.GetExecutingAssembly(),
        typeof(ExternalComponent).Assembly);
});

// Or use the direct method
services.AddNPipeline(
    Assembly.GetExecutingAssembly(),
    typeof(ExternalComponent).Assembly);
```

### Lifetime Management

Choose appropriate service lifetimes based on your requirements:

- **Transient**: New instance for every request (default)
- **Scoped**: One instance per scope (recommended for most nodes)
- **Singleton**: Single instance for the application lifetime

```csharp
services.AddNPipeline(builder =>
{
    // Use Scoped for stateful nodes that need per-request isolation
    builder.AddNode<StatefulTransformNode>(ServiceLifetime.Scoped);
    
    // Use Singleton for stateless, thread-safe nodes
    builder.AddNode<ThreadSafeValidatorNode>(ServiceLifetime.Singleton);
    
    // Use Transient for lightweight nodes
    builder.AddNode<SimpleTransformNode>(); // Uses default Transient
});
```

## Advanced Usage

### Custom Node with Dependencies

```csharp
public class MyTransformNode : ITransformNode<Input, Output>
{
    private readonly ILogger<MyTransformNode> _logger;
    private readonly IValidationService _validator;
    
    public MyTransformNode(ILogger<MyTransformNode> logger, IValidationService validator)
    {
        _logger = logger;
        _validator = validator;
    }
    
    public async Task<Output> TransformAsync(Input input, PipelineContext context)
    {
        _logger.LogInformation("Processing item: {ItemId}", input.Id);
        
        if (!_validator.Validate(input))
            throw new ValidationException("Invalid input");
            
        return new Output { ProcessedData = input.Data.ToUpper() };
    }
}

// Register with DI
var services = new ServiceCollection();
services.AddLogging();
services.AddSingleton<IValidationService, ValidationService>();

services.AddNPipeline(builder =>
{
    builder.AddNode<MyTransformNode>(ServiceLifetime.Scoped);
});
```

### Pipeline with Configuration

```csharp
public class ConfigurablePipeline : IPipelineDefinition
{
    private readonly IConfiguration _configuration;
    
    public ConfigurablePipeline(IConfiguration configuration)
    {
        _configuration = configuration;
    }
    
    public void Define(PipelineBuilder builder)
    {
        var batchSize = _configuration.GetValue<int>("Pipeline:BatchSize", 100);
        
        builder.Source<DataSourceNode>()
               .Transform<DataTransformNode>()
               .Batch(batchSize)
               .Sink<DataSinkNode>();
    }
}

// Register with configuration
var services = new ServiceCollection();
services.AddSingleton<IConfiguration>(configuration);

services.AddNPipeline(builder =>
{
    builder.AddPipeline<ConfigurablePipeline>(ServiceLifetime.Scoped);
});
```

### Error Handling with DI

```csharp
public class DatabaseErrorHandler : INodeErrorHandler
{
    private readonly ILogger<DatabaseErrorHandler> _logger;
    private readonly IErrorRepository _errorRepository;
    
    public DatabaseErrorHandler(ILogger<DatabaseErrorHandler> logger, IErrorRepository errorRepository)
    {
        _logger = logger;
        _errorRepository = errorRepository;
    }
    
    public async Task<ErrorHandlingResult> HandleAsync(ErrorContext context, CancellationToken cancellationToken)
    {
        _logger.LogError(context.Exception, "Error processing item");
        
        await _errorRepository.LogErrorAsync(context, cancellationToken);
        
        return ErrorHandlingResult.Retry;
    }
}

// Register with DI
services.AddNPipeline(builder =>
{
    builder.AddErrorHandler<DatabaseErrorHandler>(ServiceLifetime.Scoped);
});
```

## Requirements

- **.NET 8.0** or later
- **Microsoft.Extensions.DependencyInjection.Abstractions** 10.0.0 or later
- **NPipeline** core package

## License

MIT License - see LICENSE file for details.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Extensions](https://www.nuget.org/packages/NPipeline.Extensions)** - Additional pipeline components
- **[NPipeline.Extensions.Parallelism](https://www.nuget.org/packages/NPipeline.Extensions.Parallelism)** - Parallel processing extensions
- **[NPipeline.Extensions.Testing](https://www.nuget.org/packages/NPipeline.Extensions.Testing)** - Testing utilities

## Support

- **Documentation**: [https://npipeline.readthedocs.io](https://npipeline.readthedocs.io)
- **Issues**: [GitHub Issues](https://github.com/npipeline/NPipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/npipeline/NPipeline/discussions)
- **Discord**: [NPipeline Community](https://discord.gg/npipeline)
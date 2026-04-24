---
title: Frequently Asked Questions
description: Answers to common questions about NPipeline.
order: 1
---

# Frequently Asked Questions

## General

### What is NPipeline?

NPipeline is a robust, composable data orchestration library for .NET. It enables you to build data streamlines that process data through connected nodes (sources, transforms, sinks) with features like parallelism, observability, error handling, and extensibility.

### Is NPipeline free and open source?

Yes, NPipeline is distributed under the [MIT License](https://github.com/Stewie435/NPipeline/blob/main/LICENSE). You're free to use it in commercial and personal projects.

### What version of .NET is supported?

NPipeline targets **[NET8.0](https://dotnet.microsoft.com/en-us/download/dotnet/8.0)** and later. We recommend using the latest LTS release for production applications.

### How does NPipeline compare to other ETL tools?

| Feature | NPipeline | Apache Airflow | Azure Data Factory |
|---------|-----------|-----------------|-------------------|
| **Programming** | C# code-first | Python DAGs | Visual designer |
| **Learning curve** | Shallow (.NET developers) | Moderate | High |
| **Infrastructure** | In-process (lightweight) | Distributed (complex) | Managed cloud service |
| **Cost** | Free | Free (self-hosted) or cloud | Per-operation pricing |
| **Type safety** | ✅ Strong typing | ❌ Dynamic | ⚠️ Limited |
| **Testing** | ✅ Unit test simple | ⚠️ Requires testing framework | ⚠️ Complex |

**Choose NPipeline if you:**

- Build .NET applications
- Need lightweight, in-process pipelines
- Want strong type safety
- Prefer code-based configuration

### Can I use NPipeline in Azure Functions, AWS Lambda, or Kubernetes?

**Azure Functions:** Yes, but consider startup time and memory constraints.

- ✅ Works well for moderate data volumes
- ⚠️ Cold starts may take time if pipelines are large
- 💡 Pre-warm functions for production use

**AWS Lambda:** Possible but with caveats:

- ✅ Suitable for small, fast pipelines
- ❌ Not ideal for long-running operations (15-minute timeout)
- 💡 Consider Step Functions for orchestration

**Kubernetes:** Excellent choice!

- ✅ Deploy as containerized services
- ✅ Horizontal scaling with multiple instances
- ✅ Native integration with Kubernetes observability

Example Kubernetes deployment:

```csharp
// In your console app with Worker pattern
var host = Host.CreateDefaultBuilder()
    .ConfigureServices((context, services) => {
        services.AddNPipelineWorker(); // Background service
        services.AddNPipeline(Assembly.GetExecutingAssembly());
    })
    .Build();

await host.RunAsync();
```

## Getting Started

### What's the quickest way to try NPipeline?

Follow the [Quick Start guide](../getting-started/quick-start.md) - you'll have a working pipeline in minutes:

```csharp
// 1. Define a simple source, transform, sink
// 2. Register with DI
// 3. Run the pipeline

services.AddNPipeline(Assembly.GetExecutingAssembly());
var runner = serviceProvider.GetRequiredService<IPipelineRunner>();
await runner.RunAsync<MyPipeline>(context);
```

### Do I need to know about async/await to use NPipeline?

Yes, NPipeline is async throughout. If you're new to async programming in .NET:

1. **Recommended reading:** [Async/Await Best Practices](https://docs.microsoft.com/en-us/archive/msdn-magazine/2013/march/async-await-best-practices-in-asynchronous-programming)
2. **Key concepts:**
   - `async` marks a method as asynchronous
   - `await` yields control while waiting for completion
   - `Task<T>` represents async work

3. **In NPipeline:**

   ```csharp
   // Standard pattern
   public override async Task<Item> TransformAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
   {
       var result = await SomeAsync(item, cancellationToken);
       return result;
   }
   ```

### What if I only need to process data once?

Use the console app approach:

```csharp
var services = new ServiceCollection();
services.AddNPipeline(Assembly.GetExecutingAssembly());
var runner = services.BuildServiceProvider().GetRequiredService<IPipelineRunner>();

await runner.RunAsync<MyPipeline>(new PipelineContext());
// Process completes and exits
```

No background service or long-lived host needed.

## Pipeline Design

### How many nodes should a pipeline have?

There's no hard limit, but consider:

- **Sweet spot:** 3-10 nodes for most pipelines
- **Complex pipelines:** 10-50 nodes are manageable with good organization
- **Too many (50+):** Break into multiple pipelines or consider a more complex orchestrator

**Example with acceptable complexity:**

```
Source (1)
  ↓
Validate (2)
  ↓
Enrich (3)
  ├→ Transform A (4)
  ├→ Transform B (5)
  ├→ Transform C (6)
  ↓
Aggregate (7)
  ↓
Sink (8)
```

This is readable and maintains a clear data flow.

### Can I have multiple sources/sinks in one pipeline?

Yes! Use multiple source/sink nodes:

```csharp
public class MultiSourcePipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Multiple sources
        var source1 = builder.AddSource<SourceA, DataA>();
        var source2 = builder.AddSource<SourceB, DataB>();

        // Merge data
        var merge = builder.AddTransform<MergeTransform, DataA, DataC>();
        var merge2 = builder.AddTransform<MergeTransform2, DataB, DataC>();

        // Single sink
        var sink = builder.AddSink<UnifiedSink, DataC>();

        builder.Connect(source1, merge);
        builder.Connect(source2, merge2);
        builder.Connect(merge, sink);
        builder.Connect(merge2, sink);
    }
}
```

### Should transforms be stateless or can they have state?

**Stateless is preferred** but state is allowed:

- **Stateless (recommended):**
  - Easy to test
  - Thread-safe
  - Horizontally scalable
  
  ```csharp
  public class LowercaseTransform : TransformNode<string, string>
  {
      public override Task<string> TransformAsync(string item, PipelineContext context, CancellationToken cancellationToken)
      {
          return Task.FromResult(item.ToLower());
      }
  }
  ```

- **Stateful (for specific use cases):**
  - Running totals
  - Caching
  - Session tracking
  
  ```csharp
  public class RunningTotalTransform : TransformNode<int, int>
  {
      private int _total = 0;

      public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
      {
          _total += item;
          return Task.FromResult(_total);
      }
  }
  ```

> **Warning:** Stateful transforms are NOT thread-safe by default. Use locks if enabling parallelism, or use `context.Items` for thread-safe shared state.

### How do I handle branching or conditional logic?

Create conditional transform nodes:

```csharp
public class SplitByTypeTransform : TransformNode<Item, Item>
{
    public override async Task<Item> TransformAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
    {
        item.Category = item.Value > 100 ? "High" : "Low";
        return item;
    }
}

// Then filter in downstream transforms:
public class HighValueTransform : TransformNode<Item, Item>
{
    public override async Task<Item> TransformAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
    {
        if (item.Category != "High")
            throw new FilterException(); // Skip non-high items

        return await ProcessHighValueItem(item, cancellationToken);
    }
}
```

## Performance & Scaling

### How do I make my pipeline faster?

1. **Profile first** - Measure where time is spent:

   ```csharp
   var sw = Stopwatch.StartNew();
   var result = await operation();
   if (sw.ElapsedMilliseconds > Threshold)
       logger.LogWarning($"Slow: {sw.ElapsedMilliseconds}ms");
   ```

2. **Enable parallelism** (if CPU-bound):

   ```csharp
   services.AddNPipelineParallelism();
   ```

3. **Optimize I/O** (if I/O-bound):
   - Use async APIs (`GetAsync` not `Get`)
   - Connection pooling
   - Batch requests

4. **Reduce memory allocations**:
   - Use `ArrayPool<T>` for reusable buffers
   - Avoid LINQ when possible (for hot paths)
   - Consider object pooling for high-frequency objects

5. **Stream data** (not all-at-once):

   ```csharp
   // BAD - All in memory
   var data = database.GetAll().ToList();

   // GOOD - Streamed
   await foreach (var item in database.GetAllAsync())
       yield return item;
   ```

### How many pipelines can run concurrently?

It depends on:

- **Machine resources** - CPU cores, RAM, network
- **Node complexity** - Simple transforms vs. database queries
- **Data volume** - Small datasets vs. millions of records

**Practical limits:**

- **Lightweight pipelines (in-process):** Dozens to hundreds concurrently
- **I/O-bound pipelines:** Hundreds to thousands (async scaling)
- **CPU-bound pipelines:** Limited to core count (disable parallelism, use process isolation)

### How do I scale horizontally?

Deploy multiple instances:

```yaml
# Kubernetes deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: npipeline-worker
spec:
  replicas: 3 # Multiple instances
  template:
    spec:
      containers:
      - name: npipeline
        image: my-npipeline:latest
        resources:
          requests:
            cpu: 500m
            memory: 256Mi
          limits:
            cpu: 1
            memory: 512Mi
```

NPipeline handles distribution through message brokers (e.g., RabbitMQ, Kafka).

## Integration

### Can I integrate NPipeline with my existing architecture?

Yes! NPipeline integrates with:

- **Dependency Injection (DI):** Microsoft.Extensions.DependencyInjection
- **Logging:** Microsoft.Extensions.Logging (ILogger)
- **Configuration:** Microsoft.Extensions.Configuration
- **Hosting:** Microsoft.Extensions.Hosting (Worker pattern)

Example integration:

```csharp
var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.Configure<MySettings>(context.Configuration.GetSection("Pipeline"));
        services.AddNPipeline(Assembly.GetExecutingAssembly());
        services.AddHostedService<PipelineWorker>();
    })
    .ConfigureLogging(builder =>
    {
        builder.AddApplicationInsights(); // Or your logging provider
    })
    .Build();

await host.RunAsync();
```

### Can I call NPipeline from a web API?

Yes! Example with ASP.NET Core:

```csharp
[ApiController]
[Route("api/[controller]")]
public class PipelinesController : ControllerBase
{
    private readonly IPipelineRunner _runner;

    public PipelinesController(IPipelineRunner runner)
    {
        _runner = runner;
    }

    [HttpPost("{pipelineType}")]
    public async Task<ActionResult> RunPipeline(string pipelineType, [FromBody] object input)
    {
        var context = new PipelineContext { Items = { { "input", input } } };

        try
        {
            await _runner.RunAsync(Type.GetType(pipelineType), context);
            return Ok(new { Status = "Success" });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { Error = ex.Message });
        }
    }
}
```

### How do I connect to databases?

NPipeline works with any .NET database driver:

```csharp
public class DatabaseSourceNode : SourceNode<CustomerRecord>
{
    private readonly string _connString;

    public DatabaseSourceNode(IConfiguration config)
    {
        _connString = config.GetConnectionString("DefaultConnection");
    }

    public override IDataStream<CustomerRecord> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        async IAsyncEnumerable<CustomerRecord> ReadAsync()
        {
            using var conn = new SqlConnection(_connString);
            await conn.OpenAsync(cancellationToken);

            using var cmd = new SqlCommand("SELECT * FROM Customers", conn);
            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                yield return new CustomerRecord
                {
                    Id = (int)reader["Id"],
                    Name = (string)reader["Name"]
                };
            }
        }

        return new DataStream<CustomerRecord>(ReadAsync());
    }
}
```

Or use an ORM:

```csharp
public class EntityFrameworkSourceNode : SourceNode<Customer>
{
    private readonly MyDbContext _dbContext;

    public EntityFrameworkSourceNode(MyDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    public override IDataStream<Customer> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        return new DataStream<Customer>(_dbContext.Customers.AsAsyncEnumerable());
    }
}
```

## Testing

### How do I unit test a pipeline node?

Create a test that isolates the node:

```csharp
[Fact]
public async Task TransformNode_DoubleValue_ReturnsTwice()
{
    // Arrange
    var node = new MultiplyTransform(2);
    var input = new Item { Value = 5 };
    var context = new PipelineContext();

    // Act
    var result = await node.TransformAsync(input, context, CancellationToken.None);

    // Assert
    Assert.Equal(10, result.Value);
}
```

### How do I integration test an entire pipeline?

Use in-memory sources and sinks:

```csharp
[Fact]
public async Task Pipeline_ProcessesData_SuccessfullyCompletes()
{
    // Arrange
    var services = new ServiceCollection();
    services.AddNPipeline(Assembly.GetExecutingAssembly());
    var runner = services.BuildServiceProvider().GetRequiredService<IPipelineRunner>();

    // Act
    var context = new PipelineContext();
    await runner.RunAsync<MyTestPipeline>(context);

    // Assert
    var results = context.Items["results"] as List<Item>;
    Assert.NotEmpty(results);
}
```

### How do I mock external dependencies?

Use Moq or similar:

```csharp
[Fact]
public async Task Transform_CallsExternalService()
{
    // Arrange
    var mockService = new Mock<IExternalService>();
    mockService
        .Setup(x => x.EnrichAsync(It.IsAny<Item>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(new Item { Data = "enriched" });

    var services = new ServiceCollection();
    services.AddSingleton(mockService.Object);
    services.AddNPipeline(Assembly.GetExecutingAssembly());

    var runner = services.BuildServiceProvider().GetRequiredService<IPipelineRunner>();

    // Act
    await runner.RunAsync<MyPipeline>(new PipelineContext());

    // Assert
    mockService.Verify(x => x.EnrichAsync(It.IsAny<Item>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
}
```

## Troubleshooting

### My pipeline isn't processing data

**Check:**

1. Is your source yielding data? Add logging.
2. Are nodes connected? Review pipeline definition.
3. Is your sink registered? Common oversight.

See [Troubleshooting Guide](../troubleshooting/index.md) for detailed diagnosis.

### Performance is terrible

**Steps:**

1. Profile to find the bottleneck
2. Check for blocking I/O (use async)
3. Enable parallelism if CPU-bound
4. Stream data instead of loading all at once

See [Performance Issues](../troubleshooting/index.md#performance-issues).

### I keep getting timeout errors

**Solutions:**

1. Increase timeout in your configuration
2. Optimize the slow operation (profiling)
3. Use `CancellationToken` to allow graceful cancellation

### Error codes reference

For errors like `[NP0301]`, `[NP0401]`, etc., see [Error Codes Reference](../error-codes/index.md).

## Advanced Questions

### Can I extend NPipeline?

Yes! Create custom extensions by implementing node interfaces:

```csharp
public class CustomNode : TransformNode<InputType, OutputType>
{
    public override Task<OutputType> TransformAsync(InputType item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Custom logic
        return Task.FromResult(Transform(item));
    }
}
```

See [Extensions Documentation](../extensions/index.md) for more.

### How do I add observability?

NPipeline supports structured logging and distributed tracing:

```csharp
services.AddLogging(builder =>
{
    builder
        .AddConsole()
        .AddApplicationInsights() // Or your telemetry provider
        .SetMinimumLevel(LogLevel.Information);
});

// In your nodes:
private readonly ILogger<MyNode> _logger;

public override Task<Item> TransformAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    _logger.LogInformation("Processing item {@Item}", item);
    var result = Process(item);
    _logger.LogInformation("Processed item, output: {@Output}", result);
    return Task.FromResult(result);
}
```

### Can I run pipelines on a schedule?

Yes, use a scheduler like:

**Quartz.NET:**

```csharp
services.AddQuartz(q =>
{
    q.AddJob<PipelineJob>(opts => opts.WithIdentity("pipeline-job"));
    q.AddTrigger(opts => opts
        .ForJob("pipeline-job")
        .WithIdentity("pipeline-trigger")
        .WithCronSchedule("0 0 * * *")); // Daily at midnight
});

services.AddQuartzHostedService();
```

**Background Service with Timer:**

```csharp
public class ScheduledPipelineWorker : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromHours(1));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            await _runner.RunAsync<MyPipeline>(new PipelineContext());
        }
    }
}
```

## Can't find your answer?

- Check the [Troubleshooting Guide](../troubleshooting/index.md)
- Review [Error Codes Reference](../error-codes/index.md)
- Explore examples in [Samples](../samples/index.md)
- Read the [Architecture Guide](../architecture/index.md)

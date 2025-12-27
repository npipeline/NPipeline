---
title: Dependency Injection Integration
description: How NPipeline integrates with dependency injection containers.
sidebar_position: 5
---

# Dependency Injection Integration

NPipeline integrates seamlessly with dependency injection (DI) containers, allowing you to use constructor injection for node dependencies.

## Automatic Node Resolution

When you add a node to a pipeline, NPipeline automatically:

1. Creates an instance of the node
2. Resolves all constructor dependencies from the DI container
3. Injects them into the node

Pre-configure node instances for advanced scenarios:

```csharp
// Pre-configure a specific node instance
var customTransform = new MyTransform(specialConfig);
var builder = new PipelineBuilder();
builder.AddTransformNode(customTransform); // Use the pre-configured instance
```

This is useful when:
- Node setup is complex and can't be done through DI alone
- You need to share state between multiple nodes
- You're migrating legacy code

**Example:**

```csharp
public class ProcessOrderTransform : ITransformNode<Order, ProcessedOrder>
{
    private readonly IPaymentService _paymentService;
    private readonly INotificationService _notificationService;

    // Constructor dependencies are automatically injected!
    public ProcessOrderTransform(
        IPaymentService paymentService,
        INotificationService notificationService)
    {
        _paymentService = paymentService;
        _notificationService = notificationService;
    }

    public async Task<ProcessedOrder> ExecuteAsync(
        Order input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var paymentResult = await _paymentService.ChargeAsync(input.Amount, cancellationToken);
        await _notificationService.SendAsync($"Payment: {paymentResult.Status}", cancellationToken);
        return new ProcessedOrder { /* ... */ };
    }
}
```

## DI Container Integration

**Setting Up with Microsoft.Extensions.DependencyInjection:**

```csharp
var services = new ServiceCollection();

// Register your dependencies
services.AddScoped<IPaymentService, PaymentService>();
services.AddScoped<INotificationService, NotificationService>();
services.AddScoped<IOrderRepository, OrderRepository>();

var serviceProvider = services.BuildServiceProvider();

// Pipeline builder uses the service provider
var pipeline = PipelineBuilder.WithServiceProvider(serviceProvider)
    .AddSourceNode<OrderSourceNode>()
    .AddTransformNode<ProcessOrderTransform>()
    .AddTransformNode<ValidateOrderTransform>()
    .AddSinkNode<OrderSinkNode>()
    .BuildPipeline();

var context = PipelineContext.Default;
var result = await runner.ExecuteAsync(pipeline, context);
```

## Scoped Dependencies

Each pipeline execution can have its own scope for scoped dependencies:

```csharp
using (var scope = serviceProvider.CreateScope())
{
    var scopedServices = scope.ServiceProvider;
    
    var pipeline = PipelineBuilder.WithServiceProvider(scopedServices)
        .AddSourceNode<OrderSourceNode>()
        .AddTransformNode<ProcessOrderTransform>()
        .AddSinkNode<OrderSinkNode>()
        .BuildPipeline();

    await runner.ExecuteAsync(pipeline, context);
    // Scoped services are disposed here
}
```

## Advanced Pattern: Composite Services

Use DI to compose complex behavior:

```csharp
public class EnrichedOrderTransform : ITransformNode<Order, EnrichedOrder>
{
    private readonly IOrderEnricher _enricher;
    private readonly ICacheService _cache;

    public EnrichedOrderTransform(
        IOrderEnricher enricher,
        ICacheService cache)
    {
        _enricher = enricher;
        _cache = cache;
    }

    public async Task<EnrichedOrder> ExecuteAsync(
        Order input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var cached = await _cache.GetAsync(input.Id, cancellationToken);
        if (cached != null)
            return cached;

        var enriched = await _enricher.EnrichAsync(input, cancellationToken);
        await _cache.SetAsync(input.Id, enriched, cancellationToken);
        return enriched;
    }
}
```

## Benefits

**Testability:**

```csharp
// Test with mock dependencies
var mockPaymentService = new Mock<IPaymentService>();
var mockNotificationService = new Mock<INotificationService>();

var transform = new ProcessOrderTransform(
    mockPaymentService.Object,
    mockNotificationService.Object);

// Test the transform in isolation
```

**Flexibility:**

```csharp
// Swap implementations based on environment
if (isDevelopment)
{
    services.AddSingleton<IPaymentService, MockPaymentService>();
}
else
{
    services.AddSingleton<IPaymentService, StripePaymentService>();
}
```

## Next Steps

- **[Node Instantiation](node-instantiation.md)** - Understand node creation patterns and performance optimization
- **[Error Handling Architecture](error-handling-architecture.md)** - Learn how DI works with error handlers
- **[Extension Points](extension-points.md)** - Create custom nodes with DI support


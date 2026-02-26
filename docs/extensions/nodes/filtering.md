---
title: Filtering Nodes
description: Filter items based on predicates and conditions.
sidebar_position: 3
---

# Filtering Nodes

Filtering nodes selectively pass through or reject items based on predicates. When an item fails a filter, a `FilteringException` is thrown with details about why the item was filtered.

## Basic Filtering

Filter items using a predicate:

```csharp
// Single filter
builder.AddFilteringNode<Order>(x => x.Status == OrderStatus.Pending);

// Multiple filters
builder.AddFilteringNode<Product>()
    .Where(x => x.Price > 0)
    .Where(x => x.IsActive);
```

## Constructor with Predicate

Specify the filter predicate in the constructor:

```csharp
var activeOrders = builder.AddFilteringNode<Order>(x => x.Status == OrderStatus.Active);

var expensiveProducts = builder.AddFilteringNode<Product>(
    x => x.Price > 100,
    reason: "Price must exceed $100");
```

## Fluent Where Syntax

Chain multiple filter conditions:

```csharp
builder.AddFilteringNode<Order>()
    .Where(x => x.Amount > 100, "Order amount must exceed $100")
    .Where(x => x.Status != OrderStatus.Cancelled, "Order must not be cancelled")
    .Where(x => x.CreatedDate >= DateTime.Today, "Order must be from today");
```

## Fixed Message Where

Use a string instead of a factory for simple messages:

```csharp
// When you have a fixed message, this is cleaner than a factory
builder.AddFilteringNode<Order>()
    .Where(x => x.Amount > 100, "Order amount must exceed $100")
    .Where(x => x.Status != OrderStatus.Cancelled, "Order must not be cancelled");
```

## Complex Predicates

Use complex conditional logic:

```csharp
// Complex condition
builder.AddFilteringNode<Person>(x => 
    (x.Age >= 18 && x.Status == "Active") || 
    x.IsVip == true);

// With custom message
builder.AddFilteringNode<Order>(
    x => x.Amount > 1000 && x.Customer.CreditScore > 700,
    reason: "Order amount must exceed $1000 and customer credit score must be above 700");

// Multiple conditions
builder.AddFilteringNode<Document>()
    .Where(x => x.IsPublished, "Document must be published")
    .Where(x => x.Status == "Approved", "Document must be approved")
    .Where(x => !x.IsArchived, "Document must not be archived");
```

## Filtering with Collections

Filter based on collection properties:

```csharp
// Item exists in collection
builder.AddFilteringNode<Order>(x => x.Items.Count > 0, "Order must contain at least one item");

// Collection contains specific value
builder.AddFilteringNode<Category>(x => x.Tags.Contains("Featured"), "Category must be tagged as Featured");

// All items satisfy condition
builder.AddFilteringNode<Order>(x => x.Items.All(i => i.Quantity > 0), "All items must have positive quantity");

// Any item satisfies condition
builder.AddFilteringNode<Product>(x => x.Variants.Any(v => v.IsAvailable), "Product must have at least one available variant");
```

## Custom Error Messages

Provide descriptive messages when items are filtered:

```csharp
// Constructor
var filter = builder.AddFilteringNode<Order>(
    x => x.Amount > 0,
    reason: "Order amount must be greater than zero");

// With Where method
builder.AddFilteringNode<Product>()
    .Where(x => x.Price > 0, "Price must be positive")
    .Where(x => x.StockLevel > 0, "Stock level must be positive");
```

## Default Messages

If no custom message is provided, a default message is used:

```csharp
// Default: "Item did not meet filter criteria"
builder.AddFilteringNode<Order>(x => x.Amount > 0);
```

## Error Handling

Filtered items raise `FilteringException`:

```csharp
try
{
    await pipeline.ExecuteAsync();
}
catch (FilteringException ex)
{
    Console.WriteLine($"Reason: {ex.Reason}");
    Console.WriteLine($"Message: {ex.Message}");
}
```

## Node Error Decisions

Control what happens when an item is filtered:

```csharp
// Skip filtered items (default - no exception)
builder.WithErrorDecision(filterHandle, NodeErrorDecision.Skip);

// Throw exception on first filtered item
builder.WithErrorDecision(filterHandle, NodeErrorDecision.Fail);

// Retry with different criteria
builder.WithErrorDecision(filterHandle, NodeErrorDecision.Retry);
```

## Filtering Pipeline Example

```csharp
var builder = new PipelineBuilder();

// Define source
var source = builder.AddInMemorySource<Order>();

// Filter 1: Only pending orders
var filterPending = builder.AddFilteringNode<Order>(
    x => x.Status == OrderStatus.Pending,
    reason: "Order must be pending");

// Filter 2: Amount constraints
var filterAmount = builder.AddFilteringNode<Order>()
    .Where(x => x.Amount > 50, "Order amount must exceed $50")
    .Where(x => x.Amount <= 10000, "Order amount cannot exceed $10,000");

// Filter 3: Customer validation
var filterCustomer = builder.AddFilteringNode<Order>(
    x => x.Customer != null && !x.Customer.IsBlocked,
    reason: "Customer must exist and not be blocked");

// Define sink
var sink = builder.AddInMemorySink<Order>();

// Connect filters in sequence
builder.Connect(source, filterPending);
builder.Connect(filterPending, filterAmount);
builder.Connect(filterAmount, filterCustomer);
builder.Connect(filterCustomer, sink);

// Build and execute
var pipeline = builder.Build();
var result = await pipeline.ExecuteAsync();
```

## Performance Characteristics

- **Predicate Evaluation**: Performed inline on each item (no compilation)
- **Exception Creation**: `FilteringException` is created only when item fails
- **Memory**: No additional allocations for filter state
- **Thread Safety**: Stateless and thread-safe for parallel execution

## Common Patterns

### Pre-filter Before Expensive Operations

```csharp
var builder = new PipelineBuilder();

// Fast filter first
var quickFilter = builder.AddFilteringNode<Product>(x => x.Price > 0);

// Expensive operation after filtering
var enrichment = builder.AddEnrichment<Product>(x => x.Details = FetchExpensiveDetails());

builder.Connect(quickFilter, enrichment);
```

### Multi-stage Filtering

```csharp
// Filter stage 1
var stage1 = builder.AddFilteringNode<Item>(x => x.Status == "Active");

// Filter stage 2
var stage2 = builder.AddFilteringNode<Item>(x => x.Score >= 5)
    .Where(x => x.IsApproved);

// Filter stage 3
var stage3 = builder.AddFilteringNode<Item>(x => !x.IsExpired);

builder.Connect(stage1, stage2);
builder.Connect(stage2, stage3);
```

### Conditional Filtering

```csharp
// Only filter if condition met
bool applyStrictFiltering = context.GetSetting("StrictMode");

if (applyStrictFiltering)
{
    builder.AddFilteringNode<Product>(x => x.Quality >= 8);
}
else
{
    builder.AddFilteringNode<Product>(x => x.Quality >= 5);
}
```

## Combining with Other Nodes

```csharp
var builder = new PipelineBuilder();

// Cleanse first
var cleanse = builder.AddStringCleansing<Product>()
    .Trim(x => x.Name)
    .ToLower(x => x.Name);

// Validate
var validate = builder.AddStringValidation<Product>()
    .HasMinLength(x => x.Name, 3);

// Filter
var filter = builder.AddFilteringNode<Product>(x => x.IsActive);

// Connect in order
builder.Connect(cleanse, validate);
builder.Connect(validate, filter);
```

## Testing Filtered Pipelines

```csharp
[Fact]
public async Task FilterNode_ShouldFilterInactiveItems()
{
    // Arrange
    var items = new[] { 
        new Order { Id = 1, IsActive = true },
        new Order { Id = 2, IsActive = false },
        new Order { Id = 3, IsActive = true }
    };

    // Create filter node
    var filter = new FilteringNode<Order>(x => x.IsActive);
    
    // Act & Assert - filter node throws FilteringException for filtered items
    var activeItems = new List<Order>();
    foreach (var item in items)
    {
        try
        {
            var result = await filter.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
            activeItems.Add(result);
        }
        catch (FilteringException)
        {
            // Item was filtered out
        }
    }

    // Assert
    activeItems.Should().HaveCount(2);
    activeItems.All(x => x.IsActive).Should().BeTrue();
}
```

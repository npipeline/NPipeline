# NPipeline.Extensions.Nodes

A high-performance, dependency-free extension library for [NPipeline](https://github.com/NPipeline/NPipeline) that provides ready-made, granular nodes for
common data processing tasks.

## Overview

NPipeline.Extensions.Nodes provides composable, single-responsibility nodes that users can combine to build complex data processing pipelines. The library
focuses on:

- **Granularity**: Each node performs one well-defined operation
- **Performance**: Zero-allocation hot paths with compiled expressions and ValueTask support
- **Type Safety**: Strongly-typed APIs using expression-based property selection
- **Developer Experience**: Fluent API with method chaining and clear error messages
- **Dependency-Free**: No external dependencies beyond NPipeline core

## Installation

```bash
dotnet add package NPipeline.Extensions.Nodes
```

### Requirements

- .NET 8.0, 9.0, or 10.0
- NPipeline 0.1.0 or later

## Quick Start

### Using Builder Extension Methods (Recommended)

The simplest way to use nodes is through the fluent builder extension API:

```csharp
using NPipeline;
using NPipeline.Extensions.Nodes;

var builder = new PipelineBuilder();

// String cleansing with configuration
builder.AddStringCleansing<Customer>(n => n
    .Trim(x => x.Name)
    .ToLowerCase(x => x.Email));

// Numeric validation with automatic error handling
builder.AddNumericValidation<Order>(n => n
    .IsPositive(x => x.Quantity)
    .IsInRange(x => x.Discount, 0, 100));

// Collection cleansing
builder.AddCollectionCleansing<User>(n => n
    .RemoveNulls(x => x.Tags)
    .RemoveDuplicates(x => x.Roles));

// Filtering with predicates
builder.AddFilteringNode<Customer>(n => n
    .Where(x => x.Age >= 18)
    .Where(x => !string.IsNullOrEmpty(x.Email)));

// Type conversion with custom converter
builder.AddTypeConversion<string, int>(s => int.Parse(s));

// Enrichment with default values
builder.AddEnrichment<Order>(n => n
    .DefaultIfNull(x => x.OrderDate, DateTime.UtcNow)
    .Compute(x => x.Total, order => order.Quantity * order.UnitPrice));

var pipeline = builder.Build();
```

### Creating Custom Validation Nodes

For domain-specific validation, extend `ValidationNode<T>`:

```csharp
using NPipeline.Extensions.Nodes;

public sealed class CustomerValidator : ValidationNode<Customer>
{
    public CustomerValidator()
    {
        Register(c => c.Email, IsValidEmail, "ValidEmail");
        Register(c => c.Age, age => age >= 18 && age <= 120, "ValidAge");
        Register(c => c.Name, name => !string.IsNullOrWhiteSpace(name), "NotEmpty");
    }

    private static bool IsValidEmail(string email)
        => !string.IsNullOrWhiteSpace(email) && email.Contains('@');
}

// Use with builder
builder.AddValidationNode<Customer, CustomerValidator>("customer-validation");
```

### Creating Custom Transformation Nodes

For domain-specific transformations, extend `PropertyTransformationNode<T>`:

```csharp
using NPipeline.Extensions.Nodes;

public sealed class CustomerNormalizer : PropertyTransformationNode<Customer>
{
    public CustomerNormalizer()
    {
        Register(c => c.Name, name => name?.Trim().ToUpperInvariant());
        Register(c => c.Email, email => email?.Trim().ToLowerInvariant());
        Register(c => c.Phone, phone => phone?.Replace("-", "").Replace(" ", ""));
    }
}

// Use with builder
builder.AddTransform(new CustomerNormalizer(), "normalize-customer");
```

## Architecture Highlights

### Zero-Allocation Hot Paths

All nodes override `ExecuteValueTaskAsync` to return `ValueTask<T>` directly, avoiding `Task<T>` allocations on successful synchronous execution:

```csharp
protected internal override ValueTask<T> ExecuteValueTaskAsync(T item, PipelineContext context, CancellationToken cancellationToken)
{
    // Process item
    return new ValueTask<T>(item);  // No Task allocation!
}
```

### Compiled Property Access

Property selectors are compiled once at configuration time, with no reflection in the execution path:

```csharp
// This expression:
Register(c => c.Email, email => email?.Length > 0, "NotEmpty");

// Results in pre-compiled delegates that execute in O(1) time
var accessor = PropertyAccessor.Create(selector);
```

### Thread Safety

All nodes are stateless and thread-safe by design:

- Predicates and transformations are pure functions
- No shared mutable state
- Safe for concurrent execution across multiple threads

## Contributing

Contributions are welcome! Please ensure:

- Unit tests for all new code
- XML documentation for public APIs
- Follow existing code style and patterns
- No external dependencies added without discussion

## License

MIT License - see LICENSE file for details

## Support

- **Issues**: Report bugs at [GitHub Issues](https://github.com/NPipeline/NPipeline/issues)
- **Documentation**: Full docs at [NPipeline Docs](https://npipeline.dev)
- **Discussions**: Questions at [GitHub Discussions](https://github.com/NPipeline/NPipeline/discussions)

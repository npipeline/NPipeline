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

## Core Architecture

### Phase 1: Foundation (Current Release)

The foundation includes core infrastructure for building extension nodes:

#### Base Classes

- **`PropertyTransformationNode<T>`**: Base class for in-place property mutations
  - Compiled property accessors (zero reflection in hot paths)
  - Fluent API for registering transformations
  - Supports nested property access
  - Method chaining support

- **`ValidationNode<T>`**: Base class for property-level validation
  - Compiled property getters for performance
  - Rich error context (property path, rule name, value)
  - Exception-based validation integration
  - Support for custom error messages

- **`FilteringNode<T>`**: Generic filtering with predicates
  - One or more filtering predicates
  - Exception-based signalling for error handling
  - Optional custom rejection reasons
  - Zero allocations on success path

#### Utilities

- **`PropertyAccessor`**: Compiles expression-based property accessors
  - Type-safe getter and setter delegates
  - Supports nested member access (e.g., `x => x.Address.Street`)
  - Validates settability at configuration time
  - No reflection in execution path

#### Error Handling

- **`ValidationException`**: Thrown when validation fails
  - Property path, rule name, and value included
  - Integrated with error handlers

- **`FilteringException`**: Thrown when filtering rejects an item
  - Contains rejection reason
  - Integrated with error handlers

- **`TypeConversionException`**: Thrown when type conversion fails
  - Source and target types, value included
  - Integrated with error handlers

- **Default Error Handlers**:
  - `DefaultValidationErrorHandler<T>`: Configurable handling for validation failures
  - `DefaultFilteringErrorHandler<T>`: Configurable handling for filtered items
  - `DefaultTypeConversionErrorHandler<TIn, TOut>`: Configurable handling for conversion failures

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
using NPipeline.Extensions.Nodes.Core;

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
using NPipeline.Extensions.Nodes.Core;

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

## Current Features (Phase 1 Complete)

### ✅ Cleansing Nodes
- **StringCleansingNode**: Trim, case conversion, special character removal
- **NumericCleansingNode**: Clamping, rounding, scaling, absolute values
- **DateTimeCleansingNode**: Timezone conversion, date/time normalization
- **CollectionCleansingNode**: Remove nulls, remove duplicates, filtering

### ✅ Validation Nodes
- **StringValidationNode**: Length, pattern, email, URL validation
- **NumericValidationNode**: Range, positive/negative, comparison checks
- **DateTimeValidationNode**: Past/future, range, day-of-week validation
- **CollectionValidationNode**: Count, empty/non-empty checks

### ✅ Data Processing Nodes
- **FilteringNode**: Predicate-based filtering with custom rejection reasons
- **TypeConversionNode**: Type conversions with factory methods (string↔int, string↔DateTime, etc.)
- **EnrichmentNode**: Lookup enrichment, computed properties, default values

### ✅ Infrastructure
- Compiled property accessors (zero reflection)
- Automatic error handler wiring
- Configuration-first fluent builder API
- Comprehensive XML documentation

## Planned Features

### Phase 2: Advanced Data Quality

- Anomaly detection nodes
- Data profiling nodes
- Statistical validation

### Phase 3: Specialized Transformations

- Text processing (templates, extraction, normalization)
- Temporal operations (business days, scheduling)
- Format conversion (JSON, XML, Base64, Hex)

### Phase 4: Enterprise Features

- Caching nodes with multiple strategies
- Batch processing optimization
- Distributed pipeline coordination

## Performance Characteristics

All Phase 1 nodes are designed for zero or minimal allocations:

- **PropertyTransformationNode**: O(n) in number of properties, O(1) per property access
- **ValidationNode**: O(n) in number of rules, O(1) per validation
- **FilteringNode**: O(n) in number of predicates, O(1) per predicate check

Success paths produce zero allocations (using ValueTask).

## Best Practices

1. **Single Responsibility**: Create specific node subclasses for specific domains
2. **Configuration**: Configure transformations/validations in the constructor
3. **Reuse**: Create node instances once and reuse across pipeline definitions
4. **Error Handling**: Always configure appropriate error handlers for validation/filtering nodes
5. **Testing**: Test nodes in isolation before composing in pipelines

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

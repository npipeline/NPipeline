---
title: Nodes Extension
description: Pre-built, high-performance nodes for common data processing operations. Includes cleansing, validation, filtering, and transformation nodes.
sidebar_position: 4
slug: /extensions/nodes
---

# NPipeline.Extensions.Nodes

The Nodes extension provides ready-made, production-ready nodes for common data processing operations. Each node is designed to be **fast**, **reliable**, and **easy to compose** into complex pipelines.

## Philosophy

- **Granular**: Each node does one thing well
- **Composable**: Chain multiple nodes for complex operations
- **Performant**: Zero-allocation hot paths, compiled expressions for property access
- **Type-safe**: Strongly-typed APIs with expression-based property selection
- **Dependency-free**: No external dependencies beyond NPipeline core

## Available Nodes

- ✅ String Cleansing (trim, case conversion, special character handling)
- ✅ Numeric Cleansing (rounding, clamping, scaling, absolute values)
- ✅ DateTime Cleansing (timezone conversion, truncation, rounding)
- ✅ Collection Cleansing (remove nulls/duplicates, sort, take/skip)
- ✅ String Validation (length, email, URL, GUID, regex patterns)
- ✅ Numeric Validation (range checks, positive/negative, finite)
- ✅ DateTime Validation (past/future, weekday/weekend, timezone)
- ✅ Collection Validation (count, contains, unique items)
- ✅ Enrichment (lookup, compute, default values)
- ✅ Filtering (predicates, property-based filtering)
- ✅ Type Conversion (string to numeric, datetime, enum)

## Node Categories

### Data Cleansing

Normalize and clean data properties:

- **String Cleansing**: Trim, case conversion, whitespace handling, special character removal
- **Numeric Cleansing**: Rounding, clamping, scaling, absolute values, null defaults
- **DateTime Cleansing**: Timezone conversion, truncation, kind normalization
- **Collection Cleansing**: Remove nulls/duplicates, sort, take/skip, reverse

```csharp
builder.AddNumericCleansing<Order>()
    .Clamp(x => x.Discount, 0, 100)
    .Round(x => x.Price, 2);

builder.AddDateTimeCleansing<Event>()
    .ToUtc(x => x.StartTime)
    .RoundToMinute(x => x.StartTime);
```

See [Data Cleansing documentation](cleansing.md) for details.

### Data Validation

Validate property values with clear error messages:

- **String Validation**: Length limits, email/URL/GUID formats, regex patterns
- **Numeric Validation**: Range checks, positive/negative constraints, even/odd, finite
- **DateTime Validation**: Past/future, range checks, weekday/weekend, timezone validation
- **Collection Validation**: Count limits, contains checks, unique items, subset validation

```csharp
builder.AddNumericValidation<Product>()
    .IsGreaterThan(x => x.Price, 0)
    .IsLessThan(x => x.Discount, 100);

builder.AddDateTimeValidation<Event>()
    .IsInFuture(x => x.StartDate)
    .IsAfter(x => x.EndDate, x => x.StartDate);
```

See [Data Validation documentation](validation.md) for details.

### Data Filtering

Filter items based on predicates:
- **Simple Filtering**: Filter based on property values or custom predicates
- **Complex Filtering**: Multiple filter rules with flexible composition

```csharp
builder.AddFiltering<Order>(x => x.Status == OrderStatus.Active);

builder.AddFiltering<Transaction>()
    .Where(x => x.Amount > 0)
    .Where(x => x.Date >= DateTime.Today);
```

### [Type Conversion](conversion.md)

Convert between types safely:
- **String Conversion**: Parse strings to numbers, dates, enums
- **Numeric Conversion**: Convert between int, long, float, decimal
- **Type Coercion**: Flexible type conversion with fallback defaults

```csharp
builder.AddTypeConversion<ImportRow, Data>()
    .Map(x => x.Amount, x => decimal.Parse(x.AmountString))
    .Map(x => x.Date, x => DateTime.Parse(x.DateString));
```

### [Data Enrichment](enrichment.md)

Enrich data with lookups, computations, and defaults using a unified API:
- **Lookup**: Enrich from dictionaries (only sets if key exists)
- **Set**: Set from dictionaries (uses default if key missing)
- **Compute**: Calculate values from item properties
- **Default Values**: Apply fallbacks based on conditions

```csharp
var statusLookup = new Dictionary<int, string> { { 1, "Active" }, { 2, "Inactive" } };

builder.AddEnrichment<Order>()
    // Lookup enrichment
    .Lookup(x => x.StatusName, statusLookup, x => x.StatusId)
    // Computed properties
    .Compute(x => x.Total, order => order.Items.Sum(i => i.Price * i.Quantity))
    // Default values
    .DefaultIfNull(x => x.OrderDate, DateTime.UtcNow)
    .DefaultIfEmpty(x => x.Notes, "No notes");
```

## Quick Start

### Installation

```bash
dotnet add package NPipeline.Extensions.Nodes
```

### Basic Usage

```csharp
using NPipeline;
using NPipeline.Extensions.Nodes;

// Create a simple pipeline
var builder = new PipelineBuilder();

// Add numeric cleansing
var cleanseHandle = builder.AddNumericCleansing<Order>()
    .Clamp(x => x.Discount, 0, 100)
    .Round(x => x.Price, 2);

// Add numeric validation
var validateHandle = builder.AddNumericValidation<Order>()
    .IsGreaterThan(x => x.Total, 0);

// Add enrichment
var enrichHandle = builder.AddEnrichment<Order>()
    .DefaultIfNull(x => x.OrderDate, DateTime.UtcNow)
    .Compute(x => x.TotalWithDiscount, order => 
        order.Total * (1 - order.Discount / 100));

// Build and execute
var pipeline = builder.Build();
var result = await pipeline.ExecuteAsync();
```

## Common Patterns

### Chaining Operations

```csharp
builder.AddNumericCleansing<Product>()
    .Round(x => x.Price, 2)
    .Clamp(x => x.Discount, 0, 100)
    .AbsoluteValue(x => x.Adjustment);

builder.AddDateTimeCleansing<Event>()
    .ToUtc(x => x.StartTime)
    .RoundToMinute(x => x.StartTime);
```

### Multiple Properties

```csharp
builder.AddNumericCleansing<Order>()
    .Round(x => x.Subtotal, 2)
    .Round(x => x.Tax, 2)
    .Round(x => x.Total, 2);

builder.AddDateTimeCleansing<Document>()
    .ToUtc(x => x.CreatedAt)
    .ToUtc(x => x.UpdatedAt);
```

### Validation with Custom Messages

```csharp
builder.AddNumericValidation<Product>()
    .IsGreaterThan(x => x.Price, 0, "Price must be positive")
    .IsLessThan(x => x.Discount, 100, "Discount cannot exceed 100%");

builder.AddDateTimeValidation<Event>()
    .IsInFuture(x => x.StartDate, "Event must be in the future")
    .IsAfter(x => x.EndDate, x => x.StartDate, "End date must be after start date");
```

## Performance Characteristics

All nodes in this extension are optimized for performance:

| Aspect | Characteristics |
|--------|-----------------|
| **Memory** | Zero allocations in hot paths for most operations |
| **Expressions** | Property access is compiled once, reused for all items |
| **Thread-Safety** | All nodes are stateless and thread-safe |
| **Scalability** | Works efficiently with millions of items |

## Error Handling

Nodes integrate seamlessly with NPipeline's error handling:

```csharp
// Validation errors automatically create exceptions
try
{
    await pipeline.ExecuteAsync();
}
catch (ValidationException ex)
{
    Console.WriteLine($"Validation failed on {ex.PropertyPath}: {ex.Message}");
}

// Configure custom error handlers
builder.WithErrorHandler(validationHandle, new CustomValidationHandler());
```

## Best Practices

1. **Order operations efficiently**: Place cheap operations (trim) before expensive ones (regex)
2. **Cleanse before validating**: Always cleanse data before validation for consistent results
3. **Use specific validators**: Prefer specific validators (IsEmail) over generic patterns
4. **Reuse nodes**: Nodes are stateless and can be reused across pipelines
5. **Test edge cases**: Test with null, empty, whitespace, and min/max values

## API Reference

### String Operations

| Method | Description | Parameters |
|--------|-------------|------------|
| `Trim()` | Remove leading/trailing whitespace | - |
| `ToLower()` | Convert to lowercase | `CultureInfo? culture` |
| `ToUpper()` | Convert to uppercase | `CultureInfo? culture` |
| `ToTitleCase()` | Convert to title case | `CultureInfo? culture` |
| `CollapseWhitespace()` | Collapse multiple spaces to one | - |
| `RemoveSpecialCharacters()` | Remove non-alphanumeric characters | - |
| `Truncate(length)` | Truncate to max length | `int maxLength` |
| `Replace(old, new)` | Replace substring | `string old, string new` |

### Numeric Operations

| Method | Description | Parameters |
|--------|-------------|------------|
| `Round(digits)` | Round to N decimals | `int digits` |
| `Clamp(min, max)` | Clamp to range | `T min, T max` |
| `Floor()` | Round down | - |
| `Ceiling()` | Round up | - |
| `AbsoluteValue()` | Get absolute value | - |
| `Scale(factor)` | Multiply by factor | `T factor` |

### Validation Methods

| Method | Description | Parameters |
|--------|-------------|------------|
| `HasMinLength(min)` | Minimum length | `int min` |
| `HasMaxLength(max)` | Maximum length | `int max` |
| `IsEmail()` | Email format | - |
| `IsUrl()` | URL format | - |
| `IsGuid()` | GUID format | - |
| `IsGreaterThan(value)` | Greater than check | `T value` |
| `IsLessThan(value)` | Less than check | `T value` |

## See Also

- [Data Cleansing Guide](cleansing.md) - Detailed cleansing node documentation
- [Data Validation Guide](validation.md) - Detailed validation node documentation
- [Filtering Guide](filtering.md) - Filtering node documentation
- [Type Conversion Guide](conversion.md) - Type conversion documentation
- [NPipeline Core](../../getting-started/index.md) - Core pipeline concepts

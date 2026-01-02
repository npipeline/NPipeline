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

## Node Categories

### [Data Cleansing](cleansing.md)

Normalize and clean data properties:
- **String Cleansing**: Trim, case conversion, whitespace handling, special character removal
- **Numeric Cleansing**: Rounding, clamping, scaling, null defaults
- **DateTime Cleansing**: Timezone conversion, truncation, kind normalization
- **Collection Cleansing**: Deduplication, filtering, sorting

```csharp
builder.AddStringCleansing<Person>(x => x.Email)
    .Trim()
    .ToLower();

builder.AddNumericCleansing<Order>(x => x.Discount)
    .Clamp(0, 100);
```

### [Data Validation](validation.md)

Validate property values with clear error messages:
- **String Validation**: Email, URL, regex patterns, length constraints
- **Numeric Validation**: Range checks, type validation, positive/negative constraints
- **DateTime Validation**: Range checks, timezone validation
- **Collection Validation**: Length constraints, element validation

```csharp
builder.AddStringValidation<User>(x => x.Email)
    .IsEmail()
    .HasMaxLength(255);

builder.AddNumericValidation<Product>(x => x.Price)
    .IsGreaterThan(0)
    .HasDecimals(maxDecimalPlaces: 2);
```

### [Filtering](filtering.md)

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

// Add cleansing node
var cleanseHandle = builder
    .AddStringCleansing<Person>(x => x.Name)
    .Trim()
    .ToLower();

// Add validation node
var validateHandle = builder
    .AddStringValidation<Person>(x => x.Email)
    .IsEmail();

// Add filtering node
var filterHandle = builder.AddFiltering<Person>(x => x.Age >= 18);

// Connect nodes
builder.Connect(cleanseHandle, validateHandle);
builder.Connect(validateHandle, filterHandle);

// Build and execute
var pipeline = builder.Build();
var result = await pipeline.ExecuteAsync();
```

## Common Patterns

### Chaining Operations

```csharp
builder.AddStringCleansing<User>(x => x.Username)
    .Trim()
    .ToLower()
    .RemoveSpecialCharacters();

builder.AddNumericCleansing<Price>(x => x.Amount)
    .Round(2)
    .Clamp(0, decimal.MaxValue);
```

### Multiple Properties

```csharp
builder.AddStringCleansing<Contact>(x => x.FirstName)
    .Trim()
    .ToTitleCase();

builder.AddStringCleansing<Contact>(x => x.LastName)
    .Trim()
    .ToTitleCase();

builder.AddStringCleansing<Contact>(x => x.Email)
    .Trim()
    .ToLower();
```

### Validation with Custom Messages

```csharp
builder.AddStringValidation<User>(x => x.Password)
    .HasMinLength(8, "Password must be at least 8 characters")
    .Matches(@"[A-Z]", "Password must contain uppercase letter")
    .Matches(@"[0-9]", "Password must contain digit");
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

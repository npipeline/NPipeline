---
title: Data Enrichment
description: Enrich data with lookups, computed properties, and defaults using a unified, fluent API
sidebar_position: 5
---

# Data Enrichment

The `EnrichmentNode<T>` provides a unified API for enriching data through lookups, computations, and default values. All operations are chainable and execute in the order they're defined.

## Overview

Enrichment operations fall into four categories:

- **Lookup** - Enrich from dictionaries (only sets if key exists)
- **Set** - Set from dictionaries (sets to default if key missing)
- **Compute** - Calculate values from item properties
- **Default** - Apply fallback values based on conditions

## Lookup Operations

Enrich properties by looking up values in dictionaries. Only sets the property if the key exists.

```csharp
var statusLookup = new Dictionary<int, string>
{
    { 1, "Active" },
    { 2, "Inactive" },
    { 3, "Pending" }
};

builder.AddEnrichment<Order>()
    .Lookup(x => x.StatusDescription, statusLookup, x => x.StatusId);
```

### Method Signature

```csharp
Lookup<TKey, TValue>(
    Expression<Func<T, TValue>> propertySelector,
    IReadOnlyDictionary<TKey, TValue> lookup,
    Expression<Func<T, TKey>> keySelector)
```

### Examples

```csharp
// Enrich with country names
var countryLookup = new Dictionary<string, string>
{
    { "US", "United States" },
    { "CA", "Canada" },
    { "MX", "Mexico" }
};

builder.AddEnrichment<Customer>()
    .Lookup(x => x.CountryName, countryLookup, x => x.CountryCode);

// Multiple lookups
builder.AddEnrichment<Order>()
    .Lookup(x => x.StatusName, statusLookup, x => x.StatusId)
    .Lookup(x => x.ShippingMethod, shippingLookup, x => x.ShippingMethodId);
```

## Set Operations

Set property values from dictionaries. Sets to `default(TValue)` if key not found.

```csharp
builder.AddEnrichment<Product>()
    .Set(x => x.CategoryName, categoryLookup, x => x.CategoryId);
// If CategoryId not in lookup, CategoryName becomes null
```

### Method Signature

```csharp
Set<TKey, TValue>(
    Expression<Func<T, TValue>> propertySelector,
    IReadOnlyDictionary<TKey, TValue> lookup,
    Expression<Func<T, TKey>> keySelector)
```

## Compute Operations

Calculate property values from other properties on the item.

```csharp
builder.AddEnrichment<Order>()
    .Compute(x => x.Total, order => 
        order.Items.Sum(i => i.Price * i.Quantity))
    .Compute(x => x.EstimatedDelivery, order =>
        order.OrderDate.AddDays(order.ShippingDays));
```

### Method Signature

```csharp
Compute<TValue>(
    Expression<Func<T, TValue>> propertySelector,
    Func<T, TValue> computeValue)
```

### Examples

```csharp
// Calculate full name
builder.AddEnrichment<User>()
    .Compute(x => x.FullName, user => 
        $"{user.FirstName} {user.LastName}");

// Calculate age from birth date
builder.AddEnrichment<Person>()
    .Compute(x => x.Age, person =>
    {
        var today = DateTime.Today;
        var age = today.Year - person.BirthDate.Year;
        if (person.BirthDate.Date > today.AddYears(-age)) age--;
        return age;
    });
```

## Default Value Operations

Set properties to default values based on various conditions.

### DefaultIfNull

Sets a default value if property is null.

```csharp
builder.AddEnrichment<User>()
    .DefaultIfNull(x => x.CreatedDate, DateTime.UtcNow)
    .DefaultIfNull(x => x.Name, "Unknown");
```

### DefaultIfEmpty

Sets a default value for strings if null or empty.

```csharp
builder.AddEnrichment<Contact>()
    .DefaultIfEmpty(x => x.Phone, "N/A")
    .DefaultIfEmpty(x => x.Email, "no-email@example.com");
```

### DefaultIfWhitespace

Sets a default value for strings if null, empty, or whitespace.

```csharp
builder.AddEnrichment<Contact>()
    .DefaultIfWhitespace(x => x.Address, "No Address");
```

### DefaultIfZero

Sets a default value for numeric properties if zero. Overloaded for `int`, `decimal`, and `double`.

```csharp
builder.AddEnrichment<Product>()
    .DefaultIfZero(x => x.Quantity, 1)
    .DefaultIfZero(x => x.UnitPrice, 9.99m)
    .DefaultIfZero(x => x.DiscountPercent, 0.0);
```

### DefaultIfDefault

Sets a default value if property equals `default(T)`.

```csharp
builder.AddEnrichment<Order>()
    .DefaultIfDefault(x => x.OrderDate, DateTime.UtcNow);
```

### DefaultWhen

Sets a default value based on a custom condition.

```csharp
builder.AddEnrichment<Product>()
    .DefaultWhen(x => x.Status, "Available", status => 
        string.IsNullOrEmpty(status) || status == "Unknown");
```

### DefaultIfEmptyCollection

Sets a default collection if the property is null or empty.

```csharp
builder.AddEnrichment<Order>()
    .DefaultIfEmptyCollection(x => x.Items, new List<OrderItem>());
```

## Complete Examples

### Chaining Multiple Operations

All enrichment operations can be chained together:

```csharp
var statusLookup = new Dictionary<int, string>
{
    { 1, "Active" },
    { 2, "Inactive" }
};

builder.AddEnrichment<Order>()
    // First, apply defaults
    .DefaultIfNull(x => x.OrderDate, DateTime.UtcNow)
    .DefaultIfEmpty(x => x.CustomerName, "Guest")
    .DefaultIfZero(x => x.Quantity, 1)
    
    // Then, enrich from lookups
    .Lookup(x => x.StatusDescription, statusLookup, x => x.StatusId)
    
    // Finally, compute derived values
    .Compute(x => x.Total, order => order.Quantity * order.UnitPrice)
    .Compute(x => x.Label, order => 
        $"{order.CustomerName} - {order.StatusDescription}");
```

### Real-World Pipeline

```csharp
var builder = new PipelineBuilder();

// Clean the data
builder.AddStringCleansing<Order>()
    .Trim(x => x.CustomerName)
    .ToTitleCase(x => x.CustomerName);

// Validate required fields
builder.AddStringValidation<Order>()
    .IsNotEmpty(x => x.CustomerName)
    .HasMaxLength(x => x.CustomerName, 100);

builder.AddNumericValidation<Order>()
    .IsGreaterThan(x => x.Amount, 0);

// Enrich with defaults, lookups, and computed values
builder.AddEnrichment<Order>()
    .DefaultIfNull(x => x.OrderDate, DateTime.UtcNow)
    .DefaultIfEmpty(x => x.Notes, "No notes")
    .Lookup(x => x.StatusName, statusLookup, x => x.StatusId)
    .Compute(x => x.Total, order =>
        order.Items.Sum(i => i.Price * i.Quantity));

var pipeline = builder.Build();
```

## API Reference

### Lookup & Set Methods

| Method | Description |
|--------|-------------|
| `Lookup<TKey, TValue>(property, lookup, key)` | Enrich from dictionary, only if key exists |
| `Set<TKey, TValue>(property, lookup, key)` | Set from dictionary, use default if key missing |

### Compute Methods

| Method | Description |
|--------|-------------|
| `Compute<TValue>(property, computeValue)` | Calculate and set property value |

### Default Value Methods

| Method | Description | Condition |
|--------|-------------|-----------|
| `DefaultIfNull<TValue>(property, default)` | Set if null | `value == null` |
| `DefaultIfEmpty(property, default)` | Set if empty string | `string.IsNullOrEmpty(value)` |
| `DefaultIfWhitespace(property, default)` | Set if whitespace string | `string.IsNullOrWhiteSpace(value)` |
| `DefaultIfZero(property, default)` | Set if zero (int/decimal/double) | `value == 0` |
| `DefaultIfDefault<TValue>(property, default)` | Set if default value | `value == default(TValue)` |
| `DefaultWhen<TValue>(property, condition, default)` | Set if condition true | Custom predicate |
| `DefaultIfEmptyCollection<TItem>(property, default)` | Set if null/empty collection | No items in collection |

## Performance Notes

- **Compiled expressions** for zero-reflection property access
- **Dictionary lookups** use O(1) hash-based operations
- **Operations execute in order** - later operations see results of earlier ones
- **Single pass** - all enrichments applied during one item traversal

## Thread Safety

`EnrichmentNode<T>` is **immutable after construction** and safe to use across multiple pipeline executions.

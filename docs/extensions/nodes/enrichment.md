---
title: Data Enrichment Nodes
description: Enrich data with lookup values, computed properties, and default values. Set defaults based on conditions and enrich from external sources.
sidebar_position: 5
---

# Data Enrichment Nodes

Enrichment nodes add, compute, or set default values on properties. They enable data enrichment from lookup dictionaries and conditional default value assignment.

## Lookup Enrichment

Enrich data by looking up values in dictionaries based on key properties:

```csharp
var statusLookup = new Dictionary<int, string>
{
    { 1, "Active" },
    { 2, "Inactive" },
    { 3, "Pending" }
};

builder.Add(new LookupEnrichmentNode<Order>()
    .AddProperty(x => x.StatusId, statusLookup, x => x.StatusName));
```

### Available Operations

| Operation | Purpose | Use Case |
|-----------|---------|----------|
| `AddProperty<TKey, TValue>(keySelector, lookup, valueSetter)` | Add a property from lookup if key exists | Enrich with code descriptions |
| `ReplaceProperty<TKey, TValue>(keySelector, lookup, valueSetter)` | Replace property value from lookup | Update status descriptions |
| `AddProperties<TKey, TValue>(keySelector, lookup, valueSetters)` | Add multiple properties from one lookup | Populate related fields |
| `AddComputedProperty<TValue>(selector, computeValue)` | Compute and set property value | Calculate derived values |

### Examples

```csharp
// Single lookup enrichment
var countryCodeLookup = new Dictionary<string, string>
{
    { "US", "United States" },
    { "CA", "Canada" },
    { "MX", "Mexico" }
};

builder.Add(new LookupEnrichmentNode<Customer>()
    .AddProperty(x => x.CountryCode, countryCodeLookup, x => x.CountryName));

// Multiple properties from same lookup
var regionLookup = new Dictionary<int, string>
{
    { 1, "North America" },
    { 2, "Europe" },
    { 3, "Asia" }
};

builder.Add(new LookupEnrichmentNode<Location>()
    .AddProperties(
        x => x.RegionId,
        regionLookup,
        x => x.RegionName,
        x => x.RegionDescription));

// Computed properties
builder.Add(new LookupEnrichmentNode<Order>()
    .AddComputedProperty(x => x.Total, order =>
        order.Items.Sum(i => i.Price * i.Quantity)));

// Replace with fallback to default
var categoryLookup = new Dictionary<int, string>
{
    { 1, "Electronics" },
    { 2, "Books" }
};

builder.Add(new LookupEnrichmentNode<Product>()
    .ReplaceProperty(x => x.CategoryId, categoryLookup, x => x.CategoryName));
// If CategoryId not in lookup, CategoryName becomes null
```

### Chaining

Combine multiple enrichment operations:

```csharp
builder.Add(new LookupEnrichmentNode<Order>()
    .AddProperty(x => x.StatusId, statusLookup, x => x.StatusName)
    .AddProperty(x => x.ShippingMethodId, shippingLookup, x => x.ShippingMethod)
    .AddComputedProperty(x => x.EstimatedDelivery, order =>
        order.OrderDate.AddDays(order.ShippingDays)));
```

## Default Value Nodes

Set property values to defaults based on null checks and conditions:

```csharp
builder.Add(new DefaultValueNode<User>()
    .DefaultIfNull(x => x.CreatedDate, () => DateTime.UtcNow)
    .DefaultIfNullOrEmpty(x => x.Department, "Unassigned"));
```

### Available Operations

| Operation | Purpose | Trigger Condition |
|-----------|---------|------------------|
| `DefaultIfNull<TProp>(selector, default)` | Set default if null | Property is null |
| `DefaultIfNullOrEmpty(selector, default)` | Set default if null or empty string | String is null or empty |
| `DefaultIfNullOrWhitespace(selector, default)` | Set default if null or whitespace | String is null, empty, or whitespace |
| `DefaultIfDefault<TProp>(selector, default)` | Set default if equals default(T) | Property equals default value |
| `DefaultIfCondition<TProp>(selector, default, condition)` | Set default if condition true | Custom condition |
| `DefaultIfZero(selector, default)` | Set default if zero (int) | Integer property is 0 |
| `DefaultIfZero(selector, default)` | Set default if zero (decimal) | Decimal property is 0m |
| `DefaultIfZero(selector, default)` | Set default if zero (double) | Double property is 0.0 |
| `DefaultIfEmpty<TItem>(selector, default)` | Set default if empty collection | Collection has no items |

### Examples

```csharp
// Null defaults
builder.Add(new DefaultValueNode<User>()
    .DefaultIfNull(x => x.CreatedDate, DateTime.UtcNow)
    .DefaultIfNull(x => x.UpdatedDate, DateTime.UtcNow));

// String defaults
builder.Add(new DefaultValueNode<Contact>()
    .DefaultIfNullOrEmpty(x => x.Phone, "N/A")
    .DefaultIfNullOrWhitespace(x => x.Address, "No Address"));

// Numeric defaults
builder.Add(new DefaultValueNode<Product>()
    .DefaultIfZero(x => x.Quantity, 0)
    .DefaultIfZero(x => x.UnitPrice, 0m)
    .DefaultIfDefault(x => x.DiscountPercent, 0));

// Collection defaults
builder.Add(new DefaultValueNode<Order>()
    .DefaultIfEmpty(x => x.Items, new List<OrderItem>()));

// Conditional defaults
builder.Add(new DefaultValueNode<Account>()
    .DefaultIfCondition(x => x.Status, "Unknown", x => x.Status == null || x.Status == string.Empty));
```

### Chaining

Combine multiple default operations:

```csharp
builder.Add(new DefaultValueNode<User>()
    .DefaultIfNull(x => x.CreatedDate, DateTime.UtcNow)
    .DefaultIfNullOrEmpty(x => x.FirstName, "Unknown")
    .DefaultIfNullOrEmpty(x => x.LastName, "User")
    .DefaultIfNullOrEmpty(x => x.Email, "no-email@example.com")
    .DefaultIfZero(x => x.Age, 0));
```

## Complete Example

Combining cleansing, validation, and enrichment:

```csharp
var statusLookup = new Dictionary<int, string>
{
    { 1, "Active" },
    { 2, "Inactive" }
};

var builder = new PipelineBuilder();

// Clean the data
builder.AddStringCleansing<Order>(x => x.CustomerName)
    .Trim()
    .ToTitleCase();

// Validate required fields
builder.AddStringValidation<Order>(x => x.CustomerName)
    .HasMinLength(1)
    .HasMaxLength(100);

builder.AddNumericValidation<Order>(x => x.Amount)
    .IsGreaterThan(0);

// Enrich with defaults
builder.Add(new DefaultValueNode<Order>()
    .DefaultIfNull(x => x.OrderDate, DateTime.UtcNow)
    .DefaultIfNullOrEmpty(x => x.Notes, "No notes"));

// Enrich with lookups
builder.Add(new LookupEnrichmentNode<Order>()
    .AddProperty(x => x.StatusId, statusLookup, x => x.StatusName)
    .AddComputedProperty(x => x.Total, order =>
        order.Items.Sum(i => i.Price * i.Quantity)));

var pipeline = builder.Build();
```

## Performance Notes

- **Lookup operations** use compiled expressions for property access - zero reflection in hot paths
- **Dictionary lookups** use O(1) hash-based lookups
- **Computed properties** are calculated once per item during pipeline execution
- **Default operations** perform simple null/equality checks - negligible overhead

## Thread Safety

All nodes are **immutable after construction** and safe to use across multiple pipeline executions.

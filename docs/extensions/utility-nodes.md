---
title: "Utility Nodes"
description: "Pre-built nodes for common data operations: cleansing, validation, filtering, conversion, and enrichment."
order: 10
---

# Utility Nodes

The `NPipeline.Extensions.Nodes` package provides pre-built transform nodes for common data operations. These nodes use compiled expressions internally - no runtime reflection in the processing hot path.

```bash
dotnet add package NPipeline.Extensions.Nodes
```

All utility nodes operate on generic types and target specific properties using expression-based configuration.

## Cleansing Nodes

Cleansing nodes normalize and clean property values in place. They extend `PropertyTransformationNode<T>`, which uses compiled property accessors for performance.

### StringCleansingNode\<T>

Normalizes string properties: trimming, whitespace collapsing, case conversion, and more.

```csharp
var cleanse = builder.AddTransform<StringCleansingNode<Customer>, Customer, Customer>("cleanse");
```

| Operation | Description |
|-----------|-------------|
| `Trim()` | Remove leading/trailing whitespace |
| `TrimStart()` / `TrimEnd()` | Remove leading or trailing whitespace |
| `CollapseWhitespace()` | Replace multiple spaces with a single space |
| `RemoveWhitespace()` | Remove all whitespace characters |
| `ToLower()` / `ToUpper()` / `ToTitleCase()` | Case conversion |
| `RemoveSpecialCharacters()` | Remove non-alphanumeric characters |
| `RemoveDigits()` | Remove numeric characters |
| `RemoveNonAscii()` | Remove characters outside ASCII range |
| `Truncate(maxLength)` | Truncate to maximum length |
| `EnsurePrefix(value)` / `EnsureSuffix(value)` | Add prefix/suffix if not already present |
| `Replace(old, new)` | String replacement |
| `DefaultIfNullOrWhitespace(value)` | Replace null/whitespace with default |
| `DefaultIfNullOrEmpty(value)` | Replace null/empty with default |
| `NullIfWhitespace()` | Set to null if whitespace |

### NumericCleansingNode\<T>

Normalizes numeric properties: clamping to ranges, min/max bounds.

```csharp
var clamp = builder.AddTransform<NumericCleansingNode<SensorReading>, SensorReading, SensorReading>("clamp");
```

| Operation | Description |
|-----------|-------------|
| `Clamp(selector, min, max)` | Constrain value to range |
| `Min(selector, minValue)` | Ensure value is at least minimum |
| `Max(selector, maxValue)` | Ensure value is at most maximum |

Supports both nullable and non-nullable numeric types.

### DateTimeCleansingNode\<T>

Normalizes DateTime properties: kind specification and UTC conversion.

```csharp
var normalize = builder.AddTransform<DateTimeCleansingNode<Event>, Event, Event>("normalize-dates");
```

| Operation | Description |
|-----------|-------------|
| `SpecifyKind(selector, kind)` | Set `DateTimeKind` (Local, Utc, Unspecified) |
| `ToUtc(selector, sourceTimeZone?)` | Convert to UTC with optional source timezone |

Supports both `DateTime` and `DateTime?`.

### CollectionCleansingNode\<T>

Normalizes collection properties: removing nulls, deduplication, sorting, and slicing.

```csharp
var dedupe = builder.AddTransform<CollectionCleansingNode<Order>, Order, Order>("clean-items");
```

| Operation | Description |
|-----------|-------------|
| `RemoveNulls(selector)` | Remove null entries |
| `RemoveDuplicates(selector, comparer?)` | Deduplicate with optional comparer |
| `RemoveEmpty(selector)` | Remove empty strings |
| `RemoveWhitespace(selector)` | Remove whitespace-only strings |
| `Sort(selector, comparer?)` | Sort with optional comparer |
| `Reverse(selector)` | Reverse order |
| `Take(selector, count)` | Take first N items |
| `Skip(selector, count)` | Skip first N items |

### Chaining Operations

All cleansing operations are chainable and execute in order:

```csharp
builder.AddStringCleansing<Customer>()
    .Trim(x => x.Name)
    .CollapseWhitespace(x => x.Name)
    .ToTitleCase(x => x.Name)
    .DefaultIfNullOrEmpty(x => x.Phone, "N/A");
```

## Validation Nodes

Validation nodes check property values and throw `ValidationException` when constraints are violated. They extend `ValidationNode<T>`, which uses compiled getters.

The input type passes through unchanged on success (`TransformNode<T, T>`), so validation nodes can be inserted into any pipeline without changing the data type.

Rules are registered via strongly-typed expressions:

```csharp
validator.Register(
    x => x.Email,
    email => email.Contains('@'),
    "ValidEmail",
    email => $"Invalid email: {email}");
```

Use `RegisterMany` to apply the same rule to multiple properties at once.

### StringValidationNode\<T>

| Rule | Description |
|------|-------------|
| `IsNotEmpty(selector)` | String is not null or empty |
| `IsNotWhitespace(selector)` | String is not null, empty, or whitespace |
| `HasMinLength(selector, min)` | Minimum length |
| `HasMaxLength(selector, max)` | Maximum length |
| `Matches(selector, regex)` | Matches regex pattern |
| `IsEmail(selector)` | Valid email format |
| `IsUrl(selector)` | Valid URL format |
| `IsDigitsOnly(selector)` | Contains only digits |
| `IsAlphanumeric(selector)` | Contains only letters and digits |

```csharp
builder.AddStringValidation<Customer>()
    .IsNotEmpty(x => x.Name)
    .HasMaxLength(x => x.Name, 100)
    .IsEmail(x => x.Email);
```

### NumericValidationNode\<T>

| Rule | Description |
|------|-------------|
| `IsPositive(selector)` | Value > 0 |
| `IsNegative(selector)` | Value < 0 |
| `IsZeroOrPositive(selector)` | Value >= 0 |
| `IsNotZero(selector)` | Value != 0 |
| `IsEven(selector)` / `IsOdd(selector)` | Parity check |
| `IsGreaterThan(selector, threshold)` | Value > threshold |
| `IsLessThan(selector, threshold)` | Value < threshold |
| `IsBetween(selector, min, max)` | min <= value <= max |
| `IsFinite(selector)` | Not infinity or NaN |
| `IsIntegerValue(selector)` | Double/decimal is whole number |
| `IsNotNull(selector)` | Nullable value is not null |

Supports nullable types. Nullable overloads check for null first.

```csharp
builder.AddNumericValidation<Order>()
    .IsPositive(x => x.Amount)
    .IsBetween(x => x.Quantity, 1, 10000);
```

### DateTimeValidationNode\<T>

| Rule | Description |
|------|-------------|
| `IsInFuture(selector)` | After `DateTime.UtcNow` |
| `IsInPast(selector)` | Before `DateTime.UtcNow` |
| `IsToday(selector)` | Same date as today |
| `IsWeekday(selector)` / `IsWeekend(selector)` | Day of week check |
| `IsDayOfWeek(selector, day)` | Specific day |
| `IsUtc(selector)` / `IsLocal(selector)` | DateTime kind check |
| `IsBefore(selector, date)` / `IsAfter(selector, date)` | Comparison |
| `IsBetween(selector, start, end)` | Range check |
| `IsInYear(selector, year)` / `IsInMonth(selector, month)` | Calendar check |

Supports nullable types.

```csharp
builder.AddDateTimeValidation<Event>()
    .IsInFuture(x => x.ScheduledDate)
    .IsUtc(x => x.ScheduledDate);
```

### CollectionValidationNode\<T>

| Rule | Description |
|------|-------------|
| `IsNotEmpty(selector)` | Collection has at least one item |
| `HasMinCount(selector, min)` | At least N items |
| `HasMaxCount(selector, max)` | At most N items |
| `HasCountBetween(selector, min, max)` | Count in range |
| `Contains(selector, item)` | Contains specific item |
| `AllMatch(selector, predicate)` | All items satisfy predicate |
| `AnyMatch(selector, predicate)` | At least one item satisfies predicate |
| `NoneMatch(selector, predicate)` | No items satisfy predicate |
| `AllUnique(selector)` | No duplicate items |
| `IsSubsetOf(selector, superset)` | All items in superset |

```csharp
builder.AddCollectionValidation<Order>()
    .IsNotEmpty(x => x.Items)
    .AllMatch(x => x.Items, item => item.Quantity > 0);
```

### Custom Validation Messages

All rules accept an optional message or message factory:

```csharp
builder.AddStringValidation<Customer>()
    .IsNotEmpty(x => x.Email, "Email is required")
    .HasMaxLength(x => x.Name, 100, name => $"Name '{name}' exceeds 100 chars");
```

## Filtering

### FilteringNode\<T>

Filters items using one or more predicates. Items that fail the filter throw `FilteringException`, which integrates with the resilience system - you can skip, dead-letter, or retry filtered items via your resilience policy.

```csharp
// Constructor with predicate
var filter = new FilteringNode<Order>(o => o.Status == "Active");

// Fluent Where syntax
var filter = new FilteringNode<Order>()
    .Where(o => o.Status == "Active", "Must be active")
    .Where(o => o.Total > 0, o => $"Invalid total: {o.Total}");
```

Multiple `Where` calls are combined with AND logic - all predicates must pass. Each predicate can include a reason string or factory for diagnostics.

### Complex Predicates

```csharp
var filter = new FilteringNode<Order>()
    .Where(o => o.Items.Any(i => i.InStock), "Must have in-stock items")
    .Where(o => o.Customer != null && o.Customer.IsVerified, "Customer must be verified")
    .Where(o => o.OrderDate >= DateTime.UtcNow.AddDays(-30), "Order must be within 30 days");
```

### Multi-Stage Filtering

```csharp
// Stage 1: quick checks
var preFilter = new FilteringNode<Order>()
    .Where(o => o.Status == "Active");

// Stage 2: expensive checks (run only on pre-filtered items)
var detailFilter = new FilteringNode<Order>()
    .Where(o => ExternalService.IsValid(o.CustomerId));
```

> **Tip:** Combine `FilteringNode<T>` with a resilience policy that returns `Skip` for `FilteringException` to silently drop items that don't match your criteria.

## Type Conversion

### TypeConversionNode\<TIn, TOut>

Converts items from one type to another. Throws `TypeConversionException` on failure with source type, target type, and the failing value.

#### Custom Converter

```csharp
var convert = new TypeConversionNode<RawRecord, ProcessedRecord>()
    .WithConverter(raw => new ProcessedRecord(
        raw.Id,
        DateTime.Parse(raw.DateString),
        decimal.Parse(raw.AmountString)));
```

#### Built-in Factory Methods

The `TypeConversions` static class provides pre-built converters:

| From | To | Factory Method | Notes |
|------|----|----------------|-------|
| `string` | `int` | `TypeConversions.StringToInt()` | Supports NumberStyles and format providers |
| `string` | `long` | `TypeConversions.StringToLong()` | Supports NumberStyles and format providers |
| `string` | `double` | `TypeConversions.StringToDouble()` | Supports NumberStyles and format providers |
| `string` | `decimal` | `TypeConversions.StringToDecimal()` | Supports NumberStyles and format providers |
| `string` | `bool` | `TypeConversions.StringToBool()` | Accepts: true/false, yes/no, on/off, 1/0 |
| `string` | `DateTime` | `TypeConversions.StringToDateTime()` | Supports format specifiers and providers |
| `string` | `TEnum` | `TypeConversions.StringToEnum<T>()` | Case-sensitive or insensitive |
| `int` | `string` | `TypeConversions.IntToString()` | Supports format specifiers |
| `double` | `string` | `TypeConversions.DoubleToString()` | Supports format specifiers |
| `decimal` | `string` | `TypeConversions.DecimalToString()` | Supports format specifiers |
| `DateTime` | `string` | `TypeConversions.DateTimeToString()` | Supports format specifiers |
| `bool` | `string` | `TypeConversions.BoolToString()` | Customizable true/false representations |
| `TEnum` | `string` | `TypeConversions.EnumToString<T>()` | Uses enum's `ToString()` |

#### Culture-Aware Conversions

```csharp
var germanCulture = new CultureInfo("de-DE");

// German decimal separator (comma)
var node = TypeConversions.StringToDouble(
    NumberStyles.Float | NumberStyles.AllowThousands,
    germanCulture);
var result = await node.TransformAsync("42,5", context, ct);
// result = 42.5

// Format output with culture
var node = TypeConversions.DecimalToString("C", germanCulture);
var result = await node.TransformAsync(42.50m, context, ct);
// result = "42,50 €"
```

#### Edge Cases

```csharp
// Null/empty strings throw TypeConversionException
var node = TypeConversions.StringToInt();
// "" → TypeConversionException
// null → TypeConversionException

// Infinity and NaN are valid doubles
var node = TypeConversions.StringToDouble();
// "Infinity" → double.PositiveInfinity
// "NaN" → double.NaN

// Boolean parsing is case-insensitive
var node = TypeConversions.StringToBool();
// "YES" → true, "no" → false, "1" → true
```

#### Error Handling

```csharp
try
{
    var result = await TypeConversions.StringToInt()
        .TransformAsync("not a number", context, ct);
}
catch (TypeConversionException ex)
{
    // ex.SourceType = typeof(string)
    // ex.TargetType = typeof(int)
    // ex.Value = "not a number"
}
```

## Enrichment

### EnrichmentNode\<T>

Enriches data by setting properties from lookups, computations, or default values. All operations are chainable and execute in order.

#### Lookup Operations

Set a property from a dictionary - only if the key exists:

```csharp
builder.AddEnrichment<Order>()
    .Lookup(x => x.StatusName, statusLookup, x => x.StatusId);
```

#### Set Operations

Like Lookup, but sets to `default(TValue)` if the key is missing:

```csharp
builder.AddEnrichment<Product>()
    .Set(x => x.CategoryName, categoryLookup, x => x.CategoryId);
```

#### Compute Operations

Calculate values from item properties:

```csharp
builder.AddEnrichment<Order>()
    .Compute(x => x.Total, o => o.Items.Sum(i => i.Price * i.Quantity))
    .Compute(x => x.FullName, o => $"{o.FirstName} {o.LastName}");
```

#### Default Value Operations

| Method | Condition | Example |
|--------|-----------|---------|
| `DefaultIfNull(selector, value)` | Property is null | `DefaultIfNull(x => x.Name, "Unknown")` |
| `DefaultIfEmpty(selector, value)` | String is null or empty | `DefaultIfEmpty(x => x.Phone, "N/A")` |
| `DefaultIfWhitespace(selector, value)` | String is null, empty, or whitespace | `DefaultIfWhitespace(x => x.Address, "No Address")` |
| `DefaultIfZero(selector, value)` | Numeric is zero (int/decimal/double) | `DefaultIfZero(x => x.Quantity, 1)` |
| `DefaultIfDefault(selector, value)` | Property equals `default(T)` | `DefaultIfDefault(x => x.OrderDate, DateTime.UtcNow)` |
| `DefaultWhen(selector, value, condition)` | Custom predicate | `DefaultWhen(x => x.Status, "Available", s => s == "Unknown")` |
| `DefaultIfEmptyCollection(selector, value)` | Collection is null or empty | `DefaultIfEmptyCollection(x => x.Tags, new List<string>())` |

#### Chaining All Operations

```csharp
builder.AddEnrichment<Order>()
    // 1. Apply defaults first
    .DefaultIfNull(x => x.OrderDate, DateTime.UtcNow)
    .DefaultIfEmpty(x => x.CustomerName, "Guest")
    .DefaultIfZero(x => x.Quantity, 1)
    // 2. Enrich from lookups
    .Lookup(x => x.StatusDescription, statusLookup, x => x.StatusId)
    // 3. Compute derived values last
    .Compute(x => x.Total, o => o.Quantity * o.UnitPrice)
    .Compute(x => x.Label, o => $"{o.CustomerName} - {o.StatusDescription}");
```

Operations execute in order - later operations see results of earlier ones.

## Performance Characteristics

All utility nodes share these performance properties:

| Property | Description |
|----------|-------------|
| **Compiled expressions** | Property access uses pre-compiled delegates, not reflection. Cost paid once at node creation |
| **Zero-allocation hot path** | No allocations per-item for property reads/writes |
| **Dictionary lookups** | O(1) hash-based operations for enrichment |
| **Dependency-free** | No external dependencies beyond the core `NPipeline` package |
| **Thread-safe** | Nodes are immutable after configuration - safe for parallel execution |

## See Also

- [Extensions Overview](index.md)

## Error Integration

Validation and filtering nodes throw typed exceptions (`ValidationException`, `FilteringException`, `TypeConversionException`) that integrate with the resilience system. You can handle these in your resilience policy:

```csharp
var policy = ResiliencePolicyBuilder
    .ForNode<StringValidationNode<Customer>, Customer>()
    .On<ValidationException>().DeadLetter()
    .OnAny().Fail()
    .Build();
```

## Next Steps

- [Custom Nodes](../guides/custom-nodes.md) - write your own source, transform, and sink nodes
- [Resilience Policies](../error-handling/resilience-policies.md) - handle validation/filtering failures
- [Extensions Index](index.md) - see all available packages

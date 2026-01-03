---
title: Data Cleansing Nodes
description: Clean and normalize string, numeric, datetime, and collection properties.
sidebar_position: 1
---

# Data Cleansing Nodes

Cleansing nodes normalize and clean data properties. They perform in-place transformations without changing the object's type or structure.

## String Cleansing

Clean and normalize text data:

```csharp
builder.AddStringCleansing<User>(x => x.Email)
    .Trim()
    .ToLower();
```

### Available Operations

| Operation | Purpose | Example |
|-----------|---------|---------|
| `Trim()` | Remove leading/trailing whitespace | `"  hello  "` → `"hello"` |
| `TrimStart()` | Remove leading whitespace | `"  hello"` → `"hello"` |
| `TrimEnd()` | Remove trailing whitespace | `"hello  "` → `"hello"` |
| `CollapseWhitespace()` | Collapse multiple spaces | `"hello   world"` → `"hello world"` |
| `RemoveWhitespace()` | Remove all whitespace | `"hello world"` → `"helloworld"` |
| `ToLower()` | Convert to lowercase | `"Hello"` → `"hello"` |
| `ToUpper()` | Convert to uppercase | `"hello"` → `"HELLO"` |
| `ToTitleCase()` | Convert to title case | `"hello world"` → `"Hello World"` |
| `ToCamelCase()` | Convert to camelCase | `"hello_world"` → `"helloWorld"` |
| `ToPascalCase()` | Convert to PascalCase | `"hello_world"` → `"HelloWorld"` |
| `ToKebabCase()` | Convert to kebab-case | `"helloWorld"` → `"hello-world"` |
| `RemoveSpecialCharacters()` | Remove non-alphanumeric | `"hello@world!"` → `"helloworld"` |
| `RemoveDigits()` | Remove numeric characters | `"hello123"` → `"hello"` |
| `RemoveNonAscii()` | Remove non-ASCII characters | `"café"` → `"caf"` |
| `Truncate(length)` | Truncate to max length | `"hello world"` → `"hello"` (5) |
| `EnsurePrefix(prefix)` | Add prefix if missing | `"world"` → `"hello world"` |
| `EnsureSuffix(suffix)` | Add suffix if missing | `"hello"` → `"hello world"` |
| `Replace(old, new)` | Replace substring | `"hello"` → `"hallo"` |
| `StripDiacritics()` | Remove accent marks | `"café"` → `"cafe"` |
| `DefaultIfNullOrWhitespace(default)` | Use default for empty | `""` → `"N/A"` |

### Examples

```csharp
// Email normalization
builder.AddStringCleansing<User>(x => x.Email)
    .Trim()
    .ToLower()
    .DefaultIfNullOrWhitespace("no-email@example.com");

// Name normalization
builder.AddStringCleansing<Person>(x => x.FirstName)
    .Trim()
    .ToTitleCase();

// Username cleanup
builder.AddStringCleansing<Account>(x => x.Username)
    .Trim()
    .ToLower()
    .RemoveSpecialCharacters();

// Text sanitization
builder.AddStringCleansing<Document>(x => x.Title)
    .Trim()
    .RemoveNonAscii()
    .Truncate(100);
```

## Numeric Cleansing

Clean and normalize numeric data:

```csharp
builder.AddNumericCleansing<Order>(x => x.Discount)
    .Clamp(0, 100)
    .Round(2);
```

### Available Operations

| Operation | Types | Example |
|-----------|-------|---------|
| `Round(digits)` | double, decimal | `3.14159` → `3.14` |
| `Floor()` | double | `3.9` → `3.0` |
| `Ceiling()` | double | `3.1` → `4.0` |
| `Clamp(min, max)` | all numeric | `150` → `100` (clamped to max) |
| `Clamp(nullable, min, max)` | nullable numeric | clamps while preserving null |
| `Min(minValue)` | int, double, decimal | convenience method: `Clamp(minValue, max)` |
| `Max(maxValue)` | int, double, decimal | convenience method: `Clamp(min, maxValue)` |
| `AbsoluteValue()` | double, decimal | `-5.5` → `5.5` |
| `Scale(factor)` | decimal | `10m` × `2.5m` → `25m` |
| `DefaultIfNull(default)` | all nullable | `null` → `0` |
| `ToZeroIfNegative()` | double, decimal | `-5.5` → `0` |

**Note:** Type-specific methods are inferred from parameter types. For example, `Round()` works with both `double` and `decimal` properties through method overloading.

### Examples

```csharp
// Price normalization
builder.AddNumericCleansing<Product>(x => x.Price)
    .Clamp(0, decimal.MaxValue)
    .Round(2);

// Discount clamping
builder.AddNumericCleansing<Order>(x => x.Discount)
    .Clamp(0, 100);

// Percentage cleanup
builder.AddNumericCleansing<Survey>(x => x.CompletionRate)
    .Clamp(0, 100)
    .Round(1);

// Age normalization
builder.AddNumericCleansing<Person>(x => x.Age)
    .Clamp(0, 150);

// Absolute value conversion
builder.AddNumericCleansing<Measurement>(x => x.Value)
    .AbsoluteValue();

// Negative value handling
builder.AddNumericCleansing<Transaction>(x => x.Amount)
    .ToZeroIfNegative();
```

### Numeric Constraints with Min/Max

Use Min/Max helper methods for single-bound constraints:

```csharp
// Ensure age is at least 0 (cleaner than Clamp(0, int.MaxValue))
builder.AddNumericCleansing<Person>(x => x.Age)
    .Min(0);

// Ensure quantity doesn't exceed 10000
builder.AddNumericCleansing<Order>(x => x.Quantity)
    .Max(10000);

// Ensure price is at least 0.01
builder.AddNumericCleansing<Product>(x => x.Price)
    .Min(0.01m);

// Clamp only the upper bound (discount can't exceed 100%)
builder.AddNumericCleansing<Order>(x => x.DiscountPercent)
    .Max(100);

// Nullable numeric with minimum constraint
builder.AddNumericCleansing<Item>(x => x.OptionalStock)
    .Min(0);  // null values pass through, non-null values enforced >= 0
```

## DateTime Cleansing

Clean and normalize date/time data:

```csharp
builder.AddDateTimeCleansing<Event>(x => x.StartTime)
    .SpecifyKind(DateTimeKind.Utc)
    .ToUtc();
```

### Available Operations

| Operation | Purpose | Supports Nullable |
|-----------|---------|-------------------|
| `SpecifyKind(kind)` | Set DateTimeKind | DateTime only |
| `ToUtc()` | Convert to UTC | Both |
| `ToLocal()` | Convert to local time | Both |
| `StripTime()` | Remove time component | Both |
| `Truncate(precision)` | Truncate to precision | Both |
| `RoundToMinute()` | Round to nearest minute | DateTime, DateTime? |
| `RoundToHour()` | Round to nearest hour | DateTime, DateTime? |
| `RoundToDay()` | Round to nearest day | DateTime, DateTime? |
| `Clamp(min, max)` | Constrain to range | DateTime, DateTime? |
| `DefaultIfMinValue(default)` | Replace MinValue | DateTime only |
| `DefaultIfMaxValue(default)` | Replace MaxValue | DateTime only |
| `DefaultIfNull(default)` | Replace null values | DateTime? only |

### Examples

```csharp
// Timestamp normalization
builder.AddDateTimeCleansing<Transaction>(x => x.Timestamp)
    .SpecifyKind(DateTimeKind.Utc)
    .ToUtc();

// Event time cleanup with rounding
builder.AddDateTimeCleansing<Event>(x => x.StartTime)
    .ToUtc()
    .RoundToMinute();

// Date normalization (remove time)
builder.AddDateTimeCleansing<Document>(x => x.CreatedDate)
    .StripTime();

// Default handling for edge cases
builder.AddDateTimeCleansing<Record>(x => x.DateField)
    .DefaultIfMinValue(DateTime.UtcNow)
    .DefaultIfMaxValue(DateTime.UtcNow);
```

### DateTime Rounding and Clamping

Round times and constrain to ranges:

```csharp
// Round timestamps to nearest minute for metrics
builder.AddDateTimeCleansing<Metric>(x => x.RecordedAt)
    .RoundToMinute();

// Round optional timestamps
builder.AddDateTimeCleansing<Event>(x => x.OptionalEndTime)
    .RoundToMinute();  // null values pass through unchanged

// Round to nearest hour for reports
builder.AddDateTimeCleansing<Report>(x => x.GeneratedAt)
    .RoundToHour();

// Clamp dates to valid range
builder.AddDateTimeCleansing<Contract>(x => x.StartDate)
    .Clamp(
        DateTime.Now.AddYears(-1),
        DateTime.Now.AddYears(10),
        "Start date must be within ±10 years");

// Clamp optional dates
builder.AddDateTimeCleansing<Reservation>(x => x.OptionalCheckoutDate)
    .Clamp(DateTime.Now, DateTime.Now.AddYears(2));
```

## Collection Cleansing

Clean and normalize collection properties:

```csharp
builder.AddCollectionCleansing<Document>(x => x.Tags)
    .RemoveNulls()
    .RemoveDuplicates()
    .Sort();
```

### Available Operations

| Operation | Purpose | Example |
|-----------|---------|---------|
| `RemoveNulls()` | Remove null entries | `[1, null, 3]` → `[1, 3]` |
| `RemoveDuplicates()` | Remove duplicates | `[1, 2, 1, 3]` → `[1, 2, 3]` |
| `RemoveEmpty()` | Remove empty strings | `["a", "", "b"]` → `["a", "b"]` |
| `RemoveWhitespace()` | Remove whitespace strings | `["a", "   ", "b"]` → `["a", "b"]` |
| `Sort()` | Sort ascending | `[3, 1, 2]` → `[1, 2, 3]` |
| `Reverse()` | Reverse order | `[1, 2, 3]` → `[3, 2, 1]` |
| `Take(count)` | Take first N items | `[1, 2, 3, 4, 5]` → `[1, 2, 3]` (3) |
| `Skip(count)` | Skip first N items | `[1, 2, 3, 4, 5]` → `[4, 5]` (3) |

### Examples

```csharp
// Tag cleanup
builder.AddCollectionCleansing<Article>(x => x.Tags)
    .RemoveNulls()
    .RemoveEmpty()
    .RemoveDuplicates()
    .Sort();

// Category deduplication
builder.AddCollectionCleansing<Product>(x => x.Categories)
    .RemoveNulls()
    .RemoveDuplicates()
    .Sort();

// Email list cleaning
builder.AddCollectionCleansing<MailingList>(x => x.Emails)
    .RemoveNulls()
    .RemoveEmpty()
    .RemoveDuplicates();
```

## Chaining Operations

Operations can be chained fluently:

```csharp
// Multiple operations on same property
builder.AddStringCleansing<User>(x => x.Email)
    .Trim()
    .ToLower()
    .RemoveSpecialCharacters()
    .DefaultIfNullOrWhitespace("unknown@example.com");

// Multiple properties
builder.AddStringCleansing<Person>(x => x.FirstName)
    .Trim()
    .ToTitleCase();

builder.AddStringCleansing<Person>(x => x.LastName)
    .Trim()
    .ToTitleCase();

builder.AddStringCleansing<Person>(x => x.Email)
    .Trim()
    .ToLower();
```

## Thread Safety

All cleansing nodes are stateless and thread-safe. They can be safely shared across parallel pipelines.

## Performance

Cleansing nodes are optimized for performance:
- Property access uses compiled expressions (not reflection)
- String operations use `StringBuilder` to minimize allocations
- Numeric operations use native types (no boxing)
- Collection operations are evaluated lazily where possible

## Error Handling

Cleansing nodes integrate with NPipeline's error handling:

```csharp
// Custom error handler for cleansing failures
builder.WithErrorHandler(cleanseHandle, new CleansingErrorHandler());

// Continue processing even if cleansing fails
builder.WithErrorDecision(cleanseHandle, NodeErrorDecision.Skip);
```

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
builder.AddStringCleansing<User>()
    .Trim(x => x.Email)
    .ToLower(x => x.Email);
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
| `RemoveSpecialCharacters()` | Remove non-alphanumeric | `"hello@world!"` → `"helloworld"` |
| `RemoveDigits()` | Remove numeric characters | `"hello123"` → `"hello"` |
| `RemoveNonAscii()` | Remove non-ASCII characters | `"café"` → `"caf"` |
| `Truncate(length)` | Truncate to max length | `"hello world"` → `"hello"` (5) |
| `EnsurePrefix(prefix)` | Add prefix if missing | `"world"` → `"hello world"` |
| `EnsureSuffix(suffix)` | Add suffix if missing | `"hello"` → `"hello world"` |
| `Replace(old, new)` | Replace substring | `"hello"` → `"hallo"` |
| `DefaultIfNullOrWhitespace(default)` | Use default for empty | `""` → `"N/A"` |
| `DefaultIfNullOrEmpty(default)` | Use default for null/empty | `""` → `"N/A"` |
| `NullIfWhitespace()` | Convert whitespace to null | `"   "` → `null` |

### Examples

```csharp
// Email normalization
builder.AddStringCleansing<User>()
    .Trim(x => x.Email)
    .ToLower(x => x.Email)
    .DefaultIfNullOrWhitespace(x => x.Email, "no-email@example.com");

// Name normalization
builder.AddStringCleansing<Person>()
    .Trim(x => x.FirstName)
    .ToTitleCase(x => x.FirstName);

// Username cleanup
builder.AddStringCleansing<Account>()
    .Trim(x => x.Username)
    .ToLower(x => x.Username)
    .RemoveSpecialCharacters(x => x.Username);

// Text sanitization
builder.AddStringCleansing<Document>()
    .Trim(x => x.Title)
    .RemoveNonAscii(x => x.Title)
    .Truncate(x => x.Title, 100);
```

## Numeric Cleansing

Clean and normalize numeric data:

```csharp
builder.AddNumericCleansing<Order>()
    .Clamp(x => x.Discount, 0, 100)
    .Round(x => x.Price, 2);
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
builder.AddNumericCleansing<Product>()
    .Clamp(x => x.Price, 0, decimal.MaxValue)
    .Round(x => x.Price, 2);

// Discount clamping
builder.AddNumericCleansing<Order>()
    .Clamp(x => x.Discount, 0, 100);

// Percentage cleanup
builder.AddNumericCleansing<Survey>()
    .Clamp(x => x.CompletionRate, 0, 100)
    .Round(x => x.CompletionRate, 1);

// Age normalization
builder.AddNumericCleansing<Person>()
    .Clamp(x => x.Age, 0, 150);

// Absolute value conversion
builder.AddNumericCleansing<Measurement>()
    .AbsoluteValue(x => x.Value);

// Negative value handling
builder.AddNumericCleansing<Transaction>()
    .ToZeroIfNegative(x => x.Amount);
```

### Numeric Constraints with Min/Max

Use Min/Max helper methods for single-bound constraints:

```csharp
// Ensure age is at least 0 (cleaner than Clamp(0, int.MaxValue))
builder.AddNumericCleansing<Person>()
    .Min(x => x.Age, 0);

// Ensure quantity doesn't exceed 10000
builder.AddNumericCleansing<Order>()
    .Max(x => x.Quantity, 10000);

// Ensure price is at least 0.01
builder.AddNumericCleansing<Product>()
    .Min(x => x.Price, 0.01m);

// Clamp only the upper bound (discount can't exceed 100%)
builder.AddNumericCleansing<Order>()
    .Max(x => x.DiscountPercent, 100);
```

## DateTime Cleansing

Clean and normalize date/time data:

```csharp
builder.AddDateTimeCleansing<Event>()
    .SpecifyKind(x => x.StartTime, DateTimeKind.Utc)
    .ToUtc(x => x.StartTime);
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
builder.AddDateTimeCleansing<Transaction>()
    .SpecifyKind(x => x.Timestamp, DateTimeKind.Utc)
    .ToUtc(x => x.Timestamp);

// Event time cleanup with rounding
builder.AddDateTimeCleansing<Event>()
    .ToUtc(x => x.StartTime)
    .RoundToMinute(x => x.StartTime);

// Date normalization (remove time)
builder.AddDateTimeCleansing<Document>()
    .StripTime(x => x.CreatedDate);

// Default handling for edge cases
builder.AddDateTimeCleansing<Record>()
    .DefaultIfMinValue(x => x.DateField, DateTime.UtcNow)
    .DefaultIfMaxValue(x => x.DateField, DateTime.UtcNow);
```

### DateTime Rounding and Clamping

Round times and constrain to ranges:

```csharp
// Round timestamps to nearest minute for metrics
builder.AddDateTimeCleansing<Metric>()
    .RoundToMinute(x => x.RecordedAt);

// Round optional timestamps
builder.AddDateTimeCleansing<Event>()
    .RoundToMinute(x => x.OptionalEndTime);  // null values pass through unchanged

// Round to nearest hour for reports
builder.AddDateTimeCleansing<Report>()
    .RoundToHour(x => x.GeneratedAt);

// Clamp dates to valid range
builder.AddDateTimeCleansing<Contract>()
    .Clamp(x => x.StartDate,
        DateTime.Now.AddYears(-1),
        DateTime.Now.AddYears(10));

// Clamp optional dates
builder.AddDateTimeCleansing<Reservation>()
    .Clamp(x => x.OptionalCheckoutDate, DateTime.Now, DateTime.Now.AddYears(2));
```

## Collection Cleansing

Clean and normalize collection properties:

```csharp
builder.AddCollectionCleansing<Document>()
    .RemoveNulls(x => x.Tags)
    .RemoveDuplicates(x => x.Tags)
    .Sort(x => x.Tags);
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
builder.AddCollectionCleansing<Article>()
    .RemoveNulls(x => x.Tags)
    .RemoveEmpty(x => x.Tags)
    .RemoveDuplicates(x => x.Tags)
    .Sort(x => x.Tags);

// Category deduplication
builder.AddCollectionCleansing<Product>()
    .RemoveNulls(x => x.Categories)
    .RemoveDuplicates(x => x.Categories)
    .Sort(x => x.Categories);

// Email list cleaning
builder.AddCollectionCleansing<MailingList>()
    .RemoveNulls(x => x.Emails)
    .RemoveEmpty(x => x.Emails)
    .RemoveDuplicates(x => x.Emails);
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

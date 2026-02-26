---
title: Data Validation Nodes
description: Validate property values with clear, actionable error messages.
sidebar_position: 2
---

# Data Validation Nodes

Validation nodes check property values against rules and provide clear error messages when validation fails. Each validation rule can have a custom message for better error reporting.

## String Validation

Validate text data against patterns and constraints:

```csharp
builder.AddStringValidation<User>()
    .IsEmail(x => x.Email, "Email address is not in valid format")
    .HasMaxLength(x => x.Email, 255, "Email cannot exceed 255 characters");

builder.AddStringValidation<User>()
    .HasMinLength(x => x.Password, 8, "Password must be at least 8 characters")
    .Matches(x => x.Password, "[A-Z]", RegexOptions.None, "Password must contain uppercase letter")
    .Matches(x => x.Password, "[0-9]", RegexOptions.None, "Password must contain digit")
    .Matches(x => x.Password, "[!@#$%^&*]", RegexOptions.None, "Password must contain special character");

builder.AddStringValidation<SocialProfile>()
    .IsUrl(x => x.WebsiteUrl, "Website URL must be valid HTTP/HTTPS URL");

builder.AddStringValidation<Contact>()
    .IsDigitsOnly(x => x.Phone, "Phone number must contain only digits")
    .HasLengthBetween(x => x.Phone, 10, 15, "Phone number must be 10-15 digits");

builder.AddStringValidation<Account>()
    .HasMinLength(x => x.Username, 3, "Username must be at least 3 characters")
    .HasMaxLength(x => x.Username, 20, "Username must not exceed 20 characters")
    .IsAlphanumeric(x => x.Username, "Username can only contain letters and digits");
```

## Numeric Validation

Validate numeric data against ranges and constraints:

```csharp
builder.AddNumericValidation<Order>(x => x.Quantity)
    .IsGreaterThan(0)
    .IsLessThan(1000);
```

### Available Validators

| Validator | Purpose | Type |
|-----------|---------|------|
| `IsGreaterThan(value)` | Greater than | int, double, decimal |
| `IsLessThan(value)` | Less than | int, double, decimal |
| `IsBetween(min, max)` | In range (inclusive) | int, double, decimal |
| `IsPositive()` | > 0 | int, double, decimal, int?, double?, decimal? |
| `IsNegative()` | < 0 | int, double, decimal |
| `IsZeroOrPositive()` | >= 0 | int, double, decimal |
| `IsNotNegative()` | >= 0 (alias) | int, double, decimal |
| `IsNonZero()` | != 0 | int, double, decimal |
| `IsEven()` | Even number | int |
| `IsOdd()` | Odd number | int |
| `IsFinite()` | Not NaN/Infinity | double |
| `IsIntegerValue()` | No fractional part | double, decimal |
| `IsNotNull()` | Not null | int?, double?, decimal? |

### Examples

```csharp
// Price validation
builder.AddNumericValidation<Product>(x => x.Price)
    .IsGreaterThan(0, "Price must be greater than zero")
    .IsFinite("Price must be a valid number");

// Discount validation
builder.AddNumericValidation<Order>(x => x.Discount)
    .IsBetween(0, 100, "Discount must be between 0 and 100 percent");

// Age validation
builder.AddNumericValidation<Person>(x => x.Age)
    .IsPositive("Age cannot be negative")
    .IsLessThan(150, "Age must be less than 150");

// Quantity validation
builder.AddNumericValidation<OrderItem>(x => x.Quantity)
    .IsGreaterThan(0, "Quantity must be at least 1")
    .IsLessThan(10001, "Quantity cannot exceed 10000");

// Rating validation
builder.AddNumericValidation<Review>(x => x.Rating)
    .IsBetween(1, 5, "Rating must be between 1 and 5 stars");

// Measurement validation
builder.AddNumericValidation<Sensor>(x => x.Reading)
    .IsFinite("Reading must be a valid number, not NaN or Infinity");
```

### Nullable Numeric Validation

Validate nullable numeric types with built-in null handling:

```csharp
// Optional age that, if provided, must be positive
builder.AddNumericValidation<Person>(x => x.OptionalAge)
    .IsPositive("Age must be positive (null is allowed)")
    .IsNotNull("Age is required");  // If you need to enforce non-null

// Optional quantity that, if provided, must be greater than zero
builder.AddNumericValidation<OrderItem>(x => x.OptionalQuantity)
    .IsPositive("Quantity must be greater than zero if provided");

// Using IsNotNegative (more intuitive alias for IsZeroOrPositive)
builder.AddNumericValidation<Stock>(x => x.Quantity)
    .IsNotNegative("Stock quantity cannot be negative");
```

The nullable overloads for `IsPositive()` automatically handle null values - they pass validation if the value is null, but fail if it's provided and doesn't meet the criteria. Use `IsNotNull()` to explicitly require non-null values.

## DateTime Validation

Validate date and time values:

```csharp
builder.AddDateTimeValidation<Event>(x => x.StartDate)
    .IsInFuture()
    .IsWeekday();
```

### Available Validators

| Validator | Purpose | Supports Nullable |
|-----------|---------|-------------------|
| `IsInFuture()` | After current date/time | DateTime, DateTime? |
| `IsInPast()` | Before current date/time | DateTime, DateTime? |
| `IsToday()` | Today's date | DateTime only |
| `IsWeekday()` | Monday-Friday | DateTime only |
| `IsWeekend()` | Saturday-Sunday | DateTime only |
| `IsDayOfWeek(day)` | Specific day of week | DateTime only |
| `IsUtc()` | UTC timezone | DateTime only |
| `IsLocal()` | Local timezone | DateTime only |
| `IsNotMinValue()` | Not DateTime.MinValue | DateTime only |
| `IsNotMaxValue()` | Not DateTime.MaxValue | DateTime only |
| `IsBefore(date)` | Before specific date | DateTime, DateTime? |
| `IsAfter(date)` | After specific date | DateTime, DateTime? |
| `IsBetween(from, to)` | Within date range | DateTime, DateTime? |
| `IsInYear(year)` | Within specific year | DateTime only |
| `IsInMonth(month)` | Within specific month | DateTime only |
| `IsNotNull()` | Not null | DateTime? only |

### Examples

```csharp
// Event scheduling validation
builder.AddDateTimeValidation<Event>(x => x.StartTime)
    .IsInFuture("Event must start in the future")
    .IsWeekday("Events can only be scheduled on weekdays");

// Birth date validation
builder.AddDateTimeValidation<Person>(x => x.BirthDate)
    .IsInPast("Birth date must be in the past")
    .IsBetween(
        DateTime.Now.AddYears(-150), 
        DateTime.Now,
        "Birth date must be between 150 years ago and today");

// Appointment scheduling
builder.AddDateTimeValidation<Appointment>(x => x.ScheduledTime)
    .IsInFuture("Appointment must be in the future")
    .IsWeekday("Appointments can only be scheduled on weekdays");

// Transaction timestamp
builder.AddDateTimeValidation<Transaction>(x => x.Timestamp)
    .IsUtc("Transaction timestamp must be in UTC");

// Specific day requirement
builder.AddDateTimeValidation<WeeklyReport>(x => x.GeneratedDate)
    .IsDayOfWeek(DayOfWeek.Friday, "Reports must be generated on Fridays");
```

### Nullable DateTime Validation

Validate optional DateTime fields:

```csharp
// Optional deadline that, if provided, must be in the future
builder.AddDateTimeValidation<Task>(x => x.DeadlineDate)
    .IsInFuture("Deadline must be in the future (null is allowed)")
    .IsNotNull("Deadline is required");  // If you need to enforce non-null

// Optional end date that must be after start date if provided
builder.AddDateTimeValidation<Event>(x => x.EndDateTime)
    .IsInFuture("End time must be in the future");  // null passes validation

// Optional notification time
builder.AddDateTimeValidation<Reminder>(x => x.ScheduledTime)
    .IsNotNull("Scheduled time is required");
```

## Collection Validation

Validate collections and their elements:

```csharp
builder.AddCollectionValidation<Document>()
    .HasMinCount(x => x.Tags, 1)
    .HasMaxCount(x => x.Tags, 10);
```

### Available Validators

| Validator | Purpose |
|-----------|---------|
| `HasMinCount(min)` | Minimum items |
| `HasMaxCount(max)` | Maximum items |
| `HasCountBetween(min, max)` | Item count in range |
| `IsNotEmpty()` | Contains at least one item |
| `Contains(item)` | Contains specific item |
| `DoesNotContain(item)` | Does not contain item |
| `AllMatch(predicate)` | All items satisfy condition |
| `AnyMatch(predicate)` | At least one item satisfies condition |
| `NoneMatch(predicate)` | No items match condition |
| `AllUnique()` | All items are unique |
| `IsSubsetOf(allowedValues)` | All items in allowed set |

### Examples

```csharp
// Tag validation
builder.AddCollectionValidation<Article>()
    .HasMinCount(x => x.Tags, 1, "Article must have at least one tag")
    .HasMaxCount(x => x.Tags, 10, "Article cannot have more than 10 tags");

// Category selection
builder.AddCollectionValidation<Product>()
    .IsNotEmpty(x => x.Categories, "Product must have at least one category");

// Email recipients
builder.AddCollectionValidation<EmailMessage>()
    .HasMinCount(x => x.Recipients, 1, "Message must have at least one recipient")
    .AllMatch(x => x.Recipients, email => email.Contains("@"), "All recipients must have valid email format");
```

## Custom Messages

All validators support custom error messages:

```csharp
builder.AddStringValidation<User>()
    .IsEmail(x => x.Email, "Please provide a valid email address")
    .HasMaxLength(x => x.Email, 255, "Email address is too long");

builder.AddNumericValidation<Product>()
    .IsGreaterThan(x => x.Price, 0, "Products must have a price greater than zero")
    .IsBetween(x => x.Discount, 0, 100, "Discount must be between 0 and 100 percent");
```

## Multiple Rules

Chain multiple validation rules on a single property:

```csharp
builder.AddStringValidation<User>(x => x.Username)
    .HasMinLength(3, "Username must be at least 3 characters")
    .HasMaxLength(20, "Username must not exceed 20 characters")
    .IsAlphanumeric("Username can only contain letters and numbers")
    .Matches("^[a-z]", RegexOptions.IgnoreCase, false, "Username must start with a letter");

// Multiple properties
builder.AddStringValidation<User>(x => x.Email)
    .IsEmail()
    .HasMaxLength(255);

builder.AddStringValidation<User>(x => x.Password)
    .HasMinLength(8)
    .Matches("[A-Z]");
```

## Validation with Filtering

Combine validation with filtering:

```csharp
// Only validate active users
builder.AddFiltering<User>(x => x.IsActive);
builder.AddStringValidation<User>(x => x.Email).IsEmail();
```

## Error Handling

Validation errors are captured and can be handled:

```csharp
try
{
    await pipeline.ExecuteAsync();
}
catch (ValidationException ex)
{
    Console.WriteLine($"Property: {ex.PropertyPath}");
    Console.WriteLine($"Rule: {ex.RuleName}");
    Console.WriteLine($"Value: {ex.PropertyValue}");
    Console.WriteLine($"Message: {ex.Message}");
}

// Custom error handler
builder.WithErrorHandler(validationHandle, new CustomValidationHandler());

// Skip invalid items instead of throwing
builder.WithErrorDecision(validationHandle, NodeErrorDecision.Skip);
```

## Thread Safety

All validation nodes are stateless and thread-safe. Compiled validators can be safely shared across parallel pipelines.

## Performance

- Validators use compiled expressions for property access
- String validators use pre-compiled regex patterns
- Range validators use direct numeric comparisons (no boxing)
- First failure short-circuits remaining validators (configurable)

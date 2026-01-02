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
builder.AddStringValidation<User>(x => x.Email)
    .IsEmail()
    .HasMaxLength(255);
```

### Available Validators

| Validator | Purpose | Parameters |
|-----------|---------|------------|
| `IsEmail()` | Valid email format | `bool allowNull` |
| `IsUrl()` | Valid URL (HTTP/HTTPS) | `bool allowNull` |
| `IsGuid()` | Valid GUID format | `bool allowNull` |
| `IsAlphanumeric()` | Letters and digits only | `bool allowNull` |
| `IsAlphabetic()` | Letters only | `bool allowNull` |
| `IsDigitsOnly()` | Digits only | `bool allowNull` |
| `IsNumeric()` | Valid number format | `bool allowNull` |
| `HasMinLength(min)` | Minimum length | `int min, bool allowNull` |
| `HasMaxLength(max)` | Maximum length | `int max, bool allowNull` |
| `HasLengthBetween(min, max)` | Length in range | `int min, int max, bool allowNull` |
| `Matches(pattern)` | Regex pattern match | `string pattern, RegexOptions, bool allowNull` |
| `Contains(substring)` | Contains substring | `string substring, StringComparison, bool allowNull` |
| `StartsWith(prefix)` | Starts with prefix | `string prefix, StringComparison, bool allowNull` |
| `EndsWith(suffix)` | Ends with suffix | `string suffix, StringComparison, bool allowNull` |

### Examples

```csharp
// Email validation with custom messages
builder.AddStringValidation<User>(x => x.Email)
    .IsEmail("Email address is not in valid format")
    .HasMaxLength(255, "Email cannot exceed 255 characters");

// Password validation
builder.AddStringValidation<User>(x => x.Password)
    .HasMinLength(8, "Password must be at least 8 characters")
    .Matches("[A-Z]", RegexOptions.None, false, "Password must contain uppercase letter")
    .Matches("[0-9]", RegexOptions.None, false, "Password must contain digit")
    .Matches("[!@#$%^&*]", RegexOptions.None, false, "Password must contain special character");

// URL validation
builder.AddStringValidation<SocialProfile>(x => x.WebsiteUrl)
    .IsUrl("Website URL must be valid HTTP/HTTPS URL")
    .AllowNull();  // Optional field

// Phone number (numeric only)
builder.AddStringValidation<Contact>(x => x.Phone)
    .IsDigitsOnly("Phone number must contain only digits")
    .HasLengthBetween(10, 15, "Phone number must be 10-15 digits");

// Username validation
builder.AddStringValidation<Account>(x => x.Username)
    .HasMinLength(3, "Username must be at least 3 characters")
    .HasMaxLength(20, "Username must not exceed 20 characters")
    .IsAlphanumeric("Username can only contain letters and digits");
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
| `IsGreaterThan(value)` | Greater than | all numeric |
| `IsGreaterThanOrEqual(value)` | Greater or equal | all numeric |
| `IsLessThan(value)` | Less than | all numeric |
| `IsLessThanOrEqual(value)` | Less or equal | all numeric |
| `IsBetween(min, max)` | In range (inclusive) | all numeric |
| `IsPositive()` | > 0 | all numeric |
| `IsNegative()` | < 0 | all numeric |
| `IsZeroOrPositive()` | >= 0 | all numeric |
| `IsNonZero()` | != 0 | all numeric |
| `IsEven()` | Even number | int, long |
| `IsOdd()` | Odd number | int, long |
| `HasDecimals(max)` | Max decimal places | decimal |
| `IsWhole()` | No decimal places | decimal, double |
| `IsPowerOfTwo()` | Is power of 2 | int, long |

### Examples

```csharp
// Price validation
builder.AddNumericValidation<Product>(x => x.Price)
    .IsGreaterThan(0, "Price must be greater than zero")
    .HasDecimals(2, "Price can have at most 2 decimal places");

// Discount validation
builder.AddNumericValidation<Order>(x => x.Discount)
    .IsBetween(0, 100, "Discount must be between 0 and 100 percent");

// Age validation
builder.AddNumericValidation<Person>(x => x.Age)
    .IsGreaterThanOrEqual(0, "Age cannot be negative")
    .IsLessThan(150, "Age must be less than 150");

// Quantity validation
builder.AddNumericValidation<OrderItem>(x => x.Quantity)
    .IsGreaterThan(0, "Quantity must be at least 1")
    .IsLessThanOrEqual(10000, "Quantity cannot exceed 10000");

// Rating validation
builder.AddNumericValidation<Review>(x => x.Rating)
    .IsBetween(1, 5, "Rating must be between 1 and 5 stars");
```

## DateTime Validation

Validate date and time values:

```csharp
builder.AddDateTimeValidation<Event>(x => x.StartDate)
    .IsInFuture()
    .IsInYear(2024);
```

### Available Validators

| Validator | Purpose |
|-----------|---------|
| `IsInPast()` | Before current date/time |
| `IsInFuture()` | After current date/time |
| `IsInYear(year)` | Within specific year |
| `IsInMonth(year, month)` | Within specific month |
| `IsOnOrAfter(date)` | On or after date |
| `IsOnOrBefore(date)` | On or before date |
| `IsBetween(from, to)` | Within date range |
| `IsWeekday()` | Monday-Friday |
| `IsWeekend()` | Saturday-Sunday |
| `IsUtc()` | UTC timezone |
| `IsLocal()` | Local timezone |
| `IsKind(kind)` | Specific DateTimeKind |

### Examples

```csharp
// Event scheduling validation
builder.AddDateTimeValidation<Event>(x => x.StartTime)
    .IsInFuture("Event must start in the future")
    .IsInYear(2024, "Event must be in 2024");

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
```

## Collection Validation

Validate collections and their elements:

```csharp
builder.AddCollectionValidation<Document>(x => x.Tags)
    .HasMinLength(1)
    .HasMaxLength(10);
```

### Available Validators

| Validator | Purpose |
|-----------|---------|
| `HasMinLength(min)` | Minimum items |
| `HasMaxLength(max)` | Maximum items |
| `HasLengthBetween(min, max)` | Item count in range |
| `NotEmpty()` | Contains at least one item |
| `IsEmpty()` | Contains no items |
| `Contains(item)` | Contains specific item |
| `NotContains(item)` | Does not contain item |
| `AllItemsMatch(predicate)` | All items satisfy condition |
| `AnyItemMatches(predicate)` | At least one item satisfies condition |

### Examples

```csharp
// Tag validation
builder.AddCollectionValidation<Article>(x => x.Tags)
    .HasMinLength(1, "Article must have at least one tag")
    .HasMaxLength(10, "Article cannot have more than 10 tags");

// Category selection
builder.AddCollectionValidation<Product>(x => x.Categories)
    .NotEmpty("Product must have at least one category");

// Email recipients
builder.AddCollectionValidation<EmailMessage>(x => x.Recipients)
    .HasMinLength(1, "Message must have at least one recipient")
    .AllItemsMatch(email => email.Contains("@"), "All recipients must have valid email format");
```

## Custom Messages

All validators support custom error messages:

```csharp
builder.AddStringValidation<User>(x => x.Email)
    .IsEmail("Please provide a valid email address")
    .HasMaxLength(255, "Email address is too long");

builder.AddNumericValidation<Product>(x => x.Price)
    .IsGreaterThan(0, "Products must have a price greater than zero")
    .HasDecimals(2, "Prices can only have up to 2 decimal places");
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

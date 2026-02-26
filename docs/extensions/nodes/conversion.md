---
title: Type Conversion Nodes
description: Convert between types safely with comprehensive error handling.
sidebar_position: 4
---

# Type Conversion Nodes

Type conversion nodes transform items from one type to another. When conversion fails, a `TypeConversionException` is raised with details about the source type, target type, and the value that failed to convert.

The `TypeConversionNode<TIn, TOut>` provides a `WithConverter()` method for custom conversions, and the `TypeConversions` static class provides factory methods for common conversions.

## String to Numeric Conversion

Convert string representations to numeric types:

```csharp
// String to Integer
var stringToIntNode = TypeConversions.StringToInt();
var result = await stringToIntNode.ExecuteAsync("42", context, cancellationToken);
// result = 42

// String to Double
var stringToDoubleNode = TypeConversions.StringToDouble();
var result = await stringToDoubleNode.ExecuteAsync("42.5", context, cancellationToken);
// result = 42.5

// String to Decimal
var stringToDecimalNode = TypeConversions.StringToDecimal();
var result = await stringToDecimalNode.ExecuteAsync("42.50", context, cancellationToken);
// result = 42.50m

// String to Long
var stringToLongNode = TypeConversions.StringToLong();
var result = await stringToLongNode.ExecuteAsync("9223372036854775807", context, cancellationToken);
// result = 9223372036854775807L
```

## String to DateTime Conversion

Parse strings to dates:

```csharp
// Parse with default format
var node = TypeConversions.StringToDateTime();
var result = await node.ExecuteAsync("2025-01-15 14:30:00", context, cancellationToken);
// result = DateTime(2025, 1, 15, 14, 30, 0)

// Parse with specific format
var node = TypeConversions.StringToDateTime(
    format: "yyyy-MM-dd",
    formatProvider: CultureInfo.InvariantCulture);
var result = await node.ExecuteAsync("2025-01-15", context, cancellationToken);
// result = DateTime(2025, 1, 15)
```

## String to Boolean Conversion

Convert strings to boolean values with multiple formats supported:

```csharp
var node = TypeConversions.StringToBool();

// Supported true values: "true", "1", "yes", "on"
var result = await node.ExecuteAsync("true", context, cancellationToken);
// result = true

// Supported false values: "false", "0", "no", "off"
var result = await node.ExecuteAsync("no", context, cancellationToken);
// result = false

// Case-insensitive
var result = await node.ExecuteAsync("YES", context, cancellationToken);
// result = true
```

## String to Enum Conversion

Convert strings to enum values:

```csharp
public enum OrderStatus { Pending, Shipped, Delivered }

// Case-insensitive (default)
var node = TypeConversions.StringToEnum<OrderStatus>();
var result = await node.ExecuteAsync("pending", context, cancellationToken);
// result = OrderStatus.Pending

// Case-sensitive
var node = TypeConversions.StringToEnum<OrderStatus>(ignoreCase: false);
var result = await node.ExecuteAsync("Pending", context, cancellationToken);
// result = OrderStatus.Pending

// Invalid value throws TypeConversionException
var result = await node.ExecuteAsync("invalid", context, cancellationToken);
// throws TypeConversionException
```

## Numeric to String Conversion

Format numeric values as strings:

```csharp
// Integer to String
var node = TypeConversions.IntToString();
var result = await node.ExecuteAsync(42, context, cancellationToken);
// result = "42"

// With format specifier
var node = TypeConversions.IntToString("D5");
var result = await node.ExecuteAsync(42, context, cancellationToken);
// result = "00042"

// Double to String
var node = TypeConversions.DoubleToString("F2");
var result = await node.ExecuteAsync(42.567, context, cancellationToken);
// result = "42.57"

// Decimal to String
var node = TypeConversions.DecimalToString("C");
var result = await node.ExecuteAsync(42.50m, context, cancellationToken);
// result = "$42.50" (culture-dependent)
```

## DateTime to String Conversion

Format dates as strings:

```csharp
var dateTime = new DateTime(2025, 1, 15, 14, 30, 0);

// Default format
var node = TypeConversions.DateTimeToString();
var result = await node.ExecuteAsync(dateTime, context, cancellationToken);
// result = "1/15/2025 2:30:00 PM" (culture-dependent)

// Specific format
var node = TypeConversions.DateTimeToString("yyyy-MM-dd HH:mm:ss");
var result = await node.ExecuteAsync(dateTime, context, cancellationToken);
// result = "2025-01-15 14:30:00"
```

## Boolean to String Conversion

Convert boolean values with custom representations:

```csharp
// Default representations
var node = TypeConversions.BoolToString();
var result = await node.ExecuteAsync(true, context, cancellationToken);
// result = "true"

// Custom representations
var node = TypeConversions.BoolToString("yes", "no");
var result = await node.ExecuteAsync(true, context, cancellationToken);
// result = "yes"

// Binary representation
var node = TypeConversions.BoolToString("1", "0");
var result = await node.ExecuteAsync(false, context, cancellationToken);
// result = "0"
```

## Enum to String Conversion

Convert enum values to their string representation:

```csharp
public enum Color { Red, Green, Blue }

var node = TypeConversions.EnumToString<Color>();
var result = await node.ExecuteAsync(Color.Red, context, cancellationToken);
// result = "Red"
```

## Custom Converters

Use custom conversion functions:

```csharp
// Simple custom converter
var node = new TypeConversionNode<string, int>()
    .WithConverter(input => input.Length);
var result = await node.ExecuteAsync("hello", context, cancellationToken);
// result = 5

// Complex custom converter
var node = new TypeConversionNode<string, DateTime>()
    .WithConverter(input =>
    {
        var parts = input.Split('/');
        if (parts.Length != 3)
            throw new TypeConversionException(typeof(string), typeof(DateTime), input, "Invalid format");
        
        return new DateTime(
            int.Parse(parts[2]),  // year
            int.Parse(parts[0]),  // month
            int.Parse(parts[1])); // day
    });
var result = await node.ExecuteAsync("01/15/2025", context, cancellationToken);
// result = DateTime(2025, 1, 15)
```

## Culture-Aware Conversions

Use specific cultures for culture-sensitive conversions:

```csharp
var germanCulture = new CultureInfo("de-DE");

// German uses comma as decimal separator
var node = TypeConversions.StringToDouble(
    NumberStyles.Float | NumberStyles.AllowThousands,
    germanCulture);
var result = await node.ExecuteAsync("42,5", context, cancellationToken);
// result = 42.5

// Format output with culture
var node = TypeConversions.DoubleToString("F2", germanCulture);
var result = await node.ExecuteAsync(42.567, context, cancellationToken);
// result = "42,57" (German decimal separator)
```

## Error Handling

Type conversion exceptions provide detailed error information:

```csharp
try
{
    var node = TypeConversions.StringToInt();
    var result = await node.ExecuteAsync("not a number", context, cancellationToken);
}
catch (TypeConversionException ex)
{
    Console.WriteLine($"Source Type: {ex.SourceType.Name}");    // "String"
    Console.WriteLine($"Target Type: {ex.TargetType.Name}");    // "Int32"
    Console.WriteLine($"Value: {ex.Value}");                     // "not a number"
    Console.WriteLine($"Message: {ex.Message}");                 // "Cannot convert 'not a number' to int."
}
```

## Pipeline Integration

Add type conversion nodes to your pipeline:

```csharp
var builder = new PipelineBuilder();

// Create a CSV import scenario
var pipeline = builder
    .AddSource<CsvRow>(csvRows)
    .AddStringCleansing<CsvRow>(name => name)
        .Trim()
        .ToLower()
    .AddTypeConversion<CsvRow, ImportedRecord>()  // Note: needs .WithConverter()
    .AddSink<ImportedRecord>(database)
    .Build();

// For type-changing conversions, use custom converter
var node = new TypeConversionNode<CsvRow, ImportedRecord>()
    .WithConverter(row => new ImportedRecord
    {
        Id = int.Parse(row.IdString),
        Amount = decimal.Parse(row.AmountString),
        Date = DateTime.Parse(row.DateString)
    });
```

## Complete Example: Data Import with Validation

```csharp
public class ImportPipeline
{
    public async Task ImportAsync(List<string> csvLines)
    {
        var converter = new TypeConversionNode<string, ImportRecord>()
            .WithConverter(line =>
            {
                var parts = line.Split(',');
                return new ImportRecord
                {
                    Id = int.Parse(parts[0]),
                    Name = parts[1].Trim(),
                    Amount = decimal.Parse(parts[2]),
                    Date = DateTime.Parse(parts[3], CultureInfo.InvariantCulture)
                };
            });

        foreach (var line in csvLines)
        {
            try
            {
                var record = await converter.ExecuteAsync(line, PipelineContext.Default, CancellationToken.None);
                await ProcessRecord(record);
            }
            catch (TypeConversionException ex)
            {
                Console.WriteLine($"Error converting line: {ex.Message}");
                // Log error and continue
            }
        }
    }

    private async Task ProcessRecord(ImportRecord record)
    {
        // Process validated, converted record
        Console.WriteLine($"Processing: {record.Name} - {record.Amount:C}");
        await Task.Delay(100);
    }

    private class ImportRecord
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public decimal Amount { get; set; }
        public DateTime Date { get; set; }
    }
}
```

## Supported Conversions

| From | To | Factory Method | Notes |
|------|----|---------| ------|
| `string` | `int` | `TypeConversions.StringToInt()` | Supports number styles and format providers |
| `string` | `long` | `TypeConversions.StringToLong()` | Supports number styles and format providers |
| `string` | `double` | `TypeConversions.StringToDouble()` | Supports number styles and format providers |
| `string` | `decimal` | `TypeConversions.StringToDecimal()` | Supports number styles and format providers |
| `string` | `bool` | `TypeConversions.StringToBool()` | Supports: true/false, yes/no, on/off, 1/0 |
| `string` | `DateTime` | `TypeConversions.StringToDateTime()` | Supports format specifiers and format providers |
| `string` | `TEnum` | `TypeConversions.StringToEnum<TEnum>()` | Case-sensitive or insensitive |
| `int` | `string` | `TypeConversions.IntToString()` | Supports format specifiers |
| `double` | `string` | `TypeConversions.DoubleToString()` | Supports format specifiers |
| `decimal` | `string` | `TypeConversions.DecimalToString()` | Supports format specifiers |
| `DateTime` | `string` | `TypeConversions.DateTimeToString()` | Supports format specifiers |
| `bool` | `string` | `TypeConversions.BoolToString()` | Customizable true/false representations |
| `TEnum` | `string` | `TypeConversions.EnumToString<TEnum>()` | Uses enum's ToString() |
| Custom | Custom | `new TypeConversionNode<TIn, TOut>().WithConverter()` | Custom conversion function |

## Edge Cases

The type conversion nodes handle several edge cases:

```csharp
// Null or empty strings
var node = TypeConversions.StringToInt();
// Empty string throws: TypeConversionException
// Null string throws: TypeConversionException

// Infinity and NaN
var node = TypeConversions.StringToDouble();
var infinity = await node.ExecuteAsync("Infinity", context, cancellationToken);
// result = double.PositiveInfinity

var nan = await node.ExecuteAsync("NaN", context, cancellationToken);
// result = double.NaN

// Min and Max DateTime values
var node = TypeConversions.DateTimeToString();
var minValue = await node.ExecuteAsync(DateTime.MinValue, context, cancellationToken);
// result = formatted DateTime.MinValue string
```

## Performance Considerations

- **Compiled Expressions**: Factory methods use compiled functions for optimal performance
- **No Allocations**: ValueTask support ensures synchronous conversions don't allocate
- **Format Providers**: Specify format providers only when culture-specific behavior is needed
- **Error Overhead**: Throw TypeConversionException only on actual conversion failures
- **Reuse Nodes**: Create converter nodes once and reuse for multiple conversions

## Testing Type Conversions

```csharp
[Fact]
public async Task StringToInt_WithValidNumber_Converts()
{
    var node = TypeConversions.StringToInt();
    var result = await node.ExecuteAsync("42", PipelineContext.Default, CancellationToken.None);
    Assert.Equal(42, result);
}

[Fact]
public async Task StringToInt_WithInvalidInput_ThrowsTypeConversionException()
{
    var node = TypeConversions.StringToInt();
    var ex = await Assert.ThrowsAsync<TypeConversionException>(() =>
        node.ExecuteAsync("not a number", PipelineContext.Default, CancellationToken.None));
    
    Assert.Equal(typeof(string), ex.SourceType);
    Assert.Equal(typeof(int), ex.TargetType);
}
```

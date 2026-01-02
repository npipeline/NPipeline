---
title: Type Conversion Nodes
description: Convert between types safely with fallback defaults.
sidebar_position: 4
---

# Type Conversion Nodes

Type conversion nodes safely convert between different types. When conversion fails, a `TypeConversionException` is raised with details about the source type, target type, and the value that failed to convert.

## String to Numeric Conversion

Convert string representations to numeric types:

```csharp
builder.AddTypeConversion<ImportRow, Data>()
    .Map(x => x.Amount, x => decimal.Parse(x.AmountString))
    .Map(x => x.Quantity, x => int.Parse(x.QuantityString));
```

## String to DateTime Conversion

Parse strings to dates:

```csharp
builder.AddTypeConversion<ImportRow, Record>()
    .Map(x => x.BirthDate, x => DateTime.Parse(x.DateOfBirthString))
    .Map(x => x.CreatedAt, x => DateTime.ParseExact(x.DateString, "yyyy-MM-dd", CultureInfo.InvariantCulture));
```

## With Fallback Defaults

Provide defaults when conversion fails:

```csharp
builder.AddTypeConversion<ImportRow, Data>()
    .MapWithDefault(
        x => x.Amount,
        x => decimal.Parse(x.AmountString),
        fallback: 0m)
    .MapWithDefault(
        x => x.Quantity,
        x => int.Parse(x.QuantityString),
        fallback: 1);
```

## String to Enum Conversion

Convert strings to enum values:

```csharp
builder.AddTypeConversion<ImportRow, Order>()
    .Map(x => x.Status, x => Enum.Parse<OrderStatus>(x.StatusString))
    .Map(x => x.Priority, x => Enum.Parse<PriorityLevel>(x.PriorityString, ignoreCase: true));

// With fallback
builder.AddTypeConversion<ImportRow, Order>()
    .MapWithDefault(
        x => x.Status,
        x => Enum.Parse<OrderStatus>(x.StatusString),
        fallback: OrderStatus.Pending);
```

## Numeric Type Conversions

Convert between numeric types:

```csharp
builder.AddTypeConversion<Data, Report>()
    .Map(x => x.TotalAmount, x => (decimal)x.TotalInt)
    .Map(x => x.AverageScore, x => (double)x.ScoreInt / 100)
    .Map(x => x.Percentage, x => (float)x.Count / x.Total * 100);
```

## Try-Parse Pattern

Use TryParse methods for safe conversion:

```csharp
builder.AddTypeConversion<ImportRow, Data>()
    .MapWithDefault(
        x => x.Amount,
        x => decimal.TryParse(x.AmountString, out var result) ? result : throw new FormatException(),
        fallback: 0m)
    .MapWithDefault(
        x => x.Date,
        x => DateTime.TryParse(x.DateString, out var result) ? result : throw new FormatException(),
        fallback: DateTime.MinValue);
```

## Complex Conversions

Perform complex transformation logic:

```csharp
builder.AddTypeConversion<SourceData, TargetData>()
    .Map(x => x.FullName, x => $"{x.FirstName} {x.LastName}".Trim())
    .Map(x => x.Age, x => DateTime.Today.Year - DateTime.Parse(x.BirthDate).Year)
    .Map(x => x.IsAdult, x => (DateTime.Today.Year - DateTime.Parse(x.BirthDate).Year) >= 18)
    .Map(x => x.FormattedPrice, x => decimal.Parse(x.PriceString).ToString("C"));
```

## Nullable Type Handling

Handle nullable types safely:

```csharp
builder.AddTypeConversion<ImportRow, Data>()
    .Map(x => x.OptionalAmount, x => 
        string.IsNullOrEmpty(x.AmountString) 
            ? (decimal?)null 
            : decimal.Parse(x.AmountString))
    .Map(x => x.OptionalDate, x => 
        string.IsNullOrEmpty(x.DateString) 
            ? (DateTime?)null 
            : DateTime.Parse(x.DateString));
```

## Collection Conversions

Convert collections to different types:

```csharp
builder.AddTypeConversion<ImportRow, Data>()
    .Map(x => x.Tags, x => x.TagsString.Split(',').Select(t => t.Trim()).ToList())
    .Map(x => x.Values, x => x.ValuesString.Split(';').Select(decimal.Parse).ToArray());
```

## Error Handling

Type conversion exceptions provide detailed error information:

```csharp
try
{
    await pipeline.ExecuteAsync();
}
catch (TypeConversionException ex)
{
    Console.WriteLine($"Source Type: {ex.SourceType.Name}");
    Console.WriteLine($"Target Type: {ex.TargetType.Name}");
    Console.WriteLine($"Value: {ex.Value}");
    Console.WriteLine($"Message: {ex.Message}");
}
```

## Complete Example: CSV Import Pipeline

```csharp
public class CsvImportPipeline
{
    public async Task ImportAsync(string filePath)
    {
        var builder = new PipelineBuilder();

        // Read CSV
        var source = builder.AddCsvSource<CsvRow>(filePath);

        // Cleanse strings
        var cleanse = builder.AddStringCleansing<CsvRow>(x => x.Name)
            .Trim()
            .ToTitleCase();

        // Validate data
        var validate = builder.AddStringValidation<CsvRow>(x => x.Email)
            .IsEmail();

        // Convert types
        var convert = builder.AddTypeConversion<CsvRow, ImportedRecord>()
            .Map(x => x.Name, x => x.Name)
            .MapWithDefault(x => x.Amount, x => decimal.Parse(x.AmountString), fallback: 0m)
            .MapWithDefault(x => x.Date, x => DateTime.Parse(x.DateString), fallback: DateTime.MinValue);

        // Store results
        var sink = builder.AddDatabaseSink<ImportedRecord>(connection);

        // Connect nodes
        builder.Connect(source, cleanse);
        builder.Connect(cleanse, validate);
        builder.Connect(validate, convert);
        builder.Connect(convert, sink);

        // Execute
        var pipeline = builder.Build();
        var result = await pipeline.ExecuteAsync();

        Console.WriteLine($"Processed: {result.ItemsProcessed}");
        Console.WriteLine($"Failed: {result.ErrorCount}");
    }
}

private class CsvRow
{
    public string Name { get; set; }
    public string Email { get; set; }
    public string AmountString { get; set; }
    public string DateString { get; set; }
}

private class ImportedRecord
{
    public string Name { get; set; }
    public string Email { get; set; }
    public decimal Amount { get; set; }
    public DateTime Date { get; set; }
}
```

## Culture-Aware Conversions

Use specific cultures for conversions:

```csharp
var germanCulture = CultureInfo.GetCultureInfo("de-DE");

builder.AddTypeConversion<ImportRow, Data>()
    .Map(x => x.Amount, x => 
        decimal.Parse(x.AmountString, germanCulture))
    .Map(x => x.Date, x => 
        DateTime.ParseExact(x.DateString, "dd.MM.yyyy", germanCulture));
```

## Performance Considerations

- **Compiled Conversions**: Use compiled expressions for fastest conversions
- **Fallback Strategy**: Fallback defaults avoid exception overhead for expected failures
- **TryParse Pattern**: Prefer TryParse when conversion failure is common
- **Lazy Conversion**: Only convert properties that are needed
- **Batch Processing**: Conversion overhead is amortized over large batches

## Type Conversion Patterns

### Pattern 1: Direct Conversion

```csharp
.Map(x => x.IntValue, x => int.Parse(x.StringValue))
```

### Pattern 2: With Fallback

```csharp
.MapWithDefault(x => x.IntValue, x => int.Parse(x.StringValue), fallback: 0)
```

### Pattern 3: Conditional Conversion

```csharp
.Map(x => x.Value, x => 
    string.IsNullOrEmpty(x.StringValue) 
        ? null 
        : int.Parse(x.StringValue))
```

### Pattern 4: Safe Conversion

```csharp
.MapWithDefault(x => x.Value,
    x => int.TryParse(x.StringValue, out var val) ? val : throw new FormatException(),
    fallback: 0)
```

## Combining with Other Nodes

```csharp
var builder = new PipelineBuilder();

// Cleanse first
var cleanse = builder.AddStringCleansing<ImportRow>(x => x.AmountString)
    .Trim()
    .RemoveSpecialCharacters();

// Validate format
var validate = builder.AddStringValidation<ImportRow>(x => x.AmountString)
    .IsNumeric("Amount must be numeric");

// Convert type
var convert = builder.AddTypeConversion<ImportRow, Data>()
    .Map(x => x.Amount, x => decimal.Parse(x.AmountString));

// Connect
builder.Connect(cleanse, validate);
builder.Connect(validate, convert);
```

## Testing Type Conversions

```csharp
[Fact]
public async Task ConversionPipeline_ShouldConvertStringToDecimal()
{
    // Arrange
    var item = new ImportRow { AmountString = "123.45" };
    var converter = new TypeConversionNode<ImportRow, Data>();
    converter.Map(x => x.Amount, x => decimal.Parse(x.AmountString));

    // Act
    var result = await converter.ExecuteAsync(item, context);

    // Assert
    result.Amount.Should().Be(123.45m);
}

[Fact]
public async Task ConversionPipeline_ShouldUseFallbackOnParseError()
{
    // Arrange
    var item = new ImportRow { AmountString = "invalid" };
    var converter = new TypeConversionNode<ImportRow, Data>();
    converter.MapWithDefault(
        x => x.Amount,
        x => decimal.Parse(x.AmountString),
        fallback: 0m);

    // Act
    var result = await converter.ExecuteAsync(item, context);

    // Assert
    result.Amount.Should().Be(0m);
}
```

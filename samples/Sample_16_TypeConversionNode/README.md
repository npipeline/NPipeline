# TypeConversionNode Sample

This sample demonstrates comprehensive **TypeConversionNode\<TIn, TOut\>** functionality in NPipeline for seamless data type transformations between different
stages of your pipeline.

## Overview

TypeConversionNode provides a powerful and flexible way to transform items between types using a fluent mapping API. This sample showcases real-world scenarios
where type conversion is essential for data integration, legacy system modernization, and API development.

## Key Concepts Demonstrated

### TypeConversionNode Features

- **AutoMap()**: Automatic property mapping by name (case-insensitive)
- **Custom Map()**: Explicit property-to-property mapping with custom converters
- **Factory Functions**: Custom object creation for complex initialization
- **Error Handling**: Graceful handling of conversion failures with fallbacks
- **Performance Optimization**: Compiled expressions for high-throughput scenarios
- **Record Support**: Built-in support for C# record types

### Conversion Scenarios

1. **String Parsing**: Converting CSV/log data to strongly-typed objects
2. **JSON Deserialization**: Extracting data from complex JSON structures
3. **Legacy Integration**: Modernizing old system data formats
4. **API Formatting**: Converting domain objects to DTOs for external consumption
5. **Business Logic Integration**: Adding validation and enrichment during conversion
6. **Error Monitoring**: Comprehensive error handling and analysis

## Architecture

The sample implements three parallel data processing paths:

### String Data Path

```
RawStringData → SensorData → SensorReading → SensorDto
```

- Simulates CSV file or log data ingestion
- Demonstrates string parsing with validation
- Shows error handling for malformed data

### JSON Data Path

```
JsonStringData → SensorData → SensorReading → SensorDto
```

- Simulates API or message queue integration
- Demonstrates complex JSON structure parsing
- Shows business logic during transformation

### Legacy Data Path

```
LegacySensorFormat → CanonicalSensorData
```

- Simulates legacy system integration
- Demonstrates naming convention changes
- Shows enterprise data patterns

## Real-World Scenarios

### 1. Data Ingestion from External Systems

```csharp
// Converting CSV data to strongly-typed objects
var converter = new TypeConversionNode<RawStringData, SensorData>()
    .AutoMap() // Match properties by name
    .Map(src => src.Timestamp, dst => dst.Timestamp, ParseTimestamp)
    .Map(src => src.Temperature, dst => dst.Temperature, ParseDouble);
```

### 2. API Response Formatting

```csharp
// Converting domain objects to API DTOs with snake_case
var apiFormatter = new TypeConversionNode<SensorReading, SensorDto>()
    .Map(src => src.Id, dst => dst.sensor_id, id => id.ToString("N"))
    .Map(src => src.Temperature, dst => dst.temperature_celsius, temp => temp.ToString("F2"));
```

### 3. Legacy System Modernization

```csharp
// Converting legacy format to modern canonical format
var modernizer = new TypeConversionNode<LegacySensorFormat, CanonicalSensorData>()
    .Map(src => src.SENSOR_ID, dst => dst.SensorId, ParseLegacyId)
    .Map(src => src.TEMP_VAL, dst => dst.TemperatureCelsius, NormalizeTemperature);
```

## Performance Considerations

### Expression Compilation

TypeConversionNode compiles all mapping expressions to high-performance delegates:

- **Zero Reflection**: No reflection during execution
- **Cached Accessors**: Property accessors compiled once and reused
- **Optimized Conversions**: Type-specific conversion logic

### Memory Efficiency

- **Minimal Allocations**: Reuses compiled delegates
- **Stream Processing**: Designed for high-throughput scenarios
- **Error Batching**: Efficient error collection and reporting

## Error Handling Patterns

### Validation with Fallbacks

```csharp
var converter = new TypeConversionNode<RawStringData, SensorData>()
    .Map(src => src.Temperature, dst => dst.Temperature, tempStr =>
        double.TryParse(tempStr, out var temp) ? temp : 0.0); // Fallback value
```

### Error Collection and Analysis

The sample includes comprehensive error monitoring:

- **Error Categorization**: Groups errors by type and cause
- **Success Rate Tracking**: Monitors conversion success rates
- **Performance Metrics**: Tracks throughput and latency
- **Recommendations**: Provides actionable insights

## Usage Examples

### Basic AutoMap Usage

```csharp
// Simple automatic mapping for matching properties
var simpleConverter = new TypeConversionNode<Source, Destination>()
    .AutoMap(); // Maps all matching properties automatically
```

### Complex Business Logic

```csharp
// Complex transformation using whole input object
var businessConverter = new TypeConversionNode<OrderInput, OrderOutput>()
    .Map(src => src.LineItems, dst => dst.TotalAmount,
        input => input.LineItems.Sum(item => item.Quantity * item.Price))
    .Map(dst => dst.ProcessedAt, _ => DateTime.UtcNow);
```

### Custom Factory for Records

```csharp
// Custom factory for record types with required parameters
var recordConverter = new TypeConversionNode<Source, DestinationRecord>(
    factory: input => new DestinationRecord(
        Id: input.Id,
        CreatedAt: DateTime.UtcNow, // Default value
        Status: "Active" // Default value
    )
)
    .Map(src => src.Name, dst => dst.DisplayName, name => name.ToUpper());
```

## Testing

The sample includes comprehensive tests using NPipeline.Extensions.Testing:

### Unit Tests

- Individual TypeConversionNode testing
- Edge case and boundary condition testing
- Error handling validation
- Performance benchmarking

### Integration Tests

- End-to-end pipeline testing
- Multiple data source testing
- Error scenario testing

### Running Tests

```bash
dotnet test --project Sample_16_TypeConversionNode.csproj
```

## Configuration

### Pipeline Parameters

- **EnableErrorHandling**: Enable/disable error demonstrations
- **RecordCount**: Number of records to process per source

### Source Configuration

- **String Source**: Interval and record count
- **JSON Source**: Interval and complexity level
- **Legacy Source**: Format variations and error rates

## Best Practices

### 1. Use AutoMap() for Simple Cases

```csharp
// Good: Automatic mapping for matching properties
var converter = new TypeConversionNode<Source, Destination>()
    .AutoMap();
```

### 2. Custom Map() for Complex Logic

```csharp
// Good: Custom mapping with business logic
var converter = new TypeConversionNode<Source, Destination>()
    .Map(src => src.Value, dst => dst.CalculatedValue,
        (input, value) => value * input.Multiplier + input.Offset);
```

### 3. Handle Errors Gracefully

```csharp
// Good: Provide fallbacks for invalid data
.Map(src => src.Amount, dst => dst.ProcessedAmount,
    amount => decimal.TryParse(amount, out var result) ? result : 0m);
```

### 4. Validate Input Data

```csharp
// Good: Validate during conversion
.Map(dst => dst.IsValid, input => ValidateInput(input));
```

## Common Pitfalls

### 1. Not Handling Null Values

```csharp
// Bad: Throws on null
.Map(src => src.Value, dst => dst.Destination, value => value.ToString());

// Good: Handle null gracefully
.Map(src => src.Value, dst => dst.Destination, value => value?.ToString() ?? "default");
```

### 2. Inconsistent Naming

```csharp
// Bad: Manual mapping for all properties
.Map(src => src.FirstName, dst => dst.FirstName, name => name)
.Map(src => src.LastName, dst => dst.LastName, name => name);

// Good: Use AutoMap() for matching names
.AutoMap()
.Map(src => src.FirstName, dst => dst.FullName, name => $"{name} Smith");
```

### 3. Complex Logic in Converters

```csharp
// Bad: Complex inline logic
.Map(src => src.Data, dst => dst.Result, data => {
    // Complex logic here
    return ProcessComplexData(data);
});

// Good: Extract to helper methods
.Map(src => src.Data, dst => dst.Result, ProcessComplexData);
```

## Performance Tips

### 1. Minimize Object Creation

```csharp
// Good: Reuse factory for consistent object creation
var converter = new TypeConversionNode<Source, Destination>(CreateDestination);
```

### 2. Use Appropriate Types

```csharp
// Good: Choose efficient data types
public record OptimizedDestination(int Value, string Name); // Instead of object
```

### 3. Batch When Possible

```csharp
// Consider batching for high-volume scenarios
// TypeConversionNode works well with BatchingNode for efficiency
```

## Running the Sample

### Basic Execution

```bash
dotnet run --project Sample_16_TypeConversionNode.csproj
```

### With Custom Parameters

```bash
dotnet run --project Sample_16_TypeConversionNode.csproj -- --record-count 20 --enable-error-handling
```

### Test Execution

```bash
dotnet test --project Sample_16_TypeConversionNode.csproj
```

## Extending the Sample

### Adding New Conversion Types

1. Create new source and destination types
2. Implement TypeConversionNode with appropriate mappings
3. Add to pipeline definition
4. Create corresponding tests

### Custom Converters

```csharp
// Register custom type converters
var factory = TypeConverterFactory.CreateDefault();
factory.Register<CustomType, StandardType>(ConvertCustomToStandard);
```

## Related Documentation

- [Type Conversion Nodes](../../docs/core-concepts/advanced-nodes/type-conversion.md)
- [Pipeline Builder](../../docs/core-concepts/pipeline-builder.md)
- [Error Handling](../../docs/core-concepts/error-handling.md)
- [Testing](../../docs/testing/index.md)

## Conclusion

This sample demonstrates production-ready patterns for type conversion in data pipelines. The TypeConversionNode provides a flexible, performant, and
maintainable solution for transforming data between different formats, making it essential for enterprise integration scenarios.

The key takeaways are:

1. **Use AutoMap() for simple cases** - reduces boilerplate and errors
2. **Custom Map() for complex logic** - provides flexibility for business rules
3. **Handle errors gracefully** - ensures robust production operation
4. **Test thoroughly** - validates conversion logic and edge cases
5. **Monitor performance** - ensures high-throughput operation

These patterns enable seamless integration between disparate systems while maintaining code quality and performance.

# Customer Data Processing Sample

## Overview

This sample demonstrates the **NPipeline.Extensions.Nodes** library, which provides ready-made, high-performance nodes for common data processing tasks like
validation, cleansing, transformation, and enrichment.

## What This Sample Shows

The pipeline processes customer records through several data processing stages:

### 1. **String Cleansing**

- Trims whitespace from Name and Phone fields
- Converts Email to lowercase for consistency

### 2. **DateTime Cleansing**

- Normalizes CreatedDate to UTC timezone
- Handles timezone conversion automatically

### 3. **Data Enrichment**

- Applies default name if missing
- Sets default email for empty records
- Normalizes phone field to "N/A" if missing

### 4. **Custom Validation**

- Ensures required fields (Name, Email) are not empty
- Validates email format (contains @)
- Enforces age constraints (18-120 years old)
- Prevents negative account balances

### 5. **Filtering**

- Rejects accounts with negative balances
- Filters out records without valid email addresses

## Key Features Demonstrated

- **Zero-allocation hot paths**: Most operations are optimized for performance
- **Compiled property access**: Expressions are compiled once at configuration time
- **Fluent builder API**: Clean, chainable method calls for configuration
- **Custom validation nodes**: Extending `ValidationNode<T>` for domain-specific rules
- **Error handling integration**: Automatic error handler registration with configurable strategies
- **Real-world data cleaning**: A practical example of cleaning and validating customer data

## Sample Data

The pipeline processes 4 sample customers with various data quality issues:

1. **John Doe** - Has trailing whitespace and uppercase email (cleansed)
2. **Jane Smith** - Normal data (passes through pipeline)
3. **Bob Wilson** - Clean data (processes successfully)
4. **Alice Brown** - Clean data with multiple tags (processes successfully)

## Running the Sample

```bash
cd samples/Sample_NodesExtension
dotnet run
```

## Output Example

The sample displays:

- Original sample data
- Processing results for each customer
- Summary statistics (passed, failed, skipped)
- Details about why records failed or were skipped

## Key Nodes Used

- `AddStringCleansing<T>()` - Clean string properties
- `AddDateTimeCleansing<T>()` - Normalize dates
- `AddEnrichment<T>()` - Add computed values and defaults
- `AddValidationNode<T, TValidator>()` - Custom validation
- `AddFilteringNode<T>()` - Filter items with predicates

## Learning Path

1. Start with the `CustomerProcessingPipeline.cs` to understand the pipeline structure
2. Review `Nodes.cs` to see custom node implementations
3. Look at `Program.cs` to understand execution and result handling
4. Experiment by modifying the sample data or adding more validation rules

## Further Reading

For more detailed information about the Nodes extension:

- [Nodes Extension Documentation](../../docs/extensions/nodes/index.md)
- [Data Cleansing Guide](../../docs/extensions/nodes/cleansing.md)
- [Data Validation Guide](../../docs/extensions/nodes/validation.md)
- [Enrichment Guide](../../docs/extensions/nodes/enrichment.md)

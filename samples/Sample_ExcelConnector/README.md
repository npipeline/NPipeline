# Sample 12: Excel Connector

This sample demonstrates comprehensive Excel data processing using NPipeline's Excel connector components. It shows how to read data from a source Excel file,
validate it, transform it, and then write it to a target Excel file.

## Overview

The Excel Connector sample implements a complete data processing pipeline that:

1. **Reads** customer data from an Excel file using `ExcelSourceNode<T>`
2. **Validates** each customer record using a custom `ValidationTransform`
3. **Transforms** and enriches the data using a custom `DataTransform`
4. **Writes** the processed data to a new Excel file using `ExcelSinkNode<T>`

## Key Concepts Demonstrated

### Excel Connector Components

- **ExcelSourceNode<T>**: Reads Excel data and deserializes it to strongly-typed objects using ExcelDataReader
- **ExcelSinkNode<T>**: Serializes objects to Excel format and writes to files using DocumentFormat.OpenXml
- **StorageUri**: Abstracts file system access for consistent storage handling

### Data Validation

- Custom validation logic for business rules
- Configurable filtering of invalid records
- Comprehensive error reporting and logging
- Validation for various data types (string, int, decimal, DateTime, bool, double, long)

### Data Transformation

- Data normalization (name formatting, country codes)
- Data enrichment (calculated fields, derived properties)
- Flexible transformation pipeline
- Support for multiple data types

### Error Handling

- Graceful handling of malformed Excel data
- Pipeline-level error capture and reporting
- Configurable error handling strategies

## Project Structure

```
Sample_ExcelConnector/
├── Data/
│   ├── customers.xlsx                  # Sample input data with valid and invalid records
│   └── processed_customers.xlsx      # Generated output file (created by pipeline)
├── Nodes/
│   ├── ValidationTransform.cs          # Custom validation transform node
│   └── DataTransform.cs              # Custom data transformation node
├── Customer.cs                      # Customer data model with various data types
├── ExcelConnectorPipeline.cs         # Main pipeline definition
├── Program.cs                       # Entry point and execution logic
├── Sample_ExcelConnector.csproj     # Project configuration
└── README.md                        # This documentation
```

## Running the Sample

### Prerequisites

- .NET 10.0 SDK
- The NPipeline solution built

### Execution

1. Navigate to the sample directory:

   ```bash
   cd samples/Sample_ExcelConnector
   ```

2. Build and run the sample:

   ```bash
   dotnet run
   ```

### Expected Output

The pipeline will:

1. Read 17 customer records from `Data/customers.xlsx` (10 valid, 7 invalid)
2. Validate each record (filtering out invalid ones)
3. Transform the valid records (normalize names, countries, etc.)
4. Write the processed records to `Data/processed_customers.xlsx`

You should see output similar to:

```
=== NPipeline Sample: Excel Connector ===

Registered NPipeline services and scanned assemblies for nodes.

Pipeline Description:
[Detailed pipeline description...]

Pipeline Parameters:
  Source Path: Data/customers.xlsx
  Target Path: Data/processed_customers.xlsx

Starting pipeline execution...

Pipeline execution completed successfully!

Output file created: Data/processed_customers.xlsx
File size: 10240 bytes
Created: 2023-11-22 10:30:45
```

## Sample Data

### Input Data (customers.xlsx)

The sample input contains 17 records including:

- **Valid records**: Complete customer data with proper formatting
- **Invalid records**: Missing fields, invalid emails, out-of-range ages, negative balances, etc.

The data demonstrates various data types:

- `Id` (int): Customer identifier
- `FirstName`, `LastName`, `Email` (string): Text fields
- `Age` (int): Numeric field
- `RegistrationDate` (DateTime): Date field
- `Country` (string): Text field
- `AccountBalance` (decimal): Financial data
- `IsPremiumMember` (bool): Boolean flag
- `DiscountPercentage` (double): Percentage value
- `LoyaltyPoints` (long): Large integer value

### Output Data (processed_customers.xlsx)

The output contains only valid records with:

- **Normalized names**: Proper capitalization (John Doe instead of john doe)
- **Normalized countries**: Full country names (United States instead of USA)
- **Lowercase emails**: Consistent email formatting
- **Filtered invalid records**: Only valid customer data is included
- **All data types preserved**: Proper type mapping for all fields

## Configuration

### Pipeline Parameters

The pipeline accepts the following parameters:

| Parameter    | Description                   | Default Value                   |
|--------------|-------------------------------|---------------------------------|
| `SourcePath` | Path to the input Excel file  | `Data/customers.xlsx`           |
| `TargetPath` | Path to the output Excel file | `Data/processed_customers.xlsx` |

### Validation Rules

The `ValidationTransform` enforces these rules:

- **ID**: Must be greater than 0
- **Name**: First and last name are required
- **Email**: Required and must match email format regex
- **Age**: Must be between 0 and 150
- **Registration Date**: Required and cannot be in the future
- **Country**: Required and non-empty
- **Account Balance**: Cannot be negative
- **Discount Percentage**: Must be between 0 and 100
- **Loyalty Points**: Cannot be negative

### Transformation Rules

The `DataTransform` applies these transformations:

- **Name Formatting**: Capitalizes first letter, lowercases the rest
- **Email Normalization**: Converts to lowercase
- **Country Normalization**: Expands abbreviations to full names (USA → United States, UK → United Kingdom)
- **Data Trimming**: Removes whitespace from all string fields
- **Discount Normalization**: Ensures discount is within valid bounds (0-100)

## Attribute Mapping

The Excel connector supports attribute-based mapping for precise control over column mappings. This feature allows you to map properties to specific Excel
column names and exclude properties from mapping.

### Convention-Based Mapping (Default)

By default, properties are mapped to Excel columns using lowercase conversion:

```csharp
public class Customer
{
    public int Id,              // Maps to "id"
    public string FirstName,    // Maps to "firstname"
    public string LastName,     // Maps to "lastname"
    public string Email         // Maps to "email"
}
```

### Attribute-Based Mapping

Use attributes to override default mappings:

```csharp
using NPipeline.Connectors.Excel.Attributes;

public class Customer
{
    [ExcelColumn("CustomerID")]
    public int Id { get; set; }

    [ExcelColumn("First Name")]
    public string FirstName { get; set; }

    [ExcelColumn("Last Name")]
    public string LastName { get; set; }

    [ExcelColumn("Email Address")]
    public string Email { get; set; }

    [ExcelIgnore]
    public string InternalNotes { get; set; }
}
```

### Before and After Comparison

#### Without Attributes (Convention-Based)

```csharp
// Model
public class Customer
{
    public int Id { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
}

// Pipeline usage
var sourceNode = new ExcelSourceNode<Customer>(
    StorageUri.FromFilePath(sourcePath),
    row => new Customer
    {
        Id = row.Get("Id", 0),
        FirstName = row.Get("FirstName", string.Empty) ?? string.Empty,
        LastName = row.Get("LastName", string.Empty) ?? string.Empty,
    });
```

**Excel headers must be:** `id`, `firstname`, `lastname`

#### With Attributes (Attribute-Based)

```csharp
// Model with attributes
using NPipeline.Connectors.Excel.Attributes;

public class Customer
{
    [ExcelColumn("CustomerID")]
    public int Id { get; set; }

    [ExcelColumn("First Name")]
    public string FirstName { get; set; }

    [ExcelColumn("Last Name")]
    public string LastName { get; set; }

    [ExcelIgnore]
    public string InternalNotes { get; set; }
}

// Pipeline usage - mapper is automatic
var sourceNode = new ExcelSourceNode<Customer>(
    StorageUri.FromFilePath(sourcePath),
    StorageProviderFactory.CreateResolver()
);
```

**Excel headers can be:** `CustomerID`, `First Name`, `Last Name` (any casing)

### Practical Use Cases

#### Use Case 1: Non-Standard Column Names

When Excel files use column names that don't match your property naming conventions:

```csharp
public class Customer
{
    [ExcelColumn("CUST_ID")]
    public int Id { get; set; }

    [ExcelColumn("FNAME")]
    public string FirstName { get; set; }

    [ExcelColumn("LNAME")]
    public string LastName { get; set; }
}
```

#### Use Case 2: Column Names with Spaces

Excel headers often contain spaces that don't match C# property names:

```csharp
public class Customer
{
    [ExcelColumn("Customer ID")]
    public int Id { get; set; }

    [ExcelColumn("Email Address")]
    public string Email { get; set; }

    [ExcelColumn("Account Balance")]
    public decimal AccountBalance { get; set; }
}
```

#### Use Case 3: Excluding Computed Properties

Exclude properties that shouldn't be read from or written to Excel:

```csharp
public class Customer
{
    [ExcelColumn("Id")]
    public int Id { get; set; }

    [ExcelColumn("FirstName")]
    public string FirstName { get; set; }

    [ExcelColumn("LastName")]
    public string LastName { get; set; }

    // Computed property - not mapped to Excel
    [ExcelIgnore]
    public string FullName => $"{FirstName} {LastName}";
}
```

#### Use Case 4: Mixed Mapping Strategies

Combine convention-based and attribute-based mapping:

```csharp
public class Customer
{
    // Attribute mapping for non-standard columns
    [ExcelColumn("CUST_ID")]
    public int Id { get; set; }

    // Convention mapping for standard columns
    public string FirstName { get; set; }  // Maps to "firstname"
    public string LastName { get; set; }   // Maps to "lastname"

    // Excluded from mapping
    [ExcelIgnore]
    public string InternalId { get; set; }
}
```

### Complete Example with Attributes

```csharp
using NPipeline.Connectors.Excel;
using NPipeline.Connectors.Excel.Attributes;
using NPipeline.Connectors;

// Model with attribute mapping
public class Customer
{
    [ExcelColumn("CustomerID")]
    public int Id { get; set; }

    [ExcelColumn("First Name")]
    public string FirstName { get; set; } = string.Empty;

    [ExcelColumn("Last Name")]
    public string LastName { get; set; } = string.Empty;

    [ExcelColumn("Email Address")]
    public string Email { get; set; } = string.Empty;

    [ExcelColumn("Age")]
    public int Age { get; set; }

    [ExcelColumn("Registration Date")]
    public DateTime RegistrationDate { get; set; }

    [ExcelColumn("Country")]
    public string Country { get; set; } = string.Empty;

    [ExcelColumn("Account Balance")]
    public decimal AccountBalance { get; set; }

    [ExcelColumn("Is Premium")]
    public bool IsPremiumMember { get; set; }

    [ExcelColumn("Discount %")]
    public double DiscountPercentage { get; set; }

    [ExcelColumn("Loyalty Points")]
    public long LoyaltyPoints { get; set; }

    [ExcelIgnore]
    public string InternalNotes { get; set; } = string.Empty;
}

// Pipeline with attribute-based mapping
public class ExcelConnectorPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var resolver = StorageProviderFactory.CreateResolver();
        var sourcePath = GetSourcePath();
        var targetPath = GetTargetPath();

        // Source uses attribute-based mapping automatically
        var sourceNode = new ExcelSourceNode<Customer>(
            StorageUri.FromFilePath(sourcePath),
            resolver
        );

        var source = builder.AddSource(sourceNode, "excel-source");

        // Add validation and transform nodes
        var validation = builder.AddTransform<ValidationTransform, Customer, Customer>("validation-transform");
        var transform = builder.AddTransform<DataTransform, Customer, Customer>("data-transform");

        // Sink writes columns in attribute-defined order
        var sinkNode = new ExcelSinkNode<Customer>(
            StorageUri.FromFilePath(targetPath),
            resolver
        );

        var sink = builder.AddSink(sinkNode, "excel-sink");

        // Connect nodes
        builder.Connect(source, validation);
        builder.Connect(validation, transform);
        builder.Connect(transform, sink);
    }

    private static string GetSourcePath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "customers.xlsx");
    }

    private static string GetTargetPath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "processed_customers.xlsx");
    }

    private static string FindProjectDirectory(string startDirectory)
    {
        var directory = new DirectoryInfo(startDirectory);
        while (directory != null)
        {
            if (directory.GetFiles("*.csproj").FirstOrDefault() != null)
                return directory.FullName;
            directory = directory.Parent;
        }
        return startDirectory;
    }
}
```

### Advanced Features

#### Case-Insensitive Matching

Attribute-based mapping performs case-insensitive column matching:

```csharp
[ExcelColumn("CustomerID")]
public int Id { get; set; }
```

This matches: `CustomerID`, `customerid`, `CUSTOMERID`, `CuStOmErId`, etc.

#### Nullable Types

Nullable types are handled automatically:

```csharp
public class Customer
{
    [ExcelColumn("customer_id")]
    public int? Id { get; set; }

    [ExcelColumn("phone_number")]
    public string? PhoneNumber { get; set; }

    [ExcelColumn("last_order_date")]
    public DateTime? LastOrderDate { get; set; }
}
```

When an Excel column is missing or empty, nullable properties receive `null`.

#### Performance Benefits

Attribute mapping uses compiled expression tree delegates for optimal performance:

- **Compiled delegates** are significantly faster than reflection-based mapping
- **Type safety** with compile-time checking of property mappings
- **Caching** ensures mappers are compiled only once per type

## Common Attributes

NPipeline now supports **common attributes** that work across all connectors (CSV, Excel, PostgreSQL, etc.). This allows you to use the same attributes for
different data sources, making your code more portable and maintainable.

### What Are Common Attributes?

Common attributes are defined in `NPipeline.Connectors.Attributes` namespace and provide a unified way to specify column mappings across all connectors:

- **`ColumnAttribute`**: Specifies the column name for a property
- **`IgnoreColumnAttribute`**: Excludes a property from mapping

### Using Common Attributes

To use common attributes, add a reference to `NPipeline.Connectors` and import the namespace:

```csharp
using NPipeline.Connectors.Attributes;

public class CustomerWithCommonAttributes
{
    [Column("CustomerID")]
    public int Id { get; set; }

    [Column("First Name")]
    public string FirstName { get; set; } = string.Empty;

    [Column("Last Name")]
    public string LastName { get; set; } = string.Empty;

    [Column("Email Address")]
    public string Email { get; set; } = string.Empty;

    [IgnoreColumn]
    public string InternalNotes { get; set; } = string.Empty;

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

### Benefits of Common Attributes

- **Cross-connector compatibility**: Same attributes work with CSV, Excel, PostgreSQL, etc.
- **Simplified code**: Use one set of attributes across different data sources
- **Future-proof**: New connectors will automatically support common attributes
- **Easier migration**: Move data between different sources without changing attribute definitions

### Common vs Connector-Specific Attributes

Both common and connector-specific attributes are fully supported. Choose based on your needs:

| Scenario                                | Recommended Approach                                                |
|-----------------------------------------|---------------------------------------------------------------------|
| Simple column mapping                   | Common attributes (`Column`, `IgnoreColumn`)                        |
| Cross-connector compatibility           | Common attributes (`Column`, `IgnoreColumn`)                        |
| Database-specific features (PostgreSQL) | Connector-specific (`PostgresColumn` with DbType, Size, PrimaryKey) |
| Legacy code with specific attributes    | Keep existing connector-specific attributes                         |

**Example: Using Common Attributes**

```csharp
using NPipeline.Connectors.Attributes;

public class Customer
{
    [Column("CustomerID")]
    public int Id { get; set; }

    [Column("First Name")]
    public string FirstName { get; set; }
}
```

**Example: Using Connector-Specific Attributes (Excel)**

```csharp
using NPipeline.Connectors.Excel.Attributes;

public class Customer
{
    [ExcelColumn("CustomerID")]
    public int Id { get; set; }

    [ExcelColumn("First Name")]
    public string FirstName { get; set; }

    [ExcelIgnore]
    public string InternalNotes { get; set; }
}
```

### Backward Compatibility

Connector-specific attributes (`ExcelColumn`, `ExcelIgnore`, `CsvColumn`, `CsvIgnore`, `PostgresColumn`, `PostgresIgnore`) continue to work exactly as before.
You can:

- Keep existing code using connector-specific attributes
- Mix common and connector-specific attributes in the same project
- Gradually migrate to common attributes at your own pace

### Sample Code

This sample includes both approaches:

- **`Customer`**: Uses convention-based mapping (no attributes required)
- **`CustomerWithCommonAttributes`**: Demonstrates common attributes with detailed comments

Both classes work identically with the Excel connector. The choice of which to use depends on your specific requirements.

## Extending the Sample

### Adding New Validation Rules

Extend the `ValidationTransform.ExecuteAsync` method to add custom validation logic:

```csharp
// Example: Add phone number validation
if (!string.IsNullOrWhiteSpace(input.PhoneNumber) &&
    !PhoneNumberRegex.IsMatch(input.PhoneNumber))
{
    errors.Add($"Invalid phone number format: {input.PhoneNumber}");
}
```

### Adding New Transformations

Extend the `DataTransform.ExecuteAsync` method to add custom transformations:

```csharp
// Example: Add geographic region calculation
transformedCustomer.Region = GetGeographicRegion(transformedCustomer.Country);
```

### Supporting Different Excel Formats

Modify the `Customer` class and update the Excel configuration in the pipeline to support different column names, sheet names, or data formats.

## Technical Details

### Excel Libraries Used

This sample uses the following libraries for Excel processing:

- **ExcelDataReader** (v3.8.0): Used by `ExcelSourceNode<T>` to read both XLS (binary) and XLSX (Open XML) formats
    - Provides streaming access to Excel data
    - Supports configurable options for sheet selection, header handling, and type conversion
    - Handles both legacy and modern Excel formats

- **DocumentFormat.OpenXml** (v3.1.0): Used by `ExcelSinkNode<T>` to write XLSX (Open XML) format
    - Provides native .NET support for creating Excel files
    - Generates standards-compliant XLSX files
    - Note: Only supports writing XLSX format, not legacy XLS

### Type Mapping

The Excel connector supports automatic type mapping for:

- **Primitive types**: `int`, `long`, `short`, `float`, `double`, `decimal`
- **String types**: `string`
- **Boolean types**: `bool`
- **Date types**: `DateTime`
- **Other types**: `Guid`
- **Nullable versions** of all above types

### Storage Abstraction

The sample uses `FileSystemStorageProvider` through the `StorageUri` abstraction, which provides:

- Consistent file access across different storage providers
- Support for both local files and remote storage (when configured)
- Type-safe URI handling for Excel files

## Best Practices Demonstrated

1. **Separation of Concerns**: Each node has a single responsibility
2. **Type Safety**: Strongly-typed data models prevent runtime errors
3. **Error Handling**: Comprehensive validation and graceful error handling
4. **Configurability**: Pipeline behavior can be configured through parameters
5. **Testability**: All components are easily testable in isolation
6. **Logging**: Appropriate logging for monitoring and debugging
7. **Resource Management**: Proper disposal of file handles and streams
8. **Data Type Support**: Demonstrates handling of various data types in Excel

## Dependencies

This sample uses the following NPipeline packages:

- `NPipeline`: Core pipeline framework
- `NPipeline.Connectors.Excel`: Excel source and sink nodes
- `NPipeline.Connectors`: Storage abstractions
- `NPipeline.Extensions.DependencyInjection`: DI container integration

External dependencies:

- `ExcelDataReader` (v3.8.0): Excel file reading
- `ExcelDataReader.DataSet` (v3.8.0): DataSet integration for ExcelDataReader
- `DocumentFormat.OpenXml` (v3.1.0): Excel file writing (Open XML format)
- `System.IO.Packaging` (v8.0.1): Packaging support for Open XML
- `Microsoft.Extensions.Hosting` (v10.0.1): Host application framework

## Notes

- The Excel connector supports reading both XLS (binary) and XLSX (Open XML) formats
- Writing is only supported for XLSX (Open XML) format
- The sample demonstrates validation and filtering of invalid records
- All data types are properly mapped between Excel cells and .NET properties
- The pipeline uses streaming for reading to handle large files efficiently

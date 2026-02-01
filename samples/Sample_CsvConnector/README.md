# Sample 11: CSV Connector

This sample demonstrates comprehensive CSV data processing using NPipeline's CSV connector components. It shows how to read data from a source CSV file,
validate it, transform it, and then write it to a target CSV file.

## Overview

The CSV Connector sample implements a complete data processing pipeline that:

1. **Reads** customer data from a CSV file using `CsvSourceNode<T>`
2. **Validates** each customer record using a custom `ValidationTransform`
3. **Transforms** and enriches the data using a custom `DataTransform`
4. **Writes** the processed data to a new CSV file using `CsvSinkNode<T>`

## Key Concepts Demonstrated

### CSV Connector Components

- **CsvSourceNode<T>**: Reads CSV data and deserializes it to strongly-typed objects
- **CsvSinkNode<T>**: Serializes objects to CSV format and writes to files
- **StorageUri**: Abstracts file system access for consistent storage handling

### Data Validation

- Custom validation logic for business rules
- Configurable filtering of invalid records
- Comprehensive error reporting and logging

### Data Transformation

- Data normalization (name formatting, country codes)
- Data enrichment (calculated fields, derived properties)
- Flexible transformation pipeline

### Error Handling

- Graceful handling of malformed CSV data
- Pipeline-level error capture and reporting
- Configurable error handling strategies

## Attribute-Based Mapping

The CSV connector supports attribute-based mapping, allowing you to control how properties map to CSV columns without writing custom mapper functions.

### Without Attributes (Convention-Based)

By default, properties are mapped using lowercase conversion:

```csharp
public class Customer
{
    public int Id { get; set; }              // Maps to "id"
    public string FirstName { get; set; }    // Maps to "firstname"
    public string LastName { get; set; }     // Maps to "lastname"
    public string Email { get; set; }        // Maps to "email"
}
```

**CSV file headers:** `id,firstname,lastname,email`

### With Attributes (Explicit Mapping)

Use attributes to specify exact column names or exclude properties:

```csharp
using NPipeline.Connectors.Csv.Attributes;

public class Customer
{
    [CsvColumn("CustomerID")]
    public int Id { get; set; }

    [CsvColumn("FirstName")]
    public string FirstName { get; set; }

    [CsvColumn("LastName")]
    public string LastName { get; set; }

    [CsvColumn("EmailAddress")]
    public string Email { get; set; }

    [CsvIgnore]
    public string InternalNotes { get; set; }  // Not written to CSV
}
```

**CSV file headers:** `CustomerID,FirstName,LastName,EmailAddress`

### Practical Use Cases

#### Use Case 1: Legacy CSV Files with Non-Standard Headers

When working with legacy CSV files that have non-standard column names:

```csharp
public class LegacyCustomer
{
    [CsvColumn("cust_id")]
    public int Id { get; set; }

    [CsvColumn("cust_first_nm")]
    public string FirstName { get; set; }

    [CsvColumn("cust_last_nm")]
    public string LastName { get; set; }

    [CsvColumn("email_addr")]
    public string Email { get; set; }
}
```

**Why this matters:** Legacy systems often use abbreviated or non-standard column names. Attributes allow you to maintain clean, readable property names in your
code while mapping to the actual CSV headers.

#### Use Case 2: Excluding Internal Fields

Prevent internal or computed properties from being written to CSV:

```csharp
public class Customer
{
    [CsvColumn("CustomerID")]
    public int Id { get; set; }

    [CsvColumn("FirstName")]
    public string FirstName { get; set; }

    // Computed property - not in CSV
    [CsvIgnore]
    public string FullName => $"{FirstName} {LastName}";

    // Internal tracking field - not in CSV
    [CsvIgnore]
    public string InternalId { get; set; }
}
```

#### Use Case 3: Mixed Mapping Strategies

Combine convention-based and attribute-based mapping:

```csharp
public class Customer
{
    // Explicit mapping for non-standard columns
    [CsvColumn("cust_id")]
    public int Id { get; set; }

    [CsvColumn("full_name")]
    public string Name { get; set; }

    // Convention-based mapping for standard columns
    public string Email { get; set; }        // Maps to "email"
    public string Phone { get; set; }        // Maps to "phone"

    // Excluded from CSV
    [CsvIgnore]
    public string InternalNotes { get; set; }
}
```

### Performance Benefits

The attribute mapping system uses compiled expression tree delegates for optimal performance:

- **Caching:** Mappers are compiled once per type and cached
- **Type safety:** Compile-time checking of property mappings
- **No reflection overhead:** Compiled delegates are significantly faster than reflection-based mapping

### Updating the Sample to Use Attributes

To modify this sample to use attribute-based mapping:

1. Update the `Customer` class with attributes:

```csharp
using NPipeline.Connectors.Csv.Attributes;

public class Customer
{
    [CsvColumn("Id")]
    public int Id { get; set; }

    [CsvColumn("FirstName")]
    public string FirstName { get; set; } = string.Empty;

    [CsvColumn("LastName")]
    public string LastName { get; set; } = string.Empty;

    [CsvColumn("Email")]
    public string Email { get; set; } = string.Empty;

    [CsvColumn("Age")]
    public int Age { get; set; }

    [CsvColumn("RegistrationDate")]
    public DateTime RegistrationDate { get; set; }

    [CsvColumn("Country")]
    public string Country { get; set; } = string.Empty;

    [CsvIgnore]
    public string InternalNotes { get; set; } = string.Empty;
}
```

1. Simplify the pipeline definition (no need for custom mapper function):

```csharp
// Before: With custom mapper function
var sourceNode = new CsvSourceNode<Customer>(
    StorageUri.FromFilePath(sourcePath),
    row => new Customer
    {
        Id = row.Get("Id", 0),
        FirstName = row.Get("FirstName", string.Empty),
        LastName = row.Get("LastName", string.Empty),
        Email = row.Get("Email", string.Empty),
        Age = row.Get("Age", 0),
        RegistrationDate = row.Get("RegistrationDate", default(DateTime)),
        Country = row.Get("Country", string.Empty),
    });

// After: With attribute-based mapping (automatic)
var sourceNode = new CsvSourceNode<Customer>(
    StorageUri.FromFilePath(sourcePath)
);
```

The mapper is automatically built using [`CsvMapperBuilder<T>`](../../src/NPipeline.Connectors.Csv/Mapping/CsvMapperBuilder.cs) based on the attributes.

### When to Use Attributes vs Custom Mappers

| Scenario                         | Recommended Approach                                                                        |
|----------------------------------|---------------------------------------------------------------------------------------------|
| Simple CSV with standard headers | Convention-based (no attributes needed)                                                     |
| CSV with non-standard headers    | [`CsvColumnAttribute`](../../src/NPipeline.Connectors.Csv/Attributes/CsvColumnAttribute.cs) |
| Need to exclude properties       | [`CsvIgnoreAttribute`](../../src/NPipeline.Connectors.Csv/Attributes/CsvIgnoreAttribute.cs) |
| Complex transformation logic     | Custom mapper function                                                                      |
| Type conversion beyond defaults  | Custom mapper function                                                                      |

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
    [Column("Id")]
    public int Id { get; set; }

    [Column("FirstName")]
    public string FirstName { get; set; } = string.Empty;

    [Column("LastName")]
    public string LastName { get; set; } = string.Empty;

    [Column("Email")]
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
    [Column("customer_id")]
    public int Id { get; set; }

    [Column("first_name")]
    public string FirstName { get; set; }
}
```

**Example: Using Connector-Specific Attributes (PostgreSQL)**

```csharp
using NPipeline.Connectors.PostgreSQL.Mapping;

public class Customer
{
    [PostgresColumn("customer_id", PrimaryKey = true, DbType = NpgsqlDbType.Integer)]
    public int Id { get; set; }

    [PostgresColumn("first_name", DbType = NpgsqlDbType.Varchar, Size = 100)]
    public string FirstName { get; set; }
}
```

### Backward Compatibility

Connector-specific attributes (`CsvColumn`, `CsvIgnore`, `ExcelColumn`, `ExcelIgnore`, `PostgresColumn`, `PostgresIgnore`) continue to work exactly as before.
You can:

- Keep existing code using connector-specific attributes
- Mix common and connector-specific attributes in the same project
- Gradually migrate to common attributes at your own pace

### Sample Code

This sample includes both approaches:

- **`Customer`**: Uses `CsvColumn` and `CsvIgnore` attributes (connector-specific)
- **`CustomerWithCommonAttributes`**: Demonstrates common attributes with detailed comments

Both classes work identically with the CSV connector. The choice of which to use depends on your specific requirements.

## Project Structure

```
Sample_CsvConnector/
├── Data/
│   ├── customers.csv              # Sample input data with valid and invalid records
│   └── processed_customers.csv    # Generated output file (created by pipeline)
├── Nodes/
│   ├── ValidationTransform.cs     # Custom validation transform node
│   └── DataTransform.cs           # Custom data transformation node
├── Customer.cs                    # Customer data model
├── CsvConnectorPipeline.cs       # Main pipeline definition
├── Program.cs                     # Entry point and execution logic
├── Sample_CsvConnector.csproj  # Project configuration
└── README.md                      # This documentation
```

## Running the Sample

### Prerequisites

- .NET 10.0 SDK
- The NPipeline solution built

### Execution

1. Navigate to the sample directory:

   ```bash
   cd samples/Sample_CsvConnector
   ```

2. Build and run the sample:

   ```bash
   dotnet run
   ```

### Expected Output

The pipeline will:

1. Read 13 customer records from `Data/customers.csv`
2. Validate each record (filtering out invalid ones)
3. Transform the valid records (normalize names, countries, etc.)
4. Write the processed records to `Data/processed_customers.csv`

You should see output similar to:

```
=== NPipeline Sample: CSV Connector ===

Registered NPipeline services and scanned assemblies for nodes.

Pipeline Description:
[Detailed pipeline description...]

Pipeline Parameters:
  Source Path: Data/customers.csv
  Target Path: Data/processed_customers.csv

Starting pipeline execution...

Pipeline execution completed successfully!

Output file created: Data/processed_customers.csv
File size: 1024 bytes
Created: 2023-11-22 10:30:45

Sample output (first 5 lines):
  Id,FirstName,LastName,Email,Age,RegistrationDate,Country
  1,John,Doe,john.doe@example.com,28,2023-01-15,United States
  2,Jane,Smith,jane.smith@example.com,34,2023-02-20,Canada
  3,Bob,Johnson,bob.johnson@example.com,45,2023-03-10,United Kingdom
  4,Alice,Williams,alice.williams@example.com,29,2023-04-05,Australia
```

## Sample Data

### Input Data (customers.csv)

The sample input contains 13 records including:

- **Valid records**: Complete customer data with proper formatting
- **Invalid records**: Missing fields, invalid emails, out-of-range ages, etc.

### Output Data (processed_customers.csv)

The output contains only valid records with:

- **Normalized names**: Proper capitalization (John Doe instead of john doe)
- **Normalized countries**: Full country names (United States instead of USA)
- **Lowercase emails**: Consistent email formatting
- **Filtered invalid records**: Only valid customer data is included

## Configuration

### Pipeline Parameters

The pipeline accepts the following parameters:

| Parameter    | Description                 | Default Value                  |
|--------------|-----------------------------|--------------------------------|
| `SourcePath` | Path to the input CSV file  | `Data/customers.csv`           |
| `TargetPath` | Path to the output CSV file | `Data/processed_customers.csv` |

### Validation Rules

The `ValidationTransform` enforces these rules:

- **ID**: Must be greater than 0
- **Name**: First and last name are required
- **Email**: Required and must match email format regex
- **Age**: Must be between 0 and 150
- **Registration Date**: Required and cannot be in the future
- **Country**: Required and non-empty

### Transformation Rules

The `DataTransform` applies these transformations:

- **Name Formatting**: Capitalizes first letter, lowercases the rest
- **Email Normalization**: Converts to lowercase
- **Country Normalization**: Expands abbreviations to full names
- **Data Trimming**: Removes whitespace from all string fields

## Extending the Sample

### Adding New Validation Rules

Extend the `ValidationTransform.TransformAsync` method to add custom validation logic:

```csharp
// Example: Add phone number validation
if (!string.IsNullOrWhiteSpace(input.PhoneNumber) &&
    !PhoneNumberRegex.IsMatch(input.PhoneNumber))
{
    errors.Add($"Invalid phone number format: {input.PhoneNumber}");
}
```

### Adding New Transformations

Extend the `DataTransform.TransformAsync` method to add custom transformations:

```csharp
// Example: Add geographic region calculation
transformedCustomer.Region = GetGeographicRegion(transformedCustomer.Country);
```

### Supporting Different CSV Formats

Modify the `Customer` class and update the CSV configuration in the pipeline to support different column names, delimiters, or data formats.

## Best Practices Demonstrated

1. **Separation of Concerns**: Each node has a single responsibility
2. **Type Safety**: Strongly-typed data models prevent runtime errors
3. **Error Handling**: Comprehensive validation and graceful error handling
4. **Configurability**: Pipeline behavior can be configured through parameters
5. **Testability**: All components are easily testable in isolation
6. **Logging**: Appropriate logging for monitoring and debugging
7. **Resource Management**: Proper disposal of file handles and streams

## Dependencies

This sample uses the following NPipeline packages:

- `NPipeline`: Core pipeline framework
- `NPipeline.Connectors.Csv`: CSV source and sink nodes
- `NPipeline.Connectors`: Storage abstractions
- `NPipeline.Extensions.DependencyInjection`: DI container integration

External dependencies:

- `CsvHelper`: CSV parsing and serialization library
- `Microsoft.Extensions.Hosting`: Host application framework

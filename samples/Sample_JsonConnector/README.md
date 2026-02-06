# Sample 12: JSON Connector

This sample demonstrates comprehensive JSON data processing using NPipeline's JSON connector components. It shows how to read data from a source JSON file,
validate it, transform it, and then write it to a target JSON file.

## Overview

The JSON Connector sample implements a complete data processing pipeline that:

1. **Reads** customer data from a JSON file using `JsonSourceNode<T>`
2. **Validates** each customer record using a custom `ValidationTransform`
3. **Transforms** and enriches the data using a custom `DataTransform`
4. **Writes** the processed data to a new JSON file using `JsonSinkNode<T>`

## Key Concepts Demonstrated

### JSON Connector Components

- **JsonSourceNode<T>**: Reads JSON data and deserializes it to strongly-typed objects
- **JsonSinkNode<T>**: Serializes objects to JSON format and writes to files
- **StorageUri**: Abstracts file system access for consistent storage handling
- **JsonConfiguration**: Configures JSON format, naming policies, and error handling

### Data Validation

- Custom validation logic for business rules
- Configurable filtering of invalid records
- Comprehensive error reporting and logging

### Data Transformation

- Data normalization (name formatting, country codes)
- Data enrichment (calculated fields, derived properties)
- Flexible transformation pipeline

### Error Handling

- Graceful handling of malformed JSON data
- Pipeline-level error capture and reporting
- Configurable error handling strategies

## Attribute-Based Mapping

The JSON connector supports attribute-based mapping, allowing you to control how properties map to JSON properties without writing custom mapper functions.

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

**JSON properties:** `id, firstname, lastname, email`

### With Attributes (Explicit Mapping)

Use attributes to specify exact property names or exclude properties:

```csharp
using NPipeline.Connectors.Attributes;

public class Customer
{
    [Column("customer_id")]
    public int Id { get; set; }

    [Column("first_name")]
    public string FirstName { get; set; }

    [Column("last_name")]
    public string LastName { get; set; }

    [Column("email_address")]
    public string Email { get; set; }

    [IgnoreColumn]
    public string InternalNotes { get; set; }  // Not written to JSON
}
```

**JSON properties:** `customer_id, first_name, last_name, email_address`

### Practical Use Cases

#### Use Case 1: Legacy JSON Files with Non-Standard Property Names

When working with legacy JSON files that have non-standard property names:

```csharp
public class LegacyCustomer
{
    [Column("cust_id")]
    public int Id { get; set; }

    [Column("cust_first_nm")]
    public string FirstName { get; set; }

    [Column("cust_last_nm")]
    public string LastName { get; set; }

    [Column("email_addr")]
    public string Email { get; set; }
}
```

**Why this matters:** Legacy systems often use abbreviated or non-standard property names. Attributes allow you to maintain clean, readable property names in your
code while mapping to the actual JSON properties.

#### Use Case 2: Excluding Internal Fields

Prevent internal or computed properties from being written to JSON:

```csharp
public class Customer
{
    [Column("customer_id")]
    public int Id { get; set; }

    [Column("first_name")]
    public string FirstName { get; set; }

    // Computed property - not in JSON
    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";

    // Internal tracking field - not in JSON
    [IgnoreColumn]
    public string InternalId { get; set; }
}
```

#### Use Case 3: Mixed Mapping Strategies

Combine convention-based and attribute-based mapping:

```csharp
public class Customer
{
    // Explicit mapping for non-standard properties
    [Column("cust_id")]
    public int Id { get; set; }

    [Column("full_name")]
    public string Name { get; set; }

    // Convention-based mapping for standard properties
    public string Email { get; set; }        // Maps to "email"
    public string Phone { get; set; }        // Maps to "phone"

    // Excluded from JSON
    [IgnoreColumn]
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
using NPipeline.Connectors.Attributes;

public class Customer
{
    [Column("id")]
    public int Id { get; set; }

    [Column("firstName")]
    public string FirstName { get; set; } = string.Empty;

    [Column("lastName")]
    public string LastName { get; set; } = string.Empty;

    [Column("email")]
    public string Email { get; set; } = string.Empty;

    [Column("age")]
    public int Age { get; set; }

    [Column("registrationDate")]
    public DateTime RegistrationDate { get; set; }

    [Column("country")]
    public string Country { get; set; } = string.Empty;

    [Column("isActive")]
    public bool IsActive { get; set; }

    [IgnoreColumn]
    public string InternalNotes { get; set; } = string.Empty;
}
```

1. Simplify the pipeline definition (no need for custom mapper function):

```csharp
// Before: With custom mapper function
var sourceNode = new JsonSourceNode<Customer>(
    StorageUri.FromFilePath(sourcePath),
    row => new Customer
    {
        Id = row.Get("id", 0),
        FirstName = row.Get("firstName", string.Empty),
        LastName = row.Get("lastName", string.Empty),
        Email = row.Get("email", string.Empty),
        Age = row.Get("age", 0),
        RegistrationDate = row.Get("registrationDate", default(DateTime)),
        Country = row.Get("country", string.Empty),
        IsActive = row.Get("isActive", false),
    });

// After: With attribute-based mapping (automatic)
var sourceNode = new JsonSourceNode<Customer>(
    StorageUri.FromFilePath(sourcePath)
);
```

The mapper is automatically built using `JsonMapperBuilder<T>` based on the attributes.

### When to Use Attributes vs Custom Mappers

| Scenario                         | Recommended Approach                                                                        |
|----------------------------------|---------------------------------------------------------------------------------------------|
| Simple JSON with standard properties | Convention-based (no attributes needed)                                                     |
| JSON with non-standard properties    | `ColumnAttribute`                                                                            |
| Need to exclude properties       | `IgnoreColumnAttribute`                                                                      |
| Complex transformation logic     | Custom mapper function                                                                      |
| Type conversion beyond defaults  | Custom mapper function                                                                      |

## Common Attributes

NPipeline now supports **common attributes** that work across all connectors (CSV, Excel, PostgreSQL, JSON, etc.). This allows you to use the same attributes for
different data sources, making your code more portable and maintainable.

### What Are Common Attributes?

Common attributes are defined in `NPipeline.Connectors.Attributes` namespace and provide a unified way to specify column mappings across all connectors:

- **`ColumnAttribute`**: Specifies the column/property name for a property
- **`IgnoreColumnAttribute`**: Excludes a property from mapping

### Using Common Attributes

To use common attributes, add a reference to `NPipeline.Connectors` and import the namespace:

```csharp
using NPipeline.Connectors.Attributes;

public class CustomerWithCommonAttributes
{
    [Column("id")]
    public int Id { get; set; }

    [Column("firstName")]
    public string FirstName { get; set; } = string.Empty;

    [Column("lastName")]
    public string LastName { get; set; } = string.Empty;

    [Column("email")]
    public string Email { get; set; } = string.Empty;

    [IgnoreColumn]
    public string InternalNotes { get; set; } = string.Empty;

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

### Benefits of Common Attributes

- **Cross-connector compatibility**: Same attributes work with CSV, Excel, PostgreSQL, JSON, etc.
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

### Backward Compatibility

Connector-specific attributes continue to work exactly as before.
You can:

- Keep existing code using connector-specific attributes
- Mix common and connector-specific attributes in the same project
- Gradually migrate to common attributes at your own pace

### Sample Code

This sample includes both approaches:

- **`Customer`**: Uses `Column` and `IgnoreColumn` attributes (common attributes)

Both approaches work identically with the JSON connector. The choice of which to use depends on your specific requirements.

## JSON Formats

The JSON connector supports two different formats:

### Array Format (Default)

Traditional JSON array with all records in a single array:

```json
[
  {
    "id": 1,
    "firstName": "John",
    "lastName": "Doe",
    "email": "john.doe@example.com"
  },
  {
    "id": 2,
    "firstName": "Jane",
    "lastName": "Smith",
    "email": "jane.smith@example.com"
  }
]
```

### Newline-Delimited JSON (NDJSON)

Each line contains a separate JSON object:

```json
{"id": 1, "firstName": "John", "lastName": "Doe", "email": "john.doe@example.com"}
{"id": 2, "firstName": "Jane", "lastName": "Smith", "email": "jane.smith@example.com"}
```

**Benefits of NDJSON:**

- Better for streaming large datasets
- Easier to process line by line
- More resilient to partial file corruption
- Common in log files and big data processing

## Configuration Options

The `JsonConfiguration` class provides various configuration options:

### Format

```csharp
var config = new JsonConfiguration
{
    Format = JsonFormat.Array  // or JsonFormat.NewlineDelimited
};
```

### Property Naming Policy

```csharp
var config = new JsonConfiguration
{
    PropertyNamingPolicy = JsonPropertyNamingPolicy.CamelCase  // Options: LowerCase, CamelCase, SnakeCase, PascalCase, AsIs
};
```

### Write Indented

```csharp
var config = new JsonConfiguration
{
    WriteIndented = true  // Pretty-print JSON for readability
};
```

### Property Name Case Insensitive

```csharp
var config = new JsonConfiguration
{
    PropertyNameCaseInsensitive = true  // Match properties case-insensitively
};
```

### Buffer Size

```csharp
var config = new JsonConfiguration
{
    BufferSize = 8192  // Buffer size in bytes (default: 4096)
};
```

## Project Structure

```
Sample_JsonConnector/
├── Data/
│   ├── customers.json              # Sample input data with valid and invalid records
│   ├── customers.ndjson           # Sample input data in NDJSON format
│   └── processed_customers.json  # Generated output file (created by pipeline)
├── Nodes/
│   ├── ValidationTransform.cs      # Custom validation transform node
│   └── DataTransform.cs          # Custom data transformation node
├── Customer.cs                   # Customer data model
├── JsonConnectorPipeline.cs       # Main pipeline definition
├── Program.cs                    # Entry point and execution logic
├── Sample_JsonConnector.csproj  # Project configuration
└── README.md                     # This documentation
```

## Running the Sample

### Prerequisites

- .NET 10.0 SDK
- The NPipeline solution built

### Execution

1. Navigate to the sample directory:

   ```bash
   cd samples/Sample_JsonConnector
   ```

2. Build and run the sample:

   ```bash
   dotnet run
   ```

### Expected Output

The pipeline will:

1. Read 13 customer records from `Data/customers.json`
2. Validate each record (filtering out invalid ones)
3. Transform the valid records (normalize names, countries, etc.)
4. Write the processed records to `Data/processed_customers.json`

You should see output similar to:

```
=== NPipeline Sample: JSON Connector ===

Registered NPipeline services and scanned assemblies for nodes.

Pipeline Description:
JSON Connector Pipeline Sample:

WHAT IT DOES:
- Reads customer records from a JSON file (customers.json)
- Validates each record and filters out invalid ones
- Transforms and enriches the valid records
- Writes the processed records to a new JSON file (processed_customers.json)

...

Pipeline Parameters:
  Source Path: /path/to/Data/customers.json
  Target Path: /path/to/Data/processed_customers.json

Starting pipeline execution...

Pipeline execution completed successfully!

Output file created: /path/to/Data/processed_customers.json
File size: 2048 bytes
Created: 2023-11-22 10:30:45

Output file content:
[
  {
    "id": 1,
    "firstName": "John",
    "lastName": "Doe",
    "email": "john.doe@example.com",
    "age": 28,
    "registrationDate": "2023-01-15T00:00:00Z",
    "country": "United States",
    "isActive": true
  },
  ...
]
```

## Sample Data

### Input Data (customers.json)

The sample input contains 13 records including:

- **Valid records**: Complete customer data with proper formatting
- **Invalid records**: Missing fields, invalid emails, out-of-range ages, etc.

### Output Data (processed_customers.json)

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
| `SourcePath` | Path to the input JSON file  | `Data/customers.json`           |
| `TargetPath` | Path to the output JSON file | `Data/processed_customers.json` |

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

### Supporting Different JSON Formats

Modify the `JsonConfiguration` in the pipeline to support different formats:

```csharp
var config = new JsonConfiguration
{
    Format = JsonFormat.NewlineDelimited,
    PropertyNamingPolicy = JsonPropertyNamingPolicy.SnakeCase,
    WriteIndented = false,
};
```

### Enabling NDJSON Processing

Uncomment the NDJSON scenario in `JsonConnectorPipeline.cs` to demonstrate NDJSON processing:

```csharp
var ndjsonConfiguration = new JsonConfiguration
{
    Format = JsonFormat.NewlineDelimited,
    PropertyNamingPolicy = JsonPropertyNamingPolicy.CamelCase,
    WriteIndented = false,
};

var ndjsonSourceNode = new JsonSourceNode<Customer>(
    StorageUri.FromFilePath(ndjsonSourcePath),
    ndjsonConfiguration);

// ... add nodes and connect them
```

## Best Practices Demonstrated

1. **Separation of Concerns**: Each node has a single responsibility
2. **Type Safety**: Strongly-typed data models prevent runtime errors
3. **Error Handling**: Comprehensive validation and graceful error handling
4. **Configurability**: Pipeline behavior can be configured through parameters
5. **Testability**: All components are easily testable in isolation
6. **Logging**: Appropriate logging for monitoring and debugging
7. **Resource Management**: Proper disposal of file handles and streams
8. **Attribute-Based Mapping**: Clean, declarative property mapping
9. **Format Flexibility**: Support for multiple JSON formats
10. **Performance**: Compiled expression tree delegates for optimal performance

## Dependencies

This sample uses the following NPipeline packages:

- `NPipeline`: Core pipeline framework
- `NPipeline.Connectors.Json`: JSON source and sink nodes
- `NPipeline.Connectors`: Storage abstractions
- `NPipeline.Extensions.DependencyInjection`: DI container integration

External dependencies:

- `System.Text.Json`: JSON parsing and serialization library
- `Microsoft.Extensions.Hosting`: Host application framework

## Comparison with CSV Connector

The JSON Connector sample is similar to the CSV Connector sample but demonstrates JSON-specific features:

| Feature               | CSV Connector                | JSON Connector                  |
|------------------------|------------------------------|---------------------------------|
| File Format            | CSV (comma-separated values)   | JSON (Array or NDJSON)          |
| Attribute Support       | `CsvColumn`, `CsvIgnore`      | `Column`, `IgnoreColumn`         |
| Configuration          | `CsvConfiguration`             | `JsonConfiguration`              |
| Format Options         | Delimiter, HasHeader, etc.     | Format, NamingPolicy, Indented   |
| Nested Data            | Limited                       | Full support                     |
| Data Types             | String-based with conversion     | Native JSON types                |

Both connectors share the same attribute system (`Column` and `IgnoreColumn`) for cross-connector compatibility.

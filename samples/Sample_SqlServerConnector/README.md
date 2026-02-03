# Sample_SqlServerConnector

This sample demonstrates the usage of SQL Server connector in NPipeline for reading from and writing to Microsoft SQL Server databases.

## Overview

The SQL Server Connector sample showcases how to use NPipeline's SQL Server connector to perform various database operations including reading data, writing
data with different strategies, attribute-based and convention-based mapping, custom mappers, error handling, and data transformations.

## Features Demonstrated

### 1. Reading from SQL Server

- **SqlServerSourceNode**: Read data from SQL Server tables
- Parameterized queries for security
- Streaming results for efficient memory usage
- Support for custom queries

### 2. Writing to SQL Server

- **SqlServerSinkNode**: Write data to SQL Server tables
- **PerRow Write Strategy**: Write one row at a time (best for small batches)
- **Batch Write Strategy**: Write in batches (best for most scenarios)
- Transaction support for atomicity

### 3. Mapping Strategies

#### Attribute-Based Mapping

- **SqlServerTableAttribute**: Specify table name and schema
- **SqlServerColumnAttribute**: SQL Server-specific features (DbType, Size, PrimaryKey, Identity)
- **ColumnAttribute**: Common attribute for simple column name mappings
- **IgnoreColumnAttribute**: Exclude computed properties from mapping

#### Convention-Based Mapping

- Automatic PascalCase to PascalCase mapping
- No attributes required for simple scenarios
- Case-insensitive column matching

#### Custom Mappers

- `Func<T, IEnumerable<DatabaseParameter>>` for custom parameter mapping
- Transform data before writing
- Apply custom business logic

### 4. Connection Management

- Connection pooling for efficiency
- Named connections support
- Connection lifecycle management
- Windows and SQL Server authentication

### 5. Error Handling

- Retry logic for transient errors
- Row-level error handling
- Continue-on-error mode
- Transaction rollback support

### 6. Transformations

- Data enrichment
- Aggregation operations
- Multiple table processing
- Computed properties

## Prerequisites

### SQL Server / LocalDB

You need one of the following installed:

1. **SQL Server Express LocalDB** (Recommended for development)
    - Included with Visual Studio 2017 and later
    - Download from [Microsoft SQL Server Express](https://www.microsoft.com/en-us/sql-server/sql-server-downloads)

2. **SQL Server Developer Edition**
    - Free for development
    - Download from [Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server/sql-server-downloads)

3. **Azure SQL Database**
    - Cloud-hosted SQL Server
    - Create an account at [Azure Portal](https://portal.azure.com)

### .NET SDK

- .NET 8.0 SDK or later
- [Download .NET SDK](https://dotnet.microsoft.com/download)

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd NPipeline
```

### 2. Restore Dependencies

```bash
dotnet restore
```

### 3. Build the Solution

```bash
dotnet build
```

### 4. Prepare the Database

The sample will automatically create the necessary database schema (tables) when you run it. You just need to ensure your SQL Server instance is running.

#### For LocalDB (Default)

No additional setup required. The sample uses the default LocalDB connection string:

```
Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=NPipelineSamples;Integrated Security=True;MultipleActiveResultSets=True;Connect Timeout=30;
```

#### For SQL Server

1. Ensure SQL Server is running
2. Create a database (optional, the sample will use the default database)
3. Note your connection string

#### For Azure SQL

1. Create an Azure SQL Database
2. Get the connection string from the Azure Portal
3. Ensure your IP is allowed in the firewall rules

## How to Run the Sample

### Using Default LocalDB Connection

```bash
dotnet run --project samples/Sample_SqlServerConnector
```

### Using Custom Connection String

```bash
dotnet run --project samples/Sample_SqlServerConnector "<connection-string>"
```

### Example Connection Strings

#### LocalDB

```
"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=MyDb;Integrated Security=True;"
```

#### SQL Server with Windows Authentication

```
"Server=localhost;Database=MyDb;Integrated Security=True;MultipleActiveResultSets=True;"
```

#### SQL Server with SQL Server Authentication

```
"Server=localhost;Database=MyDb;User Id=sa;Password=yourpassword;MultipleActiveResultSets=True;"
```

#### Azure SQL Database

```
"Server=tcp:myserver.database.windows.net,1433;Database=mydb;User Id=myuser;Password=mypassword;Encrypt=True;"
```

## What the Sample Demonstrates

The sample executes the following steps in sequence:

### Step 1: Initialize Database Schema

- Creates `Sales` and `Analytics` schemas
- Creates `Sales.Customers` table
- Creates `Sales.Orders` table
- Creates `Sales.Products` table
- Creates `Analytics.EnrichedCustomers` table

### Step 2: PerRow Write Strategy

- Writes 3 sample customers using PerRow strategy
- Demonstrates row-by-row inserts
- Shows transaction support
- Displays performance metrics

### Step 3: Batch Write Strategy

- Writes 50 sample orders using Batch strategy
- Demonstrates batched inserts (10 rows per batch)
- Shows improved performance over PerRow
- Displays batch count and timing

### Step 4: Attribute-Based Mapping

- Reads customers using attribute-based mapping
- Demonstrates SqlServerTable, SqlServerColumn, Column, and IgnoreColumn attributes
- Shows SQL Server-specific features (DbType, Size, PrimaryKey, Identity)
- Displays mapped data

### Step 5: Convention-Based Mapping

- Writes and reads products using convention-based mapping
- Demonstrates automatic PascalCase to PascalCase mapping
- Shows no attributes required for simple scenarios
- Displays mapped data

### Step 6: Custom Mappers

- Writes orders using custom mapper function
- Demonstrates data transformation before writing
- Shows custom business logic application
- Displays transformed data

### Step 7: Transformation and Enrichment

- Reads customers and orders
- Enriches customer data with order statistics
- Calculates total orders, total spent, average order value
- Determines customer tier (Bronze, Silver, Gold, Platinum)
- Writes enriched data to Analytics schema

### Step 8: Error Handling

- Attempts to write orders with invalid customer IDs
- Demonstrates error handling with ContinueOnError = false
- Demonstrates error handling with ContinueOnError = true
- Shows retry logic and transient error handling

## Expected Output

When you run the sample, you'll see output similar to:

```
=== NPipeline Sample: SQL Server Connector ===

No connection string provided. Using default LocalDB connection:
  Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=NPipelineSamples;Integrated Security=True;MultipleActiveResultSets=True;Connect Timeout=30;

...

Pipeline Description:
SQL Server Connector Sample Pipeline
====================================

This pipeline demonstrates the following features:

1. Reading from SQL Server
   - SqlServerSourceNode for data retrieval
   - Parameterized queries
   - Streaming results

2. Writing to SQL Server
   - SqlServerSinkNode for data insertion
   - PerRow write strategy (row-by-row)
   - Batch write strategy (batched inserts)

...

Step 1: Initializing Database Schema
-----------------------------------
✓ Database schema initialized successfully

Step 2: Demonstrating PerRow Write Strategy
---------------------------------------------
Writing 3 customers using PerRow strategy...
✓ PerRow write completed in XX.XXms
  - Strategy: PerRow (row-by-row inserts)
  - Transaction: Enabled

Step 3: Demonstrating Batch Write Strategy
-------------------------------------------
Writing 50 orders using Batch strategy...
  - Batch size: 10
✓ Batch write completed in XX.XXms
  - Strategy: Batch (batched inserts)
  - Transaction: Enabled
  - Batches processed: 5

...

Pipeline execution completed successfully!
```

## Database Schema

### Sales.Customers

| Column           | Type          | Description                                   |
|------------------|---------------|-----------------------------------------------|
| CustomerID       | INT IDENTITY  | Primary key, auto-increment                   |
| FirstName        | NVARCHAR(100) | Customer's first name                         |
| LastName         | NVARCHAR(100) | Customer's last name                          |
| Email            | NVARCHAR(255) | Customer's email address                      |
| PhoneNumber      | NVARCHAR(50)  | Customer's phone number (optional)            |
| RegistrationDate | DATE          | Date when customer registered                 |
| Status           | NVARCHAR(50)  | Customer status (Active, Inactive, Suspended) |

### Sales.Orders

| Column          | Type           | Description                 |
|-----------------|----------------|-----------------------------|
| OrderID         | INT IDENTITY   | Primary key, auto-increment |
| CustomerID      | INT            | Foreign key to Customers    |
| OrderDate       | DATETIME2      | Date and time of order      |
| TotalAmount     | DECIMAL(18,2)  | Total order amount          |
| Status          | NVARCHAR(50)   | Order status                |
| ShippingAddress | NVARCHAR(500)  | Shipping address (optional) |
| Notes           | NVARCHAR(1000) | Order notes (optional)      |

### Sales.Products

| Column        | Type          | Description                 |
|---------------|---------------|-----------------------------|
| ProductID     | INT IDENTITY  | Primary key, auto-increment |
| ProductName   | NVARCHAR(255) | Product name                |
| Category      | NVARCHAR(100) | Product category            |
| Price         | DECIMAL(18,2) | Product price               |
| StockQuantity | INT           | Stock quantity              |

### Analytics.EnrichedCustomers

| Column            | Type          | Description                                    |
|-------------------|---------------|------------------------------------------------|
| CustomerID        | INT           | Primary key (foreign key)                      |
| FullName          | NVARCHAR(255) | Customer's full name                           |
| Email             | NVARCHAR(255) | Customer's email (uppercase)                   |
| PhoneNumber       | NVARCHAR(50)  | Customer's phone number                        |
| RegistrationDate  | DATE          | Original registration date                     |
| Status            | NVARCHAR(50)  | Customer status                                |
| TotalOrders       | INT           | Total number of orders                         |
| TotalSpent        | DECIMAL(18,2) | Total amount spent                             |
| AverageOrderValue | DECIMAL(18,2) | Average order value                            |
| CustomerTier      | NVARCHAR(50)  | Customer tier (Bronze, Silver, Gold, Platinum) |
| LastOrderDate     | DATETIME2     | Date of last order                             |
| EnrichmentDate    | DATETIME2     | Date when enrichment was performed             |

## Key Concepts

### Write Strategies

#### PerRow Strategy

- **Best for**: Small batches (< 10 rows), testing, debugging
- **Performance**: Slowest (one INSERT per row)
- **Memory**: Lowest
- **Error Handling**: Per-row error handling

#### Batch Strategy

- **Best for**: Most scenarios (100-1,000 rows)
- **Performance**: Good (multiple rows per INSERT)
- **Memory**: Moderate
- **Error Handling**: Batch-level error handling

### Mapping Strategies

#### Attribute-Based Mapping

Use attributes when you need:

- Custom column names
- SQL Server-specific features (DbType, Size, PrimaryKey, Identity)
- Explicit control over mapping

```csharp
[SqlServerTable("Customers", Schema = "Sales")]
public class Customer
{
    [SqlServerColumn("CustomerID", PrimaryKey = true, Identity = true)]
    public int CustomerId { get; set; }

    [Column("FirstName")]
    public string FirstName { get; set; }

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

#### Convention-Based Mapping

Use convention when:

- Property names match column names
- No special features needed
- Simpler code is preferred

```csharp
public class Product
{
    public int ProductId { get; set; }  // Maps to ProductId
    public string ProductName { get; set; }  // Maps to ProductName
    public decimal Price { get; set; }  // Maps to Price
}
```

#### Custom Mappers

Use custom mappers when:

- You need to transform data before writing
- Complex parameter mapping is required
- Business logic must be applied

```csharp
Func<Order, IEnumerable<DatabaseParameter>> mapper = order =>
[
    new DatabaseParameter("@CustomerID", order.CustomerId),
    new DatabaseParameter("@OrderDate", order.OrderDate),
    new DatabaseParameter("@TotalAmount", order.TotalAmount),
    new DatabaseParameter("@Status", "Custom-" + order.Status)
];
```

## Troubleshooting

### Connection Issues

**Error**: "A network-related or instance-specific error occurred"

**Solutions**:

1. Ensure SQL Server/LocalDB is running
2. Verify the server name in the connection string
3. Check firewall settings
4. For LocalDB, ensure the instance name is correct: `(localdb)\MSSQLLocalDB`

### Permission Issues

**Error**: "Cannot open database requested by the login"

**Solutions**:

1. Ensure the database exists
2. Verify your login has permissions
3. For LocalDB, ensure you're running as the correct user
4. For Azure SQL, check firewall rules

### Timeout Issues

**Error**: "Execution Timeout Expired"

**Solutions**:

1. Increase `CommandTimeout` in configuration
2. Optimize your queries
3. Check for blocking locks
4. Increase `ConnectTimeout` in connection string

## Additional Resources

- [NPipeline Documentation](../../docs/)
- [SQL Server Connector Documentation](../../src/NPipeline.Connectors.SqlServer/README.md)
- [Microsoft.Data.SqlClient Documentation](https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.sqlclient)
- [SQL Server Connection Strings](https://www.connectionstrings.com/sql-server/)

## License

This sample is part of the NPipeline project. See the main repository for license information.

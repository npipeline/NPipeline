# Sample_SqlServerConnector

This sample demonstrates the usage of SQL Server connector in NPipeline for reading from and writing to Microsoft SQL Server databases.

## Overview

The SQL Server Connector sample showcases how to use NPipeline's SQL Server connector to perform various database operations including reading data, writing
data with different strategies, attribute-based and convention-based mapping, custom mappers, error handling, and data transformations.

## Features Demonstrated

### 1. Reading from SQL Server

- **`SqlServerConnector.Source<T>(connectionString, query)`**: stream query results into records, with nothing buffered
- Columns bind to members by name (case-insensitively) through a mapper compiled once per query
- Parameterized queries (`Parameters = [new DatabaseParameter("@since", value)]`)
- A manual mapper over `SqlRow` for full control

### 2. Writing to SQL Server

- **`SqlServerConnector.Sink<T>(connectionString, table, configure)`**: write records in batches
- **`PerRow`**: one statement per row (slowest, simplest to debug)
- **`Batch`** (default): multi-row `INSERT` statements under SQL Server's 2,100-parameter limit
- **`Transaction`**: `PerBatch` (the default; each batch lands whole and is retried safely), `WholeRun`, or `None`

### 3. Mapping Strategies

#### Attribute-Based Mapping

- **`[SqlServerColumn]`**: SQL Server-specific features (`DbType`, `Size`, `Identity`; identity columns are read but never written)
- **`[Column]`**: the common attribute for simple column name mappings
- **`[IgnoreColumn]`**: exclude computed properties from mapping

#### Convention-Based Mapping

- Members map to columns of the same name, case-insensitively
- No attributes required for simple scenarios

#### Manual Mapping

- `SqlServerConnector.Source(connectionString, query, row => ...)` maps each `SqlRow` yourself
- To shape what is written, transform the records in front of the sink

### 4. Connections

- Connection strings are used as they are: pool sizes, timeouts and encryption belong in them
- Windows and SQL Server authentication, `mssql://` storage URIs, and named connections through `ISqlServerConnectionPool`

### 5. Error Handling

- Transient errors (deadlocks, timeouts, lost connections) are retried with `SqlServerConnectorResilience`, because each batch is its own transaction
- A row that fails to map goes through `RowErrorHandler` (`Fail`, `Skip`, `DeadLetter`)
- A batch that fails can fail the write or go to the dead-letter sink (`FailedBatches`)
- `Transaction = WholeRun` rolls everything back when one batch fails

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
- Demonstrates SqlServerColumn, Column, and IgnoreColumn attributes
- Shows SQL Server-specific features (DbType, Size, Identity)
- Displays mapped data

### Step 5: Convention-Based Mapping

- Writes and reads products using convention-based mapping
- Demonstrates automatic PascalCase to PascalCase mapping
- Shows no attributes required for simple scenarios
- Displays mapped data

### Step 6: Custom Mappers

- Shapes orders (status prefix, default address) before writing them
- Reads them back with a manual mapper over `SqlRow`
- Displays transformed data

### Step 7: Transformation and Enrichment

- Reads customers and orders
- Enriches customer data with order statistics
- Calculates total orders, total spent, average order value
- Determines customer tier (Bronze, Silver, Gold, Platinum)
- Writes enriched data to Analytics schema

### Step 8: Error Handling

- Attempts to write orders with an invalid customer ID
- With the default `PerBatch` transactions, the orders before the failed one stay written
- With `Transaction = WholeRun`, the failure rolls back every order
- Transient errors are retried by the connector's `Resilience` policy; a foreign key violation is not

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
   - SqlServerConnector.Source for data retrieval
   - Parameterized queries
   - Streaming results

2. Writing to SQL Server
   - SqlServerConnector.Sink for data insertion
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
| ProductID     | INT           | Primary key                 |
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

- **Best for**: Small volumes, testing, debugging
- **Performance**: Slowest (one INSERT per row)
- **Memory**: Lowest

#### Batch Strategy

- **Best for**: Most scenarios
- **Performance**: Good (up to ten rows per INSERT, the size SQL Server compiles fastest)
- **Memory**: Moderate

For large loads, `SqlServerWriteStrategy.BulkCopy` streams each batch through `SqlBulkCopy`.

### Mapping Strategies

#### Attribute-Based Mapping

Use attributes when you need:

- Custom column names
- SQL Server-specific features (DbType, Size, Identity)
- Explicit control over mapping

```csharp
public class Customer
{
    [SqlServerColumn("CustomerID", Identity = true)]
    public int CustomerId { get; set; }

    [Column("FirstName")]
    public string FirstName { get; set; }

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}

var sink = SqlServerConnector.Sink<Customer>(connectionString, "Customers", o => o with { Schema = "Sales" });
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

#### Manual Mapping

Use a manual mapper when you need full control over how a row becomes a record:

```csharp
var source = SqlServerConnector.Source(connectionString, "SELECT OrderID, Status FROM Sales.Orders",
    row => (Id: row.Get<int>("OrderID"), Status: row.GetOrDefault("Status", "Unknown")));
```

### Transactions and Failed Batches

| `Transaction` | Behaviour |
|---------------|-----------|
| `PerBatch` (default) | Each batch commits in its own transaction and is retried safely on a transient error |
| `WholeRun` | One transaction for the whole write, rolled back if anything fails |
| `None` | No transaction of the sink's own; failed batches are not retried |

With `FailedBatches = FailedBatchAction.DeadLetter`, a batch that fails goes to the pipeline's dead-letter sink and the
write continues. See [SQL Connectors: Shared Behaviour](../../docs/connectors/sql-connectors.md).

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

1. Increase `CommandTimeout` in the source or sink options (30 seconds by default)
2. Optimize your queries
3. Check for blocking locks
4. Increase `Connect Timeout` in the connection string

## Additional Resources

- [NPipeline Documentation](../../docs/)
- [SQL Server Connector Documentation](../../docs/connectors/sqlserver.md)
- [SQL Connectors: Shared Behaviour](../../docs/connectors/sql-connectors.md)
- [Microsoft.Data.SqlClient Documentation](https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.sqlclient)
- [SQL Server Connection Strings](https://www.connectionstrings.com/sql-server/)

## License

This sample is part of the NPipeline project. See the main repository for license information.

# Sample: PostgreSQL Connector

This sample demonstrates comprehensive PostgreSQL data processing using NPipeline's PostgreSQL connector components. It shows how to read data from PostgreSQL
tables, transform it, and write to PostgreSQL tables using various strategies and configurations.

## Overview

The PostgreSQL Connector sample implements a complete data processing pipeline that:

1. **Reads** customer, product, and order data from PostgreSQL tables with `PostgresConnector.Source<T>`
2. **Transforms** and aggregates order data into summaries
3. **Writes** processed data to PostgreSQL tables with `PostgresConnector.Sink<T>`
4. **Demonstrates** different write strategies (PerRow, Batch)
5. **Shows** attribute-based mapping with `PostgresColumnAttribute` and the snake_case naming convention

## Key Concepts Demonstrated

### PostgreSQL Connector Components

- **`PostgresConnector.Source<T>(connectionString, query, configure)`**: streams query results into strongly-typed records
- **`PostgresConnector.Sink<T>(connectionString, table, configure)`**: writes records to a table in batches
- **`PostgresReadOptions` / `PostgresWriteOptions`**: immutable option records, adjusted with `o => o with { ... }`

### Attribute-Based Mapping

- **`PostgresColumnAttribute`**: maps C# properties to PostgreSQL columns, and can set the parameter's `DbType`
- **Convention-based mapping**: members map to snake_case columns (`CustomerId` to `customer_id`) with no attributes; acronyms stay one word (`HTTPStatus` to `http_status`). Set `Naming = ColumnNamingPolicy.AsIs` for columns named like the members.
- The table is named when the sink is created, not on the class

### Common Attributes

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

public class Customer
{
    [Column("customer_id")]
    public int CustomerId { get; set; }

    [Column("first_name")]
    public string FirstName { get; set; } = string.Empty;

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

### Common vs Connector-Specific Attributes

Both are fully supported, and a member with both takes its name from the common `[Column]`:

| Scenario                                | Recommended Approach                                |
|-----------------------------------------|-----------------------------------------------------|
| Simple column mapping                   | Common attributes (`Column`, `IgnoreColumn`)        |
| Cross-connector compatibility           | Common attributes (`Column`, `IgnoreColumn`)        |
| Database-specific features (PostgreSQL) | Connector-specific (`PostgresColumn` with `DbType`) |

```csharp
using NPipeline.Connectors.Postgres.Mapping;
using NpgsqlTypes;

public class Event
{
    [PostgresColumn("payload", DbType = NpgsqlDbType.Jsonb)]
    public string Payload { get; set; } = "{}";
}
```

### Sample Code

The sample's models (`Customer`, `Product`, `Order`, `OrderItem`, `OrderSummary`) use `PostgresColumn` to name their columns.

### Write Strategies

- **PerRow**: Writes one row at a time (slowest, simplest to debug)
- **Batch**: Writes multi-row `INSERT` statements (good balance of performance and memory)
- **Copy**: Binary `COPY`, the fastest for large loads (inserts only)

### Data Models

The sample includes realistic business models:

- **Customer**: Customer information with contact details
- **Product**: Product catalog with pricing and inventory
- **Order**: Order header with customer and shipping information
- **OrderItem**: Order line items linking orders to products
- **OrderSummary**: Aggregated order data for reporting

## Project Structure

```
Sample_PostgresConnector/
├── Models.cs                          # Data model classes (Customer, Product, Order, etc.)
├── PostgresConnectorPipeline.cs       # Main pipeline definition
├── Program.cs                          # Entry point and execution logic
├── Sample_PostgresConnector.csproj    # Project configuration
└── README.md                           # This documentation
```

## Running the Sample

### Prerequisites

- .NET 8.0, 9.0, or 10.0 SDK
- PostgreSQL 12 or later installed and running
- A database named `NPipelineSamples` (or modify connection string)
- The NPipeline solution built

### Database Setup

Create the database:

```bash
# Connect to PostgreSQL
psql -U postgres

# Create the database
CREATE DATABASE NPipelineSamples;
```

### Connection String Configuration

The sample uses the default connection string:

```
Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=NPipelineSamples
```

You can override this by setting an environment variable:

```bash
# Linux/macOS
export NPipeline_PostgreSQL_ConnectionString="Host=localhost;Port=5432;Username=your_user;Password=your_password;Database=your_database"

# Windows PowerShell
$env:NPipeline_PostgreSQL_ConnectionString="Host=localhost;Port=5432;Username=your_user;Password=your_password;Database=your_database"

# Windows Command Prompt
set NPipeline_PostgreSQL_ConnectionString=Host=localhost;Port=5432;Username=your_user;Password=your_password;Database=your_database
```

### Execution

1. Navigate to the sample directory:

   ```bash
   cd samples/Sample_PostgresConnector
   ```

2. Build and run the sample:

   ```bash
   dotnet run
   ```

### Expected Output

The pipeline will:

1. Create database tables (customers, products, orders, order_items, order_summaries, and copy tables for the demonstrations)
2. Seed sample data (5 customers, 8 products, 6 orders with items)
3. Process and copy customers and products
4. Generate order summaries by joining orders with customers and items
5. Demonstrate different write strategies with performance comparison
6. Demonstrate in-memory checkpointing: stop after 5 rows, then resume with the rest

You should see output similar to:

```
=== NPipeline Sample: PostgreSQL Connector ===

Connection Configuration:
  Connection String: Host=localhost;Port=5432;Username=postgres;Password=****;Database=NPipelineSamples

Testing database connection...
Database connection successful!

Registered NPipeline services and scanned assemblies for nodes.

Pipeline Description:
[Detailed pipeline description...]

Starting pipeline execution...

=== PostgreSQL Connector Pipeline ===

Step 1: Setting up database tables...
  Tables created successfully.

Step 2: Seeding sample data...
  Sample data seeded successfully.

Step 3: Processing customers...
  Processed 5 customers.

Step 4: Processing products...
  Processed 8 products.

Step 5: Processing orders and generating summaries...
  Generated 6 order summaries.

Step 6: Demonstrating write strategies...
  PerRow strategy: 160 ms
  Batch strategy (size 25): 11 ms
  Performance improvement: Batch is 14.55x faster than PerRow

Step 7: Demonstrating in-memory checkpointing...
  Simulating interruption after 5 rows...
  Resumed and processed 21 remaining rows.

=== Pipeline Execution Summary ===
  Total time: 448 ms
  Customers processed: 5
  Products processed: 8
  Order summaries generated: 6

Pipeline execution completed successfully!
```

## Sample Data

### Customers

The sample includes 5 customers with complete contact information:

| Customer ID | Name           | Email                        | City        | State | Country |
|-------------|----------------|------------------------------|-------------|-------|---------|
| 1           | John Doe       | <john.doe@example.com>       | Springfield | IL    | USA     |
| 2           | Jane Smith     | <jane.smith@example.com>     | Chicago     | IL    | USA     |
| 3           | Bob Johnson    | <bob.johnson@example.com>    | Milwaukee   | WI    | USA     |
| 4           | Alice Williams | <alice.williams@example.com> | Minneapolis | MN    | USA     |
| 5           | Charlie Brown  | <charlie.brown@example.com>  | Des Moines  | IA    | USA     |

### Products

The sample includes 8 products across categories:

| Product ID | Name                 | SKU          | Category    | Price     | Stock |
|------------|----------------------|--------------|-------------|-----------|-------|
| 1          | Laptop Computer      | LAPTOP-001   | Electronics | $1,299.99 | 50    |
| 2          | Wireless Mouse       | MOUSE-001    | Electronics | $29.99    | 200   |
| 3          | Mechanical Keyboard  | KEYBOARD-001 | Electronics | $149.99   | 75    |
| 4          | USB-C Hub            | HUB-001      | Electronics | $49.99    | 150   |
| 5          | Monitor Stand        | STAND-001    | Accessories | $39.99    | 100   |
| 6          | Webcam HD            | CAM-001      | Electronics | $79.99    | 80    |
| 7          | Desk Lamp            | LAMP-001     | Accessories | $34.99    | 120   |
| 8          | Cable Management Kit | CABLE-001    | Accessories | $19.99    | 300   |

### Orders

The sample includes 6 orders with varying statuses:

| Order ID | Customer       | Date       | Status     | Total     |
|----------|----------------|------------|------------|-----------|
| 1        | John Doe       | 2024-06-01 | completed  | $1,500.37 |
| 2        | Jane Smith     | 2024-06-02 | shipped    | $182.38   |
| 3        | Bob Johnson    | 2024-06-03 | processing | $1,348.98 |
| 4        | John Doe       | 2024-06-05 | pending    | $58.98    |
| 5        | Alice Williams | 2024-06-06 | completed  | $233.38   |
| 6        | Charlie Brown  | 2024-06-07 | shipped    | $42.78    |

## Configuration

### Pipeline Parameters

The pipeline accepts the following parameters:

| Parameter          | Description                  | Default Value                                                                            |
|--------------------|------------------------------|------------------------------------------------------------------------------------------|
| `ConnectionString` | PostgreSQL connection string | `Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=NPipelineSamples` |

### Write Options

The sample demonstrates various options:

```csharp
var sink = PostgresConnector.Sink<Customer>(connectionString, "customers_copy", o => o with
{
    WriteStrategy = PostgresWriteStrategy.Batch,  // PerRow, Batch or Copy
    BatchSize = 100,
    // Transaction = SqlTransactionMode.PerBatch (the default): each batch lands whole and is retried safely
    // Upsert = SqlUpsert.On("customer_id"): INSERT ... ON CONFLICT
});
```

### Write Strategy Comparison

| Strategy   | Performance | Memory Usage | Best For                              |
|------------|-------------|--------------|---------------------------------------|
| **PerRow** | Slowest     | Lowest       | Small volumes, debugging              |
| **Batch**  | Good        | Moderate     | Most scenarios, balanced performance  |
| **Copy**   | Fastest     | Moderate     | Large loads                           |

## Code Examples

### Reading from PostgreSQL

```csharp
var sql = "SELECT customer_id, first_name, last_name, email FROM customers ORDER BY customer_id";
var sourceNode = PostgresConnector.Source<Customer>(connectionString, sql);

await foreach (var customer in sourceNode.OpenStream(new PipelineContext(), cancellationToken))
{
    Console.WriteLine($"Customer: {customer.FullName}");
}
```

### Writing to PostgreSQL

```csharp
var sinkNode = PostgresConnector.Sink<Customer>(connectionString, "customers_copy", o => o with { BatchSize = 100 });
var context = new PipelineContext();

await sinkNode.ConsumeAsync(sourceNode.OpenStream(context, cancellationToken), context, cancellationToken);
```

### Attribute-Based Mapping

```csharp
public class Customer
{
    [PostgresColumn("customer_id")]
    public int CustomerId { get; set; }

    [PostgresColumn("first_name")]
    public string FirstName { get; set; } = string.Empty;

    [PostgresColumn("last_name")]
    public string LastName { get; set; } = string.Empty;

    // Computed property (not written)
    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

## Extending the Sample

### Adding New Models

Create a new model class; members map to snake_case columns, and `[PostgresColumn]` names one explicitly:

```csharp
public class YourModel
{
    public int Id { get; set; }

    [PostgresColumn("name")]
    public string Name { get; set; } = string.Empty;

    public DateTime CreatedAt { get; set; }  // created_at
}

var sink = PostgresConnector.Sink<YourModel>(connectionString, "your_table");
```

### Adding Custom Transformations

Extend the pipeline to add custom transformations:

```csharp
private async IAsyncEnumerable<EnrichedOrder> EnrichOrders(IAsyncEnumerable<Order> orders)
{
    await foreach (var order in orders)
    {
        var enriched = new EnrichedOrder
        {
            // Copy fields
            OrderId = order.OrderId,
            CustomerId = order.CustomerId,

            // Add computed fields
            Priority = CalculatePriority(order),
            ProcessingTime = CalculateProcessingTime(order)
        };

        yield return enriched;
    }
}
```

### Using Different Write Strategies

Experiment with different strategies:

```csharp
// PerRow for small volumes
var perRow = PostgresConnector.Sink<Customer>(connectionString, "customers", o => o with { WriteStrategy = PostgresWriteStrategy.PerRow });

// Batch for most scenarios
var batch = PostgresConnector.Sink<Customer>(connectionString, "customers", o => o with { WriteStrategy = PostgresWriteStrategy.Batch, BatchSize = 100 });

// Binary COPY for large loads
var copy = PostgresConnector.Sink<Customer>(connectionString, "customers", o => o with { WriteStrategy = PostgresWriteStrategy.Copy });
```

## Troubleshooting

### Connection Issues

If you get connection errors:

1. Verify PostgreSQL is running: `psql -h localhost -U postgres`
2. Check connection string in [`Program.cs`](Program.cs) or environment variable
3. Ensure database exists: `CREATE DATABASE NPipelineSamples;`
4. Verify user has necessary permissions

### Table Not Found Errors

If you get table not found errors:

1. Check that tables are created in the pipeline setup
2. Verify the table name passed to `PostgresConnector.Sink` (and that column names match, `customer_id` not `CustomerId`)
3. Check schema name (default is "public")

### Permission Errors

If you get permission errors:

1. Ensure user has CREATE TABLE permissions
2. Check INSERT permissions on target tables
3. Verify SELECT permissions on source tables

### Performance Issues

If performance is poor:

1. Use the Batch or Copy write strategy instead of PerRow
2. Increase `BatchSize`
3. Add appropriate indexes on your tables
4. Npgsql pools connections per connection string, so reuse one connection string across nodes

## Best Practices Demonstrated

1. **Separation of Concerns**: Each model and operation has a single responsibility
2. **Type Safety**: Strongly-typed data models prevent runtime errors
3. **Attribute-Based Mapping**: Clear and explicit mapping configuration
4. **Error Handling**: Comprehensive error handling with connection testing
5. **Configurability**: Pipeline behavior can be configured through parameters
6. **Performance Optimization**: Demonstrates different write strategies
7. **Resource Management**: Proper disposal of database connections
8. **Security**: Password masking in connection strings for display

## Dependencies

This sample uses the following NPipeline packages:

- `NPipeline`: Core pipeline framework
- `NPipeline.Connectors.Postgres`: PostgreSQL source and sink nodes
- `NPipeline.Extensions.DependencyInjection`: DI container integration

External dependencies:

- `Npgsql`: PostgreSQL data provider for .NET
- `Microsoft.Extensions.Hosting`: Host application framework

## Additional Resources

- [PostgreSQL Connector Documentation](../../docs/connectors/postgres.md)
- [SQL Connectors: Shared Behaviour](../../docs/connectors/sql-connectors.md)
- [NPipeline Documentation](../../docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Npgsql Documentation](https://www.npgsql.org/doc/)

## License

This sample code is part of the NPipeline project. See the main [LICENSE](../../LICENSE) file for details.

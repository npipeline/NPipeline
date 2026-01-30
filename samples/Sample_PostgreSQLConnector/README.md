# Sample: PostgreSQL Connector

This sample demonstrates comprehensive PostgreSQL data processing using NPipeline's PostgreSQL connector components. It shows how to read data from PostgreSQL tables, transform it, and write to PostgreSQL tables using various strategies and configurations.

## Overview

The PostgreSQL Connector sample implements a complete data processing pipeline that:

1. **Reads** customer, product, and order data from PostgreSQL tables using `PostgresSourceNode<T>`
2. **Transforms** and aggregates order data into summaries
3. **Writes** processed data to PostgreSQL tables using `PostgresSinkNode<T>`
4. **Demonstrates** different write strategies (PerRow, Batch)
5. **Shows** attribute-based mapping with `PostgresTableAttribute` and `PostgresColumnAttribute`

## Key Concepts Demonstrated

### PostgreSQL Connector Components

- **PostgresSourceNode<T>**: Reads PostgreSQL data and deserializes it to strongly-typed objects
- **PostgresSinkNode<T>**: Serializes objects and writes them to PostgreSQL tables
- **PostgresConfiguration**: Configuration for connection strings, write strategies, and other options

### Attribute-Based Mapping

- **PostgresTableAttribute**: Maps a C# class to a PostgreSQL table
- **PostgresColumnAttribute**: Maps C# properties to PostgreSQL columns
- **PrimaryKey**: Specifies primary key columns
- **Convention-based mapping**: Automatic snake_case conversion for unmapped properties

### Write Strategies

- **PerRow**: Writes one row at a time (slowest, best for small batches)
- **Batch**: Writes in batches (good balance of performance and memory)

### Data Models

The sample includes realistic business models:

- **Customer**: Customer information with contact details
- **Product**: Product catalog with pricing and inventory
- **Order**: Order header with customer and shipping information
- **OrderItem**: Order line items linking orders to products
- **OrderSummary**: Aggregated order data for reporting

## Project Structure

```
Sample_PostgreSQLConnector/
├── Models.cs                          # Data model classes (Customer, Product, Order, etc.)
├── PostgreSQLConnectorPipeline.cs       # Main pipeline definition
├── Program.cs                          # Entry point and execution logic
├── Sample_PostgreSQLConnector.csproj    # Project configuration
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
   cd samples/Sample_PostgreSQLConnector
   ```

2. Build and run the sample:

   ```bash
   dotnet run
   ```

### Expected Output

The pipeline will:

1. Create database tables (customers, products, orders, order_items, order_summaries)
2. Seed sample data (5 customers, 8 products, 6 orders with items)
3. Process and copy customers and products
4. Generate order summaries by joining orders with customers and items
5. Demonstrate different write strategies with performance comparison

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
  PerRow strategy: 150 ms
  Batch strategy (size 25): 45 ms
  Performance improvement: Batch is 3.33x faster than PerRow

=== Pipeline Execution Summary ===
  Total time: 850 ms
  Customers processed: 5
  Products processed: 8
  Order summaries generated: 6

Pipeline execution completed successfully!
```

## Sample Data

### Customers

The sample includes 5 customers with complete contact information:

| Customer ID | Name | Email | City | State | Country |
|-------------|------|-------|------|-------|---------|
| 1 | John Doe | <john.doe@example.com> | Springfield | IL | USA |
| 2 | Jane Smith | <jane.smith@example.com> | Chicago | IL | USA |
| 3 | Bob Johnson | <bob.johnson@example.com> | Milwaukee | WI | USA |
| 4 | Alice Williams | <alice.williams@example.com> | Minneapolis | MN | USA |
| 5 | Charlie Brown | <charlie.brown@example.com> | Des Moines | IA | USA |

### Products

The sample includes 8 products across categories:

| Product ID | Name | SKU | Category | Price | Stock |
|------------|------|-----|----------|-------|-------|
| 1 | Laptop Computer | LAPTOP-001 | Electronics | $1,299.99 | 50 |
| 2 | Wireless Mouse | MOUSE-001 | Electronics | $29.99 | 200 |
| 3 | Mechanical Keyboard | KEYBOARD-001 | Electronics | $149.99 | 75 |
| 4 | USB-C Hub | HUB-001 | Electronics | $49.99 | 150 |
| 5 | Monitor Stand | STAND-001 | Accessories | $39.99 | 100 |
| 6 | Webcam HD | CAM-001 | Electronics | $79.99 | 80 |
| 7 | Desk Lamp | LAMP-001 | Accessories | $34.99 | 120 |
| 8 | Cable Management Kit | CABLE-001 | Accessories | $19.99 | 300 |

### Orders

The sample includes 6 orders with varying statuses:

| Order ID | Customer | Date | Status | Total |
|----------|----------|------|--------|-------|
| 1 | John Doe | 2024-06-01 | completed | $1,500.37 |
| 2 | Jane Smith | 2024-06-02 | shipped | $182.38 |
| 3 | Bob Johnson | 2024-06-03 | processing | $1,348.98 |
| 4 | John Doe | 2024-06-05 | pending | $58.98 |
| 5 | Alice Williams | 2024-06-06 | completed | $233.38 |
| 6 | Charlie Brown | 2024-06-07 | shipped | $42.78 |

## Configuration

### Pipeline Parameters

The pipeline accepts the following parameters:

| Parameter | Description | Default Value |
|-----------|-------------|---------------|
| `ConnectionString` | PostgreSQL connection string | `Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=NPipelineSamples` |

### PostgresConfiguration Options

The sample demonstrates various configuration options:

```csharp
var config = new PostgresConfiguration
{
    ConnectionString = "your_connection_string",
    WriteStrategy = PostgresWriteStrategy.Batch,  // PerRow or Batch in the free connector
    BatchSize = 100,                            // For Batch strategy
    // Additional options available:
    // CheckpointStrategy = CheckpointStrategy.InMemory,
    // DeliverySemantic = DeliverySemantic.AtLeastOnce,
};
```

### Write Strategy Comparison

| Strategy | Performance | Memory Usage | Best For |
|----------|-------------|--------------|----------|
| **PerRow** | Slowest | Lowest | Small batches, per-row error handling |
| **Batch** | Good | Moderate | Most scenarios, balanced performance |

## Code Examples

### Reading from PostgreSQL

```csharp
var config = new PostgresConfiguration
{
    ConnectionString = connectionString
};

var sql = "SELECT customer_id, first_name, last_name, email FROM customers ORDER BY customer_id";
var sourceNode = new PostgresSourceNode<Customer>(connectionString, sql, configuration: config);

var context = new PipelineContext();
await foreach (var customer in sourceNode.Initialize(context, cancellationToken))
{
    Console.WriteLine($"Customer: {customer.FullName}");
}
```

### Writing to PostgreSQL

```csharp
var config = new PostgresConfiguration
{
    ConnectionString = connectionString,
    WriteStrategy = PostgresWriteStrategy.Batch,
    BatchSize = 100
};

var sinkNode = new PostgresSinkNode<Customer>(connectionString, "customers_copy", configuration: config);
var context = new PipelineContext();

await sinkNode.ExecuteAsync(sourceNode.Initialize(context, cancellationToken), context, cancellationToken);
```

### Attribute-Based Mapping

```csharp
[PostgresTable("customers")]
public class Customer
{
    [PostgresColumn("customer_id", PrimaryKey = true)]
    public int CustomerId { get; set; }

    [PostgresColumn("first_name")]
    public string FirstName { get; set; } = string.Empty;

    [PostgresColumn("last_name")]
    public string LastName { get; set; } = string.Empty;

    // Computed property (not mapped)
    public string FullName => $"{FirstName} {LastName}";
}
```

## Extending the Sample

### Adding New Models

Create a new model class with attributes:

```csharp
[PostgresTable("your_table")]
public class YourModel
{
    [PostgresColumn("id", PrimaryKey = true)]
    public int Id { get; set; }

    [PostgresColumn("name")]
    public string Name { get; set; } = string.Empty;

    [PostgresColumn("created_at")]
    public DateTime CreatedAt { get; set; }
}
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
// PerRow for small batches
var perRowConfig = new PostgresConfiguration
{
    ConnectionString = connectionString,
    WriteStrategy = PostgresWriteStrategy.PerRow
};

// Batch for most scenarios
var batchConfig = new PostgresConfiguration
{
    ConnectionString = connectionString,
    WriteStrategy = PostgresWriteStrategy.Batch,
    BatchSize = 100
};

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
2. Verify table names match the [`PostgresTableAttribute`](Models.cs)
3. Check schema name (default is "public")

### Permission Errors

If you get permission errors:

1. Ensure user has CREATE TABLE permissions
2. Check INSERT permissions on target tables
3. Verify SELECT permissions on source tables

### Performance Issues

If performance is poor:

1. Use Batch write strategy instead of PerRow
2. Increase batch size for Batch strategy
3. Add appropriate indexes on your tables
4. Consider using connection pooling

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
- `NPipeline.Connectors.PostgreSQL`: PostgreSQL source and sink nodes
- `NPipeline.Extensions.DependencyInjection`: DI container integration

External dependencies:

- `Npgsql`: PostgreSQL data provider for .NET
- `Microsoft.Extensions.Hosting`: Host application framework

## Additional Resources

- [PostgreSQL Connector Documentation](../../docs/connectors/postgresql.md)
- [NPipeline Documentation](../../docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Npgsql Documentation](https://www.npgsql.org/doc/)

## License

This sample code is part of the NPipeline project. See the main [LICENSE](../../LICENSE) file for details.

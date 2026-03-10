using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Nodes;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using Snowflake.Data.Client;

namespace Sample_SnowflakeConnector;

/// <summary>
///     Pipeline demonstrating Snowflake connector features including:
///     - Reading from Snowflake using SnowflakeSourceNode
///     - Writing to Snowflake using SnowflakeSinkNode
///     - PerRow, Batch, and StagedCopy write strategies
///     - Attribute-based and convention-based mapping
///     - Custom mappers
///     - Connection pooling
///     - Upsert (MERGE) operations
///     - Error handling
///     - Transformations and multiple table processing
/// </summary>
public sealed class SnowflakeConnectorPipeline
{
    private readonly string _connectionString;

    public SnowflakeConnectorPipeline(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    ///     Gets a description of the pipeline and its features.
    /// </summary>
    public static string GetDescription()
    {
        return """
               Snowflake Connector Sample Pipeline
               ====================================

               This pipeline demonstrates the following features:

               1. Reading from Snowflake
                  - SnowflakeSourceNode for data retrieval
                  - Parameterized queries
                  - Streaming results

               2. Writing to Snowflake
                  - SnowflakeSinkNode for data insertion
                  - PerRow write strategy (row-by-row)
                  - Batch write strategy (batched inserts)
                  - StagedCopy write strategy (PUT + COPY INTO)

               3. Mapping Strategies
                  - Attribute-based mapping (SnowflakeTable, SnowflakeColumn, Column, IgnoreColumn)
                  - Convention-based mapping (PascalCase to UPPER_SNAKE_CASE)
                  - Custom mappers (Func<T, IEnumerable<DatabaseParameter>>)

               4. Connection Management
                  - Connection pooling
                  - Snowflake cloud connectivity
                  - Query tagging for observability

               5. Upsert Operations
                  - MERGE-based insert-or-update semantics
                  - Configurable key columns

               6. Error Handling
                  - Retry logic for transient errors
                  - Row-level error handling
                  - Transaction support (PerRow and Batch)

               7. Transformations
                  - Data enrichment
                  - Aggregation
                  - Multiple table processing
               """;
    }

    /// <summary>
    ///     Executes the pipeline with the given context and cancellation token.
    /// </summary>
    public async Task ConsumeAsync(PipelineContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Starting Snowflake Connector Pipeline...");
        Console.WriteLine();

        // Step 1: Initialize database schema
        await InitializeDatabaseSchemaAsync(cancellationToken);
        Console.WriteLine();

        // Step 2: Demonstrate Batch write strategy
        await DemonstrateBatchWriteStrategyAsync(cancellationToken);
        Console.WriteLine();

        // Step 3: Demonstrate PerRow write strategy
        await DemonstratePerRowWriteStrategyAsync(cancellationToken);
        Console.WriteLine();

        // Step 4: Demonstrate StagedCopy write strategy (PUT + COPY INTO)
        await DemonstrateStagedCopyWriteStrategyAsync(cancellationToken);
        Console.WriteLine();

        // Step 5: Demonstrate attribute-based mapping (read customers)
        await DemonstrateAttributeBasedMappingAsync(cancellationToken);
        Console.WriteLine();

        // Step 6: Demonstrate convention-based mapping (order summaries via JOIN)
        await DemonstrateConventionBasedMappingAsync(cancellationToken);
        Console.WriteLine();

        // Step 7: Demonstrate upsert (MERGE)
        await DemonstrateUpsertAsync(cancellationToken);
        Console.WriteLine();

        // Step 8: Clean up test tables
        await CleanupAsync(cancellationToken);
        Console.WriteLine();

        Console.WriteLine("Pipeline execution completed successfully!");
    }

    /// <summary>
    ///     Initializes the Snowflake database schema with required tables.
    /// </summary>
    private async Task InitializeDatabaseSchemaAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 1: Setting up schema...");
        Console.WriteLine("----------------------------");

        using var connection = new SnowflakeDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Create CUSTOMERS table
        var createCustomersSql = @"
            CREATE TABLE IF NOT EXISTS PUBLIC.CUSTOMERS (
                ID NUMBER AUTOINCREMENT PRIMARY KEY,
                FIRST_NAME VARCHAR(100) NOT NULL,
                LAST_NAME VARCHAR(100) NOT NULL,
                EMAIL VARCHAR(255) NOT NULL,
                PHONE_NUMBER VARCHAR(50),
                CREATED_AT TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                STATUS VARCHAR(50) NOT NULL DEFAULT 'Active'
            )";

        using (var command = connection.CreateCommand())
        {
            command.CommandText = createCustomersSql;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        // Create ORDERS table
        var createOrdersSql = @"
            CREATE TABLE IF NOT EXISTS PUBLIC.ORDERS (
                ORDER_ID NUMBER AUTOINCREMENT PRIMARY KEY,
                CUSTOMER_ID NUMBER NOT NULL,
                ORDER_DATE TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                AMOUNT NUMBER(18,2) NOT NULL,
                STATUS VARCHAR(50) NOT NULL DEFAULT 'Pending',
                SHIPPING_ADDRESS VARCHAR(500),
                NOTES VARCHAR(1000)
            )";

        using (var command = connection.CreateCommand())
        {
            command.CommandText = createOrdersSql;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        // Create ENRICHED_CUSTOMERS table
        var createEnrichedSql = @"
            CREATE TABLE IF NOT EXISTS PUBLIC.ENRICHED_CUSTOMERS (
                CUSTOMER_ID NUMBER PRIMARY KEY,
                FULL_NAME VARCHAR(255) NOT NULL,
                EMAIL VARCHAR(255) NOT NULL,
                PHONE_NUMBER VARCHAR(50),
                CREATED_AT TIMESTAMP_NTZ NOT NULL,
                STATUS VARCHAR(50) NOT NULL,
                TOTAL_ORDERS NUMBER NOT NULL DEFAULT 0,
                TOTAL_SPENT NUMBER(18,2) NOT NULL DEFAULT 0,
                AVERAGE_ORDER_VALUE NUMBER(18,2) NOT NULL DEFAULT 0,
                CUSTOMER_TIER VARCHAR(50) NOT NULL DEFAULT 'Bronze',
                LAST_ORDER_DATE TIMESTAMP_NTZ,
                ENRICHMENT_DATE TIMESTAMP_NTZ NOT NULL
            )";

        using (var command = connection.CreateCommand())
        {
            command.CommandText = createEnrichedSql;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        Console.WriteLine("  ✓ Created CUSTOMERS, ORDERS, and ENRICHED_CUSTOMERS tables");
    }

    /// <summary>
    ///     Demonstrates Batch write strategy.
    /// </summary>
    private async Task DemonstrateBatchWriteStrategyAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 2: Writing customers (Batch strategy)...");
        Console.WriteLine("----------------------------------------------");

        var customers = new List<Customer>
        {
            new()
            {
                FirstName = "John",
                LastName = "Doe",
                Email = "john.doe@example.com",
                PhoneNumber = "555-0101",
                CreatedAt = DateTime.UtcNow.AddDays(-365),
                Status = "Active",
            },
            new()
            {
                FirstName = "Jane",
                LastName = "Smith",
                Email = "jane.smith@example.com",
                PhoneNumber = "555-0102",
                CreatedAt = DateTime.UtcNow.AddDays(-180),
                Status = "Active",
            },
            new()
            {
                FirstName = "Bob",
                LastName = "Johnson",
                Email = "bob.johnson@example.com",
                PhoneNumber = "555-0103",
                CreatedAt = DateTime.UtcNow.AddDays(-90),
                Status = "Inactive",
            },
            new()
            {
                FirstName = "Alice",
                LastName = "Williams",
                Email = "alice.williams@example.com",
                PhoneNumber = "555-0104",
                CreatedAt = DateTime.UtcNow.AddDays(-30),
                Status = "Active",
            },
            new()
            {
                FirstName = "Charlie",
                LastName = "Brown",
                Email = "charlie.brown@example.com",
                PhoneNumber = "555-0105",
                CreatedAt = DateTime.UtcNow.AddDays(-7),
                Status = "Active",
            },
        };

        var configuration = new SnowflakeConfiguration
        {
            WriteStrategy = SnowflakeWriteStrategy.Batch,
            BatchSize = 10,
            UseTransaction = true,
            CommandTimeout = 60,
            Schema = "PUBLIC",
        };

        var sinkNode = new SnowflakeSinkNode<Customer>(
            _connectionString,
            "CUSTOMERS",
            configuration);

        Console.WriteLine($"  Writing {customers.Count} customers using Batch strategy...");

        var startTime = DateTime.Now;
        var dataStream = new InMemoryDataStream<Customer>(customers);
        await sinkNode.ConsumeAsync(dataStream, null!, cancellationToken);
        var elapsed = DateTime.Now - startTime;

        Console.WriteLine($"  ✓ Inserted {customers.Count} customers in {elapsed.TotalSeconds:F2}s");
        Console.WriteLine("    - Strategy: Batch (multi-row INSERT)");
        Console.WriteLine("    - Transaction: Enabled");
    }

    /// <summary>
    ///     Demonstrates PerRow write strategy.
    /// </summary>
    private async Task DemonstratePerRowWriteStrategyAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 3: Writing additional orders (PerRow strategy)...");
        Console.WriteLine("------------------------------------------------------");

        var orders = new List<Order>
        {
            new()
            {
                CustomerId = 1,
                OrderDate = DateTime.UtcNow.AddDays(-30),
                Amount = 150.00m,
                Status = "Shipped",
                ShippingAddress = "123 Main St",
                Notes = "Express delivery",
            },
            new()
            {
                CustomerId = 2,
                OrderDate = DateTime.UtcNow.AddDays(-15),
                Amount = 89.99m,
                Status = "Pending",
                ShippingAddress = "456 Oak Ave",
                Notes = "Gift wrapping requested",
            },
            new()
            {
                CustomerId = 1,
                OrderDate = DateTime.UtcNow.AddDays(-5),
                Amount = 2500.00m,
                Status = "Shipped",
                ShippingAddress = "123 Main St",
                Notes = "High value order",
            },
        };

        var configuration = new SnowflakeConfiguration
        {
            WriteStrategy = SnowflakeWriteStrategy.PerRow,
            UseTransaction = true,
            CommandTimeout = 60,
            Schema = "PUBLIC",
        };

        var sinkNode = new SnowflakeSinkNode<Order>(
            _connectionString,
            "ORDERS",
            configuration);

        Console.WriteLine($"  Writing {orders.Count} orders using PerRow strategy...");

        var startTime = DateTime.Now;
        var dataStream = new InMemoryDataStream<Order>(orders);
        await sinkNode.ConsumeAsync(dataStream, null!, cancellationToken);
        var elapsed = DateTime.Now - startTime;

        Console.WriteLine($"  ✓ Inserted {orders.Count} orders in {elapsed.TotalSeconds:F2}s");
        Console.WriteLine("    - Strategy: PerRow (row-by-row INSERT)");
        Console.WriteLine("    - Transaction: Enabled");
    }

    /// <summary>
    ///     Demonstrates the StagedCopy write strategy using PUT + COPY INTO.
    /// </summary>
    private async Task DemonstrateStagedCopyWriteStrategyAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 4: Bulk loading orders (Staged COPY strategy)...");
        Console.WriteLine("-----------------------------------------------------");

        // Generate a larger batch of orders for the staged copy demo
        var orders = new List<Order>();
        var random = new Random(42);

        for (var i = 1; i <= 100; i++)
        {
            orders.Add(new Order
            {
                CustomerId = random.Next(1, 6),
                OrderDate = DateTime.UtcNow.AddDays(-random.Next(1, 365)),
                Amount = Math.Round((decimal)(random.NextDouble() * 2000), 2),
                Status = random.Next(0, 10) > 3
                    ? "Shipped"
                    : "Pending",
                ShippingAddress = $"{random.Next(100, 999)} Elm Blvd, City {random.Next(1, 50)}",
                Notes = $"Bulk order #{i}",
            });
        }

        var configuration = new SnowflakeConfiguration
        {
            WriteStrategy = SnowflakeWriteStrategy.StagedCopy,
            StageName = "~", // User stage
            FileFormat = "CSV",
            CopyCompression = "GZIP",
            PurgeAfterCopy = true,
            CommandTimeout = 120,
            Schema = "PUBLIC",
        };

        var sinkNode = new SnowflakeSinkNode<Order>(
            _connectionString,
            "ORDERS",
            configuration);

        Console.WriteLine($"  Bulk loading {orders.Count} orders using PUT + COPY INTO...");

        var startTime = DateTime.Now;
        var dataStream = new InMemoryDataStream<Order>(orders);
        await sinkNode.ConsumeAsync(dataStream, null!, cancellationToken);
        var elapsed = DateTime.Now - startTime;

        Console.WriteLine($"  ✓ Loaded {orders.Count} orders via PUT+COPY in {elapsed.TotalSeconds:F2}s");
        Console.WriteLine("    - Strategy: StagedCopy (PUT + COPY INTO)");
        Console.WriteLine("    - Stage: User stage (~)");
        Console.WriteLine("    - Format: CSV, Compression: GZIP");
        Console.WriteLine("    - Purge after copy: Enabled");
    }

    /// <summary>
    ///     Demonstrates attribute-based mapping by reading customers.
    /// </summary>
    private async Task DemonstrateAttributeBasedMappingAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 5: Reading and transforming customers...");
        Console.WriteLine("----------------------------------------------");

        var sourceConfiguration = new SnowflakeConfiguration
        {
            StreamResults = true,
            CommandTimeout = 60,
        };

        var sourceNode = new SnowflakeSourceNode<Customer>(
            _connectionString,
            "SELECT * FROM PUBLIC.CUSTOMERS ORDER BY ID",
            sourceConfiguration);

        Console.WriteLine("  Reading customers with attribute-based mapping...");
        Console.WriteLine("    - SnowflakeTable: CUSTOMERS (Schema: PUBLIC)");
        Console.WriteLine("    - SnowflakeColumn: ID (PrimaryKey)");
        Console.WriteLine("    - SnowflakeColumn: FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER");
        Console.WriteLine("    - SnowflakeColumn: CREATED_AT (TIMESTAMP_NTZ)");
        Console.WriteLine("    - IgnoreColumn: FullName (computed property)");

        var customers = new List<Customer>();

        await foreach (var customer in sourceNode.OpenStream(null!, cancellationToken))
        {
            customers.Add(customer);
            Console.WriteLine($"    - Read: {customer.FullName} (ID: {customer.Id}, Email: {customer.Email})");
        }

        Console.WriteLine($"  ✓ Read {customers.Count} customers, enriched with computed fields");
    }

    /// <summary>
    ///     Demonstrates convention-based mapping using an aggregation query.
    /// </summary>
    private async Task DemonstrateConventionBasedMappingAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 6: Querying order summaries...");
        Console.WriteLine("------------------------------------");

        var sourceConfiguration = new SnowflakeConfiguration
        {
            StreamResults = true,
            CommandTimeout = 60,
        };

        // Use a JOIN query to aggregate order data per customer
        var query = @"
            SELECT
                c.ID AS CUSTOMERID,
                c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMERNAME,
                COUNT(o.ORDER_ID) AS TOTALORDERS,
                COALESCE(SUM(o.AMOUNT), 0) AS TOTALAMOUNT
            FROM PUBLIC.CUSTOMERS c
            LEFT JOIN PUBLIC.ORDERS o ON c.ID = o.CUSTOMER_ID
            GROUP BY c.ID, c.FIRST_NAME, c.LAST_NAME
            ORDER BY TOTALAMOUNT DESC";

        var sourceNode = new SnowflakeSourceNode<OrderSummary>(
            _connectionString,
            query,
            sourceConfiguration);

        Console.WriteLine("  Reading order summaries with convention-based mapping...");
        Console.WriteLine("    - No attributes used on OrderSummary class");
        Console.WriteLine("    - Property names map to Snowflake column aliases (case-insensitive)");

        var summaries = new List<OrderSummary>();

        await foreach (var summary in sourceNode.OpenStream(null!, cancellationToken))
        {
            summaries.Add(summary);
            Console.WriteLine($"    - {summary.CustomerName}: {summary.TotalOrders} orders, ${summary.TotalAmount:N2}");
        }

        Console.WriteLine($"  ✓ Retrieved {summaries.Count} aggregated order summaries");
    }

    /// <summary>
    ///     Demonstrates upsert (MERGE) operations.
    /// </summary>
    private async Task DemonstrateUpsertAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 7: Upserting updated customers (MERGE)...");
        Console.WriteLine("------------------------------------------------");

        // Create enriched customer records for upsert
        var enrichedCustomers = new List<EnrichedCustomer>
        {
            new()
            {
                CustomerId = 1,
                FullName = "John Doe",
                Email = "john.doe@example.com",
                PhoneNumber = "555-0101",
                CreatedAt = DateTime.UtcNow.AddDays(-365),
                Status = "Active",
                TotalOrders = 2,
                TotalSpent = 2650.00m,
                AverageOrderValue = 1325.00m,
                CustomerTier = "Gold",
                LastOrderDate = DateTime.UtcNow.AddDays(-5),
                EnrichmentDate = DateTime.UtcNow,
            },
            new()
            {
                CustomerId = 2,
                FullName = "Jane Smith",
                Email = "jane.smith@example.com",
                PhoneNumber = "555-0102",
                CreatedAt = DateTime.UtcNow.AddDays(-180),
                Status = "Active",
                TotalOrders = 1,
                TotalSpent = 89.99m,
                AverageOrderValue = 89.99m,
                CustomerTier = "Bronze",
                LastOrderDate = DateTime.UtcNow.AddDays(-15),
                EnrichmentDate = DateTime.UtcNow,
            },
            new()
            {
                CustomerId = 3,
                FullName = "Bob Johnson",
                Email = "bob.johnson@example.com",
                PhoneNumber = "555-0103",
                CreatedAt = DateTime.UtcNow.AddDays(-90),
                Status = "Inactive",
                TotalOrders = 0,
                TotalSpent = 0m,
                AverageOrderValue = 0m,
                CustomerTier = "Bronze",
                LastOrderDate = null,
                EnrichmentDate = DateTime.UtcNow,
            },
        };

        var configuration = new SnowflakeConfiguration
        {
            WriteStrategy = SnowflakeWriteStrategy.Batch,
            UseUpsert = true,
            OnMergeAction = OnMergeAction.Update,
            UseTransaction = true,
            CommandTimeout = 60,
            Schema = "PUBLIC",
        };

        var sinkNode = new SnowflakeSinkNode<EnrichedCustomer>(
            _connectionString,
            "ENRICHED_CUSTOMERS",
            configuration);

        Console.WriteLine($"  Merging {enrichedCustomers.Count} enriched customer records...");

        var startTime = DateTime.Now;
        var dataStream = new InMemoryDataStream<EnrichedCustomer>(enrichedCustomers);
        await sinkNode.ConsumeAsync(dataStream, null!, cancellationToken);
        var elapsed = DateTime.Now - startTime;

        Console.WriteLine($"  ✓ Merged {enrichedCustomers.Count} customer updates in {elapsed.TotalSeconds:F2}s");
        Console.WriteLine("    - Strategy: Batch with UseUpsert (MERGE INTO)");
        Console.WriteLine("    - On match: Update all non-key columns");
    }

    /// <summary>
    ///     Cleans up test tables.
    /// </summary>
    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 8: Cleaning up...");
        Console.WriteLine("-----------------------");

        using var connection = new SnowflakeDbConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "DROP TABLE IF EXISTS PUBLIC.ENRICHED_CUSTOMERS";
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "DROP TABLE IF EXISTS PUBLIC.ORDERS";
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "DROP TABLE IF EXISTS PUBLIC.CUSTOMERS";
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        Console.WriteLine("  ✓ Dropped test tables");
    }
}

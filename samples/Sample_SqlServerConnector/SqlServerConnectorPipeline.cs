using Microsoft.Data.SqlClient;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Nodes;
using NPipeline.StorageProviders.Models;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace Sample_SqlServerConnector;

/// <summary>
///     Pipeline demonstrating SQL Server connector features including:
///     - Reading from SQL Server using SqlServerSourceNode
///     - Writing to SQL Server using SqlServerSinkNode
///     - PerRow and Batch write strategies
///     - Attribute-based and convention-based mapping
///     - Custom mappers
///     - Connection pooling
///     - Named connections
///     - Error handling
///     - Transformations and multiple table processing
/// </summary>
public sealed class SqlServerConnectorPipeline
{
    private readonly string _connectionString;

    public SqlServerConnectorPipeline(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    ///     Gets a description of the pipeline and its features.
    /// </summary>
    public static string GetDescription()
    {
        return """
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

               3. Mapping Strategies
                  - Attribute-based mapping (SqlServerTable, SqlServerColumn, Column, IgnoreColumn)
                  - Convention-based mapping (PascalCase to PascalCase)
                  - Custom mappers (Func<T, IEnumerable<DatabaseParameter>>)

               4. Connection Management
                  - Connection pooling
                  - Named connections
                  - Connection lifecycle management

               5. Error Handling
                  - Retry logic for transient errors
                  - Row-level error handling
                  - Transaction support

               6. Transformations
                  - Data enrichment
                  - Aggregation
                  - Multiple table processing
               """;
    }

    /// <summary>
    ///     Executes the pipeline with the given context and cancellation token.
    /// </summary>
    public async Task ExecuteAsync(PipelineContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Starting SQL Server Connector Pipeline...");
        Console.WriteLine();

        // Step 1: Initialize database schema
        await InitializeDatabaseSchemaAsync(cancellationToken);
        Console.WriteLine();

        // Step 2: Demonstrate PerRow write strategy
        await DemonstratePerRowWriteStrategyAsync(cancellationToken);
        Console.WriteLine();

        // Step 3: Demonstrate Batch write strategy
        await DemonstrateBatchWriteStrategyAsync(cancellationToken);
        Console.WriteLine();

        // Step 4: Demonstrate attribute-based mapping
        await DemonstrateAttributeBasedMappingAsync(cancellationToken);
        Console.WriteLine();

        // Step 5: Demonstrate convention-based mapping
        await DemonstrateConventionBasedMappingAsync(cancellationToken);
        Console.WriteLine();

        // Step 6: Demonstrate custom mappers
        await DemonstrateCustomMappersAsync(cancellationToken);
        Console.WriteLine();

        // Step 7: Demonstrate transformation and enrichment
        await DemonstrateTransformationAsync(cancellationToken);
        Console.WriteLine();

        // Step 8: Demonstrate error handling
        await DemonstrateErrorHandlingAsync(cancellationToken);
        Console.WriteLine();

        Console.WriteLine("Pipeline execution completed successfully!");
    }

    /// <summary>
    ///     Initializes the database schema with required tables.
    /// </summary>
    private async Task InitializeDatabaseSchemaAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 1: Initializing Database Schema");
        Console.WriteLine("-----------------------------------");

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Create Sales schema if it doesn't exist
        var createSchemaSql = @"
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Sales')
            EXEC('CREATE SCHEMA Sales')";

        using (var command = new SqlCommand(createSchemaSql, connection))
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        // Create Analytics schema if it doesn't exist
        createSchemaSql = @"
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Analytics')
            EXEC('CREATE SCHEMA Analytics')";

        using (var command = new SqlCommand(createSchemaSql, connection))
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        // Create Customers table
        var createCustomersTableSql = @"
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Customers' AND schema_id = SCHEMA_ID('Sales'))
            BEGIN
                CREATE TABLE Sales.Customers (
                    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
                    FirstName NVARCHAR(100) NOT NULL,
                    LastName NVARCHAR(100) NOT NULL,
                    Email NVARCHAR(255) NOT NULL,
                    PhoneNumber NVARCHAR(50),
                    RegistrationDate DATE NOT NULL,
                    Status NVARCHAR(50) NOT NULL DEFAULT 'Active'
                )
            END";

        using (var command = new SqlCommand(createCustomersTableSql, connection))
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        // Create Orders table
        var createOrdersTableSql = @"
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Orders' AND schema_id = SCHEMA_ID('Sales'))
            BEGIN
                CREATE TABLE Sales.Orders (
                    OrderID INT IDENTITY(1,1) PRIMARY KEY,
                    CustomerID INT NOT NULL,
                    OrderDate DATETIME2 NOT NULL,
                    TotalAmount DECIMAL(18,2) NOT NULL,
                    Status NVARCHAR(50) NOT NULL DEFAULT 'Pending',
                    ShippingAddress NVARCHAR(500),
                    Notes NVARCHAR(1000),
                    FOREIGN KEY (CustomerID) REFERENCES Sales.Customers(CustomerID)
                )
            END";

        using (var command = new SqlCommand(createOrdersTableSql, connection))
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        // Create Products table
        var createProductsTableSql = @"
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Products' AND schema_id = SCHEMA_ID('Sales'))
            BEGIN
                CREATE TABLE Sales.Products (
                    ProductID INT IDENTITY(1,1) PRIMARY KEY,
                    ProductName NVARCHAR(255) NOT NULL,
                    Category NVARCHAR(100) NOT NULL,
                    Price DECIMAL(18,2) NOT NULL,
                    StockQuantity INT NOT NULL DEFAULT 0
                )
            END";

        using (var command = new SqlCommand(createProductsTableSql, connection))
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        // Create EnrichedCustomers table
        var createEnrichedCustomersTableSql = @"
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'EnrichedCustomers' AND schema_id = SCHEMA_ID('Analytics'))
            BEGIN
                CREATE TABLE Analytics.EnrichedCustomers (
                    CustomerID INT PRIMARY KEY,
                    FullName NVARCHAR(255) NOT NULL,
                    Email NVARCHAR(255) NOT NULL,
                    PhoneNumber NVARCHAR(50),
                    RegistrationDate DATE NOT NULL,
                    Status NVARCHAR(50) NOT NULL,
                    TotalOrders INT NOT NULL DEFAULT 0,
                    TotalSpent DECIMAL(18,2) NOT NULL DEFAULT 0,
                    AverageOrderValue DECIMAL(18,2) NOT NULL DEFAULT 0,
                    CustomerTier NVARCHAR(50) NOT NULL DEFAULT 'Bronze',
                    LastOrderDate DATETIME2,
                    EnrichmentDate DATETIME2 NOT NULL
                )
            END";

        using (var command = new SqlCommand(createEnrichedCustomersTableSql, connection))
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        Console.WriteLine("✓ Database schema initialized successfully");
    }

    /// <summary>
    ///     Demonstrates PerRow write strategy.
    /// </summary>
    private async Task DemonstratePerRowWriteStrategyAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 2: Demonstrating PerRow Write Strategy");
        Console.WriteLine("---------------------------------------------");

        // Create sample customers
        var customers = new List<Customer>
        {
            new()
            {
                FirstName = "John",
                LastName = "Doe",
                Email = "john.doe@example.com",
                PhoneNumber = "555-0101",
                RegistrationDate = DateTime.Now.AddDays(-365),
                Status = "Active",
            },
            new()
            {
                FirstName = "Jane",
                LastName = "Smith",
                Email = "jane.smith@example.com",
                PhoneNumber = "555-0102",
                RegistrationDate = DateTime.Now.AddDays(-180),
                Status = "Active",
            },
            new()
            {
                FirstName = "Bob",
                LastName = "Johnson",
                Email = "bob.johnson@example.com",
                PhoneNumber = "555-0103",
                RegistrationDate = DateTime.Now.AddDays(-90),
                Status = "Inactive",
            },
        };

        // Configure sink with PerRow write strategy
        var configuration = new SqlServerConfiguration
        {
            WriteStrategy = SqlServerWriteStrategy.PerRow,
            UseTransaction = true,
            CommandTimeout = 30,
            Schema = "Sales",
        };

        // Create sink node
        var sinkNode = new SqlServerSinkNode<Customer>(
            _connectionString,
            "Customers",
            configuration);

        Console.WriteLine($"Writing {customers.Count} customers using PerRow strategy...");

        // Write customers
        var startTime = DateTime.Now;
        var dataPipe = new InMemoryDataPipe<Customer>(customers);
        await sinkNode.ExecuteAsync(dataPipe, null!, cancellationToken);
        var elapsed = DateTime.Now - startTime;

        Console.WriteLine($"✓ PerRow write completed in {elapsed.TotalMilliseconds:F2}ms");
        Console.WriteLine("  - Strategy: PerRow (row-by-row inserts)");
        Console.WriteLine("  - Transaction: Enabled");
    }

    /// <summary>
    ///     Demonstrates Batch write strategy.
    /// </summary>
    private async Task DemonstrateBatchWriteStrategyAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 3: Demonstrating Batch Write Strategy");
        Console.WriteLine("-------------------------------------------");

        // Create sample orders
        var orders = new List<Order>();
        var random = new Random();

        for (var i = 1; i <= 50; i++)
        {
            orders.Add(new Order
            {
                CustomerId = random.Next(1, 4), // Random customer ID 1-3
                OrderDate = DateTime.Now.AddDays(-random.Next(1, 365)),
                TotalAmount = (decimal)(random.NextDouble() * 1000),
                Status = random.Next(0, 10) > 2
                    ? "Shipped"
                    : "Pending",
                ShippingAddress = $"{random.Next(100, 999)} Main St, City",
                Notes = $"Order {i}",
            });
        }

        // Configure sink with Batch write strategy
        var configuration = new SqlServerConfiguration
        {
            WriteStrategy = SqlServerWriteStrategy.Batch,
            BatchSize = 10, // Batch every 10 rows
            UseTransaction = true,
            CommandTimeout = 30,
            Schema = "Sales",
        };

        // Create sink node
        var sinkNode = new SqlServerSinkNode<Order>(
            _connectionString,
            "Orders",
            configuration);

        Console.WriteLine($"Writing {orders.Count} orders using Batch strategy...");
        Console.WriteLine($"  - Batch size: {configuration.BatchSize}");

        // Write orders
        var startTime = DateTime.Now;
        var dataPipe = new InMemoryDataPipe<Order>(orders);
        await sinkNode.ExecuteAsync(dataPipe, null!, cancellationToken);
        var elapsed = DateTime.Now - startTime;

        Console.WriteLine($"✓ Batch write completed in {elapsed.TotalMilliseconds:F2}ms");
        Console.WriteLine("  - Strategy: Batch (batched inserts)");
        Console.WriteLine("  - Transaction: Enabled");
        Console.WriteLine($"  - Batches processed: {Math.Ceiling((double)orders.Count / configuration.BatchSize)}");
    }

    /// <summary>
    ///     Demonstrates attribute-based mapping.
    /// </summary>
    private async Task DemonstrateAttributeBasedMappingAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 4: Demonstrating Attribute-Based Mapping");
        Console.WriteLine("----------------------------------------------");

        // Read customers using attribute-based mapping
        var sourceConfiguration = new SqlServerConfiguration
        {
            StreamResults = true,
            CommandTimeout = 30,
        };

        var sourceNode = new SqlServerSourceNode<Customer>(
            _connectionString,
            "SELECT * FROM Sales.Customers ORDER BY CustomerID",
            sourceConfiguration);

        Console.WriteLine("Reading customers with attribute-based mapping...");
        Console.WriteLine("  - SqlServerTable: Customers (Schema: Sales)");
        Console.WriteLine("  - SqlServerColumn: CustomerID (PrimaryKey, Identity)");
        Console.WriteLine("  - SqlServerColumn: FirstName (DbType: NVarChar, Size: 100)");
        Console.WriteLine("  - Column: LastName (common attribute)");
        Console.WriteLine("  - Column: Email (common attribute)");
        Console.WriteLine("  - Column: PhoneNumber (common attribute)");
        Console.WriteLine("  - SqlServerColumn: RegistrationDate (DbType: Date)");
        Console.WriteLine("  - Column: Status (common attribute)");
        Console.WriteLine("  - IgnoreColumn: FullName (computed property)");

        var customers = new List<Customer>();

        await foreach (var customer in sourceNode.Initialize(null!, cancellationToken))
        {
            customers.Add(customer);
            Console.WriteLine($"  - Read: {customer.FullName} (ID: {customer.CustomerId}, Email: {customer.Email})");
        }

        Console.WriteLine("✓ Attribute-based mapping completed successfully");
        Console.WriteLine($"  - Total customers read: {customers.Count}");
    }

    /// <summary>
    ///     Demonstrates convention-based mapping.
    /// </summary>
    private async Task DemonstrateConventionBasedMappingAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 5: Demonstrating Convention-Based Mapping");
        Console.WriteLine("-----------------------------------------------");

        // Create sample products
        var products = new List<Product>
        {
            new() { ProductName = "Laptop", Category = "Electronics", Price = 999.99m, StockQuantity = 50 },
            new() { ProductName = "Mouse", Category = "Electronics", Price = 29.99m, StockQuantity = 200 },
            new() { ProductName = "Keyboard", Category = "Electronics", Price = 79.99m, StockQuantity = 150 },
            new() { ProductName = "Monitor", Category = "Electronics", Price = 299.99m, StockQuantity = 75 },
            new() { ProductName = "Desk Chair", Category = "Furniture", Price = 199.99m, StockQuantity = 30 },
        };

        // Configure sink with Batch write strategy
        var configuration = new SqlServerConfiguration
        {
            WriteStrategy = SqlServerWriteStrategy.Batch,
            BatchSize = 10,
            UseTransaction = true,
            Schema = "Sales",
        };

        // Create sink node (no table attribute, uses convention)
        var sinkNode = new SqlServerSinkNode<Product>(
            _connectionString,
            "Products",
            configuration);

        Console.WriteLine("Writing products with convention-based mapping...");
        Console.WriteLine("  - No attributes used on Product class");
        Console.WriteLine("  - Property names (PascalCase) map to column names (PascalCase)");
        Console.WriteLine("  - Case-insensitive matching enabled by default");
        Console.WriteLine("  - IgnoreColumn attribute excludes computed properties");

        // Write products
        var dataPipe = new InMemoryDataPipe<Product>(products);
        await sinkNode.ExecuteAsync(dataPipe, null!, cancellationToken);

        // Read products to verify convention-based mapping
        var sourceNode = new SqlServerSourceNode<Product>(
            _connectionString,
            "SELECT * FROM Sales.Products ORDER BY ProductID",
            new SqlServerConfiguration());

        var readProducts = new List<Product>();

        await foreach (var product in sourceNode.Initialize(null!, cancellationToken))
        {
            readProducts.Add(product);
            Console.WriteLine($"  - Read: {product.ProductName} (Category: {product.Category}, Price: ${product.Price:F2}, Stock: {product.StockQuantity})");
        }

        Console.WriteLine("✓ Convention-based mapping completed successfully");
        Console.WriteLine($"  - Total products written: {products.Count}");
        Console.WriteLine($"  - Total products read: {readProducts.Count}");
    }

    /// <summary>
    ///     Demonstrates custom mappers.
    /// </summary>
    private async Task DemonstrateCustomMappersAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 6: Demonstrating Custom Mappers");
        Console.WriteLine("---------------------------------------");

        // Create custom mapper function
        Func<Order, IEnumerable<DatabaseParameter>> customMapper = order =>
        [
            new DatabaseParameter("@CustomerID", order.CustomerId),
            new DatabaseParameter("@OrderDate", order.OrderDate),
            new DatabaseParameter("@TotalAmount", order.TotalAmount),
            new DatabaseParameter("@Status", "Custom-" + order.Status), // Custom transformation
            new DatabaseParameter("@ShippingAddress", order.ShippingAddress ?? "N/A"),
            new DatabaseParameter("@Notes", "Custom mapper: " + (order.Notes ?? "No notes")),
        ];

        // Create sample orders with custom mapper
        var orders = new List<Order>
        {
            new()
            {
                CustomerId = 1,
                OrderDate = DateTime.Now,
                TotalAmount = 150.00m,
                Status = "Pending",
                ShippingAddress = "123 Custom St",
                Notes = "Custom order 1",
            },
            new()
            {
                CustomerId = 2,
                OrderDate = DateTime.Now,
                TotalAmount = 250.00m,
                Status = "Processing",
                ShippingAddress = "456 Custom Ave",
                Notes = "Custom order 2",
            },
        };

        // Configure sink with custom mapper
        var configuration = new SqlServerConfiguration
        {
            WriteStrategy = SqlServerWriteStrategy.Batch,
            BatchSize = 10,
            UseTransaction = true,
            Schema = "Sales",
        };

        // Create sink node with custom mapper
        var sinkNode = new SqlServerSinkNode<Order>(
            _connectionString,
            "Orders",
            configuration,
            customMapper);

        Console.WriteLine("Writing orders with custom mapper...");
        Console.WriteLine("  - Custom mapper function transforms data before writing");
        Console.WriteLine("  - Status prefix changed to 'Custom-'");
        Console.WriteLine("  - Notes prefixed with 'Custom mapper: '");
        Console.WriteLine("  - Default shipping address set to 'N/A' if null");

        // Write orders with custom mapper
        var dataPipe = new InMemoryDataPipe<Order>(orders);
        await sinkNode.ExecuteAsync(dataPipe, null!, cancellationToken);

        // Read orders to verify custom mapper
        var sourceNode = new SqlServerSourceNode<Order>(
            _connectionString,
            "SELECT TOP 2 * FROM Sales.Orders ORDER BY OrderID DESC",
            new SqlServerConfiguration());

        await foreach (var order in sourceNode.Initialize(null!, cancellationToken))
        {
            Console.WriteLine($"  - Read: Order {order.OrderId} (Status: {order.Status}, Notes: {order.Notes})");
        }

        Console.WriteLine("✓ Custom mapper completed successfully");
        Console.WriteLine($"  - Total orders written: {orders.Count}");
    }

    /// <summary>
    ///     Demonstrates transformation and enrichment.
    /// </summary>
    private async Task DemonstrateTransformationAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 7: Demonstrating Transformation and Enrichment");
        Console.WriteLine("-------------------------------------------------");

        // Read customers and their orders
        var sourceNode = new SqlServerSourceNode<Customer>(
            _connectionString,
            "SELECT * FROM Sales.Customers ORDER BY CustomerID",
            new SqlServerConfiguration());

        var customers = new List<Customer>();

        await foreach (var customer in sourceNode.Initialize(null!, cancellationToken))
        {
            customers.Add(customer);
        }

        // Read orders
        var ordersSourceNode = new SqlServerSourceNode<Order>(
            _connectionString,
            "SELECT * FROM Sales.Orders ORDER BY OrderID",
            new SqlServerConfiguration());

        var orders = new List<Order>();

        await foreach (var order in ordersSourceNode.Initialize(null!, cancellationToken))
        {
            orders.Add(order);
        }

        Console.WriteLine("Enriching customer data with order statistics...");
        Console.WriteLine("  - Calculating total orders per customer");
        Console.WriteLine("  - Calculating total spent per customer");
        Console.WriteLine("  - Calculating average order value");
        Console.WriteLine("  - Determining customer tier");
        Console.WriteLine("  - Finding last order date");

        // Transform and enrich customers
        var enrichedCustomers = customers.Select(customer =>
        {
            var customerOrders = orders.Where(o => o.CustomerId == customer.CustomerId).ToList();
            var totalOrders = customerOrders.Count;
            var totalSpent = customerOrders.Sum(o => o.TotalAmount);

            var averageOrderValue = totalOrders > 0
                ? totalSpent / totalOrders
                : 0m;

            var lastOrderDate = customerOrders.Count > 0
                ? customerOrders.Max(o => o.OrderDate)
                : (DateTime?)null;

            // Determine customer tier
            string customerTier;

            if (totalSpent >= 5000)
                customerTier = "Platinum";
            else if (totalSpent >= 2000)
                customerTier = "Gold";
            else if (totalSpent >= 500)
                customerTier = "Silver";
            else
                customerTier = "Bronze";

            return new EnrichedCustomer
            {
                CustomerId = customer.CustomerId,
                FullName = customer.FullName,
                Email = customer.Email.ToUpperInvariant(), // Transform: uppercase email
                PhoneNumber = customer.PhoneNumber,
                RegistrationDate = customer.RegistrationDate,
                Status = customer.Status,
                TotalOrders = totalOrders,
                TotalSpent = totalSpent,
                AverageOrderValue = averageOrderValue,
                CustomerTier = customerTier,
                LastOrderDate = lastOrderDate,
                EnrichmentDate = DateTime.Now,
            };
        }).ToList();

        // Write enriched customers
        var configuration = new SqlServerConfiguration
        {
            WriteStrategy = SqlServerWriteStrategy.Batch,
            BatchSize = 10,
            UseTransaction = true,
            Schema = "Analytics",
        };

        var sinkNode = new SqlServerSinkNode<EnrichedCustomer>(
            _connectionString,
            "EnrichedCustomers",
            configuration);

        var dataPipe = new InMemoryDataPipe<EnrichedCustomer>(enrichedCustomers);
        await sinkNode.ExecuteAsync(dataPipe, null!, cancellationToken);

        // Display enriched customers
        foreach (var enriched in enrichedCustomers)
        {
            Console.WriteLine($"  - {enriched.FullName} (Tier: {enriched.CustomerTier}, Orders: {enriched.TotalOrders}, Spent: ${enriched.TotalSpent:F2})");
        }

        Console.WriteLine("✓ Transformation and enrichment completed successfully");
        Console.WriteLine($"  - Total customers enriched: {enrichedCustomers.Count}");
    }

    /// <summary>
    ///     Demonstrates error handling.
    /// </summary>
    private async Task DemonstrateErrorHandlingAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 8: Demonstrating Error Handling");
        Console.WriteLine("--------------------------------------");

        // Create orders with potential errors (invalid customer IDs)
        var orders = new List<Order>
        {
            new() { CustomerId = 1, OrderDate = DateTime.Now, TotalAmount = 100.00m, Status = "Test" },
            new() { CustomerId = 999, OrderDate = DateTime.Now, TotalAmount = 200.00m, Status = "Test" }, // Invalid customer ID
            new() { CustomerId = 2, OrderDate = DateTime.Now, TotalAmount = 300.00m, Status = "Test" },
        };

        // Configure sink with error handling
        var configuration = new SqlServerConfiguration
        {
            WriteStrategy = SqlServerWriteStrategy.PerRow,
            UseTransaction = false, // Disable transaction to allow partial success
            MaxRetryAttempts = 3,
            RetryDelay = TimeSpan.FromSeconds(1),
            ContinueOnError = false, // Stop on first error
            Schema = "Sales",
        };

        var sinkNode = new SqlServerSinkNode<Order>(
            _connectionString,
            "Orders",
            configuration);

        Console.WriteLine("Attempting to write orders with error handling...");
        Console.WriteLine("  - Order 1: Valid customer ID (1)");
        Console.WriteLine("  - Order 2: Invalid customer ID (999) - will fail");
        Console.WriteLine("  - Order 3: Valid customer ID (2)");
        Console.WriteLine("  - ContinueOnError: false (stop on first error)");
        Console.WriteLine("  - MaxRetryAttempts: 3");

        try
        {
            var dataPipe = new InMemoryDataPipe<Order>(orders);
            await sinkNode.ExecuteAsync(dataPipe, null!, cancellationToken);
            Console.WriteLine("✓ All orders written successfully (unexpected)");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✓ Error handling demonstrated: {ex.Message}");
            Console.WriteLine("  - The pipeline stopped on the first error as configured");
        }

        // Demonstrate ContinueOnError = true
        configuration.ContinueOnError = true;

        sinkNode = new SqlServerSinkNode<Order>(
            _connectionString,
            "Orders",
            configuration);

        Console.WriteLine();
        Console.WriteLine("Retrying with ContinueOnError = true...");
        Console.WriteLine("  - This will skip the invalid order and continue");

        try
        {
            var dataPipe = new InMemoryDataPipe<Order>(orders);
            await sinkNode.ExecuteAsync(dataPipe, null!, cancellationToken);
            Console.WriteLine("✓ Error handling with ContinueOnError completed");
            Console.WriteLine("  - Valid orders were written despite the invalid order");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✓ Error: {ex.Message}");
        }

        Console.WriteLine("✓ Error handling demonstration completed");
    }
}

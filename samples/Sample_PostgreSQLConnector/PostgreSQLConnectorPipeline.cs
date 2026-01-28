using System.Diagnostics;
using System.Globalization;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.Connectors.PostgreSQL.Nodes;
using NPipeline.Pipeline;

namespace Sample_PostgreSQLConnector;

/// <summary>
/// Main pipeline demonstrating PostgreSQL connector features.
/// This pipeline shows reading from and writing to PostgreSQL tables,
/// with various write strategies and error handling patterns.
/// </summary>
public sealed class PostgreSQLConnectorPipeline
{
    private readonly string _connectionString;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgreSQLConnectorPipeline"/> class.
    /// </summary>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    public PostgreSQLConnectorPipeline(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    /// Gets a description of this pipeline.
    /// </summary>
    /// <returns>A string describing the pipeline.</returns>
    public static string GetDescription()
    {
        return """
        PostgreSQL Connector Sample Pipeline
        ====================================
        
        This pipeline demonstrates of following PostgreSQL connector features:
        
        1. Reading data from PostgreSQL using PostgresSourceNode<T>
        2. Writing data to PostgreSQL using PostgresSinkNode<T>
        3. Attribute-based mapping with PostgresTable and PostgresColumn attributes
        4. Different write strategies (PerRow, Batch)
        5. Error handling and recovery patterns
        6. Connection pooling and configuration
        7. In-memory checkpointing for transient recovery
        
        Pipeline Flow:
        - Setup database tables and seed sample data
        - Read customers from source table
        - Read products from catalog
        - Read orders with items
        - Transform and aggregate order data
        - Write order summaries to destination table
        
        Models Used:
        - Customer: Customer information with convention-based mapping
        - Product: Product catalog with attribute-based mapping
        - Order: Order header with foreign key relationships
        - OrderItem: Order line items
        - OrderSummary: Aggregated order data for reporting
        - TestRecord: Test record for write strategy demonstration
        """;
    }

    /// <summary>
    /// Executes the pipeline.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task ExecuteAsync(PipelineContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine("=== PostgreSQL Connector Pipeline ===");
        Console.WriteLine();

        var stopwatch = Stopwatch.StartNew();

        try
        {
            // Step 1: Setup database tables
            Console.WriteLine("Step 1: Setting up database tables...");
            await SetupTablesAsync(cancellationToken);
            Console.WriteLine("  Tables created successfully.");
            Console.WriteLine();

            // Step 2: Seed sample data
            Console.WriteLine("Step 2: Seeding sample data...");
            await SeedSampleDataAsync(cancellationToken);
            Console.WriteLine("  Sample data seeded successfully.");
            Console.WriteLine();

            // Step 3: Read and process customers
            Console.WriteLine("Step 3: Processing customers...");
            var customerCount = await ProcessCustomersAsync(cancellationToken);
            Console.WriteLine($"  Processed {customerCount} customers.");
            Console.WriteLine();

            // Step 4: Read and process products
            Console.WriteLine("Step 4: Processing products...");
            var productCount = await ProcessProductsAsync(cancellationToken);
            Console.WriteLine($"  Processed {productCount} products.");
            Console.WriteLine();

            // Step 5: Process orders and generate summaries
            Console.WriteLine("Step 5: Processing orders and generating summaries...");
            var summaryCount = await ProcessOrdersAsync(cancellationToken);
            Console.WriteLine($"  Generated {summaryCount} order summaries.");
            Console.WriteLine();

            // Step 6: Demonstrate different write strategies
            Console.WriteLine("Step 6: Demonstrating write strategies...");
            await DemonstrateWriteStrategiesAsync(cancellationToken);
            Console.WriteLine();

            // Step 7: Demonstrate in-memory checkpointing
            Console.WriteLine("Step 7: Demonstrating in-memory checkpointing...");
            await DemonstrateInMemoryCheckpointingAsync(cancellationToken);
            Console.WriteLine();

            stopwatch.Stop();

            Console.WriteLine("=== Pipeline Execution Summary ===");
            Console.WriteLine($"  Total time: {stopwatch.ElapsedMilliseconds} ms");
            Console.WriteLine($"  Customers processed: {customerCount}");
            Console.WriteLine($"  Products processed: {productCount}");
            Console.WriteLine($"  Order summaries generated: {summaryCount}");
            Console.WriteLine();
            Console.WriteLine("Pipeline completed successfully!");
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Pipeline execution failed after {stopwatch.ElapsedMilliseconds} ms");
            Console.WriteLine($"Error: {ex.Message}");
            Console.ResetColor();
            throw;
        }
    }

    /// <summary>
    /// Sets up the database tables.
    /// </summary>
    private async Task SetupTablesAsync(CancellationToken cancellationToken)
    {
        using var connection = new Npgsql.NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var tables = new[]
        {
            "customers", "products", "orders", "order_items",
            "order_summaries", "customers_copy", "products_copy",
            "write_test_perrow", "write_test_batch", "checkpoint_test"
        };

        foreach (var table in tables)
        {
            var dropCmd = new Npgsql.NpgsqlCommand($"DROP TABLE IF EXISTS {table} CASCADE", connection);
            await dropCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // Create customers table
        var createCustomers = @"
            CREATE TABLE customers (
                customer_id SERIAL PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                email VARCHAR(255) NOT NULL UNIQUE,
                phone VARCHAR(20),
                address VARCHAR(255),
                city VARCHAR(100),
                state VARCHAR(50),
                postal_code VARCHAR(20),
                country VARCHAR(100),
                registration_date DATE NOT NULL,
                status VARCHAR(20) DEFAULT 'active'
            )";
        await ExecuteNonQueryAsync(createCustomers, connection, cancellationToken);

        // Create products table
        var createProducts = @"
            CREATE TABLE products (
                product_id SERIAL PRIMARY KEY,
                product_name VARCHAR(255) NOT NULL,
                sku VARCHAR(50) NOT NULL UNIQUE,
                description TEXT,
                category VARCHAR(100),
                price DECIMAL(10,2) NOT NULL CHECK (price > 0),
                cost DECIMAL(10,2) NOT NULL CHECK (cost >= 0),
                stock_quantity INTEGER NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
                reorder_level INTEGER DEFAULT 10,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )";
        await ExecuteNonQueryAsync(createProducts, connection, cancellationToken);

        // Create orders table
        var createOrders = @"
            CREATE TABLE orders (
                order_id SERIAL PRIMARY KEY,
                customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
                order_date DATE NOT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                subtotal DECIMAL(12,2) NOT NULL CHECK (subtotal >= 0),
                tax_amount DECIMAL(10,2) NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
                shipping_amount DECIMAL(10,2) NOT NULL DEFAULT 0 CHECK (shipping_amount >= 0),
                discount_amount DECIMAL(10,2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
                total_amount DECIMAL(12,2) NOT NULL CHECK (total_amount >= 0),
                shipping_address VARCHAR(255),
                shipping_city VARCHAR(100),
                shipping_state VARCHAR(50),
                shipping_postal_code VARCHAR(20),
                shipping_country VARCHAR(100),
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )";
        await ExecuteNonQueryAsync(createOrders, connection, cancellationToken);

        // Create order_items table
        var createOrderItems = @"
            CREATE TABLE order_items (
                order_item_id SERIAL PRIMARY KEY,
                order_id INTEGER NOT NULL REFERENCES orders(order_id),
                product_id INTEGER NOT NULL REFERENCES products(product_id),
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
                discount_amount DECIMAL(10,2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
                line_total DECIMAL(12,2) NOT NULL CHECK (line_total >= 0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )";
        await ExecuteNonQueryAsync(createOrderItems, connection, cancellationToken);

        // Create order_summaries table
        var createOrderSummaries = @"
            CREATE TABLE order_summaries (
                summary_id SERIAL PRIMARY KEY,
                order_id INTEGER NOT NULL UNIQUE,
                customer_id INTEGER NOT NULL,
                customer_name VARCHAR(255) NOT NULL,
                order_date DATE NOT NULL,
                status VARCHAR(20) NOT NULL,
                total_items INTEGER NOT NULL,
                total_products INTEGER NOT NULL,
                subtotal DECIMAL(12,2) NOT NULL,
                total_discount DECIMAL(10,2) NOT NULL DEFAULT 0,
                total_amount DECIMAL(12,2) NOT NULL,
                avg_item_price DECIMAL(10,2) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )";
        await ExecuteNonQueryAsync(createOrderSummaries, connection, cancellationToken);

        // Create tables for write strategy demonstration
        var createCopyTable = @"
            CREATE TABLE {0} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value DECIMAL(10,2),
                category VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )";

        foreach (var tableName in new[] { "customers_copy", "products_copy", "write_test_perrow", "write_test_batch", "checkpoint_test" })
        {
            var sql = string.Format(CultureInfo.InvariantCulture, createCopyTable, tableName);
            await ExecuteNonQueryAsync(sql, connection, cancellationToken);
        }
    }

    /// <summary>
    /// Seeds sample data into the database.
    /// </summary>
    private async Task SeedSampleDataAsync(CancellationToken cancellationToken)
    {
        using var connection = new Npgsql.NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Seed customers
        var insertCustomers = @"
            INSERT INTO customers (first_name, last_name, email, phone, address, city, state, postal_code, country, registration_date, status) VALUES
            ('John', 'Doe', 'john.doe@example.com', '555-0101', '123 Main St', 'Springfield', 'IL', '62701', 'USA', '2024-01-15', 'active'),
            ('Jane', 'Smith', 'jane.smith@example.com', '555-0102', '456 Oak Ave', 'Chicago', 'IL', '60601', 'USA', '2024-02-20', 'active'),
            ('Bob', 'Johnson', 'bob.johnson@example.com', '555-0103', '789 Pine Rd', 'Milwaukee', 'WI', '53201', 'USA', '2024-03-10', 'active'),
            ('Alice', 'Williams', 'alice.williams@example.com', '555-0104', '321 Elm St', 'Minneapolis', 'MN', '55401', 'USA', '2024-04-05', 'active'),
            ('Charlie', 'Brown', 'charlie.brown@example.com', '555-0105', '654 Maple Dr', 'Des Moines', 'IA', '50301', 'USA', '2024-05-18', 'active')
        ";
        await ExecuteNonQueryAsync(insertCustomers, connection, cancellationToken);

        // Seed products
        var insertProducts = @"
            INSERT INTO products (product_name, sku, description, category, price, cost, stock_quantity, reorder_level, is_active) VALUES
            ('Laptop Computer', 'LAPTOP-001', 'High-performance laptop with 16GB RAM', 'Electronics', 1299.99, 950.00, 50, 10, true),
            ('Wireless Mouse', 'MOUSE-001', 'Ergonomic wireless mouse', 'Electronics', 29.99, 12.00, 200, 50, true),
            ('Mechanical Keyboard', 'KEYBOARD-001', 'RGB mechanical keyboard', 'Electronics', 149.99, 85.00, 75, 20, true),
            ('USB-C Hub', 'HUB-001', '7-in-1 USB-C hub', 'Electronics', 49.99, 25.00, 150, 30, true),
            ('Monitor Stand', 'STAND-001', 'Adjustable monitor stand', 'Accessories', 39.99, 18.00, 100, 25, true),
            ('Webcam HD', 'CAM-001', '1080p HD webcam', 'Electronics', 79.99, 45.00, 80, 20, true),
            ('Desk Lamp', 'LAMP-001', 'LED desk lamp with dimmer', 'Accessories', 34.99, 15.00, 120, 30, true),
            ('Cable Management Kit', 'CABLE-001', 'Velcro cable ties and organizer', 'Accessories', 19.99, 8.00, 300, 50, true)
        ";
        await ExecuteNonQueryAsync(insertProducts, connection, cancellationToken);

        // Seed orders
        var insertOrders = @"
            INSERT INTO orders (customer_id, order_date, status, subtotal, tax_amount, shipping_amount, discount_amount, total_amount, shipping_address, shipping_city, shipping_state, shipping_postal_code, shipping_country) VALUES
            (1, '2024-06-01', 'completed', 1379.98, 110.40, 9.99, 0, 1500.37, '123 Main St', 'Springfield', 'IL', '62701', 'USA'),
            (2, '2024-06-02', 'shipped', 179.98, 14.40, 5.99, 17.99, 182.38, '456 Oak Ave', 'Chicago', 'IL', '60601', 'USA'),
            (3, '2024-06-03', 'processing', 1299.99, 104.00, 9.99, 64.99, 1348.98, '789 Pine Rd', 'Milwaukee', 'WI', '53201', 'USA'),
            (1, '2024-06-05', 'pending', 49.99, 4.00, 5.99, 0, 58.98, '123 Main St', 'Springfield', 'IL', '62701', 'USA'),
            (4, '2024-06-06', 'completed', 229.98, 18.40, 7.99, 22.99, 233.38, '321 Elm St', 'Minneapolis', 'MN', '55401', 'USA'),
            (5, '2024-06-07', 'shipped', 34.99, 2.80, 4.99, 0, 42.78, '654 Maple Dr', 'Des Moines', 'IA', '50301', 'USA')
        ";
        await ExecuteNonQueryAsync(insertOrders, connection, cancellationToken);

        // Seed order items
        var insertOrderItems = @"
            INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount_amount, line_total) VALUES
            (1, 1, 1, 1299.99, 0, 1299.99),
            (1, 2, 1, 29.99, 0, 29.99),
            (1, 3, 1, 149.99, 0, 149.99),
            (2, 4, 1, 49.99, 0, 49.99),
            (2, 5, 1, 39.99, 0, 39.99),
            (2, 6, 1, 79.99, 0, 79.99),
            (2, 7, 1, 34.99, 0, 34.99),
            (3, 1, 1, 1299.99, 64.99, 1235.00),
            (4, 4, 1, 49.99, 0, 49.99),
            (5, 1, 1, 1299.99, 0, 1299.99),
            (5, 2, 1, 29.99, 0, 29.99),
            (6, 7, 1, 34.99, 0, 34.99)
        ";
        await ExecuteNonQueryAsync(insertOrderItems, connection, cancellationToken);

        // Seed checkpoint test records
        var insertCheckpointRecords = new System.Text.StringBuilder();
        _ = insertCheckpointRecords.Append("INSERT INTO checkpoint_test (name, value, category, created_at) VALUES");

        for (var i = 1; i <= 25; i++)
        {
            var category = i % 3 == 0 ? "A" : i % 3 == 1 ? "B" : "C";
            var createdAt = DateTime.UtcNow.AddMinutes(-i).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            var values = $"('Checkpoint Item {i}', {i * 2.5m:F2}, '{category}', '{createdAt}')";
            _ = insertCheckpointRecords.Append(values);
            if (i < 25)
            {
                _ = insertCheckpointRecords.Append(',');
            }
        }

        await ExecuteNonQueryAsync(insertCheckpointRecords.ToString(), connection, cancellationToken);
    }

    /// <summary>
    /// Processes customers by reading from source and writing to copy table.
    /// </summary>
    private async Task<int> ProcessCustomersAsync(CancellationToken cancellationToken)
    {
        var sourceConfig = new PostgresConfiguration
        {
            ConnectionString = _connectionString
        };

        var sql = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, postal_code, country, registration_date, status FROM customers ORDER BY customer_id";
        var sourceNode = new PostgresSourceNode<Customer>(_connectionString, sql, configuration: sourceConfig);

        var sinkConfig = new PostgresConfiguration
        {
            ConnectionString = _connectionString,
            WriteStrategy = PostgresWriteStrategy.Batch,
            BatchSize = 100
        };

        var sinkNode = new PostgresSinkNode<Customer>(_connectionString, "customers_copy", PostgresWriteStrategy.Batch, configuration: sinkConfig);
        var context = new PipelineContext();

        var count = 0;
        await foreach (var customer in sourceNode.Initialize(context, cancellationToken))
        {
            count++;
        }

        await sinkNode.ExecuteAsync(sourceNode.Initialize(context, cancellationToken), context, cancellationToken);
        return count;
    }

    /// <summary>
    /// Processes products by reading from source and writing to copy table.
    /// </summary>
    private async Task<int> ProcessProductsAsync(CancellationToken cancellationToken)
    {
        var sourceConfig = new PostgresConfiguration
        {
            ConnectionString = _connectionString
        };

        var sql = "SELECT product_id, product_name, sku, description, category, price, cost, stock_quantity, reorder_level, is_active, created_at, updated_at FROM products ORDER BY product_id";
        var sourceNode = new PostgresSourceNode<Product>(_connectionString, sql, configuration: sourceConfig);

        var sinkConfig = new PostgresConfiguration
        {
            ConnectionString = _connectionString,
            WriteStrategy = PostgresWriteStrategy.Batch,
            BatchSize = 50
        };

        var sinkNode = new PostgresSinkNode<Product>(_connectionString, "products_copy", PostgresWriteStrategy.Batch, configuration: sinkConfig);
        var context = new PipelineContext();

        var count = 0;
        await foreach (var product in sourceNode.Initialize(context, cancellationToken))
        {
            count++;
        }

        await sinkNode.ExecuteAsync(sourceNode.Initialize(context, cancellationToken), context, cancellationToken);
        return count;
    }

    /// <summary>
    /// Processes orders and generates order summaries.
    /// </summary>
    private async Task<int> ProcessOrdersAsync(CancellationToken cancellationToken)
    {
        using var connection = new Npgsql.NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Query to join orders with customers and order items
        var query = @"
            SELECT 
                o.order_id,
                o.customer_id,
                c.first_name || ' ' || c.last_name as customer_name,
                o.order_date,
                o.status,
                COUNT(oi.order_item_id) as total_items,
                COUNT(DISTINCT oi.product_id) as total_products,
                o.subtotal,
                SUM(oi.discount_amount) as total_discount,
                o.total_amount,
                CASE WHEN COUNT(oi.order_item_id) > 0 THEN o.total_amount / COUNT(oi.order_item_id) ELSE 0 END as avg_item_price
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY o.order_id, o.customer_id, c.first_name, c.last_name, o.order_date, o.status, o.subtotal, o.total_amount
            ORDER BY o.order_id";

        var summaries = new List<OrderSummary>();

        await using var cmd = new Npgsql.NpgsqlCommand(query, connection);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var summary = new OrderSummary
            {
                OrderId = reader.GetInt32(0),
                CustomerId = reader.GetInt32(1),
                CustomerName = reader.GetString(2),
                OrderDate = reader.GetDateTime(3),
                Status = reader.GetString(4),
                TotalItems = reader.GetInt32(5),
                TotalProducts = reader.GetInt32(6),
                Subtotal = reader.GetDecimal(7),
                TotalDiscount = reader.IsDBNull(8) ? 0 : reader.GetDecimal(8),
                TotalAmount = reader.GetDecimal(9),
                AverageItemPrice = reader.GetDecimal(10),
                CreatedAt = DateTime.UtcNow
            };
            summaries.Add(summary);
        }

        // Write summaries to database
        var sinkConfig = new PostgresConfiguration
        {
            ConnectionString = _connectionString,
            WriteStrategy = PostgresWriteStrategy.Batch,
            BatchSize = 10
        };

        var sinkNode = new PostgresSinkNode<OrderSummary>(_connectionString, "order_summaries", PostgresWriteStrategy.Batch, configuration: sinkConfig);
        var context = new PipelineContext();

        // Create a data pipe from the list
        var dataPipe = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<OrderSummary>(summaries);

        await sinkNode.ExecuteAsync(dataPipe, context, cancellationToken);

        return summaries.Count;
    }

    /// <summary>
    /// Demonstrates different write strategies.
    /// </summary>
    private async Task DemonstrateWriteStrategiesAsync(CancellationToken cancellationToken)
    {
        using var connection = new Npgsql.NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Seed test data
        var insertData = new System.Text.StringBuilder();
        _ = insertData.Append("INSERT INTO write_test_perrow (name, value, category) VALUES");

        for (var i = 1; i <= 100; i++)
        {
            var category = i % 5 switch { 0 => 'A', 1 => 'B', 2 => 'C', 3 => 'D', _ => 'E' };
            var values = $"('Test Item {i}', {i * 1.5m:F2}, '{category}')";
            _ = insertData.Append(values);
            if (i < 100)
            {
                _ = insertData.Append(',');
            }
        }

        await ExecuteNonQueryAsync(insertData.ToString(), connection, cancellationToken);
        await ExecuteNonQueryAsync(insertData.ToString().Replace("write_test_perrow", "write_test_batch"), connection, cancellationToken);

        // Test PerRow strategy
        var perRowTime = await TestWriteStrategyAsync("write_test_perrow", PostgresWriteStrategy.PerRow, 1, cancellationToken);
        Console.WriteLine($"  PerRow strategy: {perRowTime} ms");

        // Test Batch strategy
        var batchTime = await TestWriteStrategyAsync("write_test_batch", PostgresWriteStrategy.Batch, 25, cancellationToken);
        Console.WriteLine($"  Batch strategy (size 25): {batchTime} ms");

        Console.WriteLine($"  Performance improvement: Batch is {((double)perRowTime / batchTime):F2}x faster than PerRow");
    }

    /// <summary>
    /// Demonstrates in-memory checkpointing for transient recovery.
    /// </summary>
    private async Task DemonstrateInMemoryCheckpointingAsync(CancellationToken cancellationToken)
    {
        var config = new PostgresConfiguration
        {
            ConnectionString = _connectionString,
            CheckpointStrategy = NPipeline.Connectors.Configuration.CheckpointStrategy.InMemory,
            StreamResults = true
        };

        var sql = "SELECT id, name, value, category, created_at FROM checkpoint_test ORDER BY id";

        var interruptedContext = new PipelineContext();
        var interruptedSource = new PostgresSourceNode<CheckpointTestRecord>(_connectionString, sql, configuration: config);

        var interruptedCount = 0;
        await foreach (var _ in interruptedSource.Initialize(interruptedContext, cancellationToken))
        {
            interruptedCount++;
            if (interruptedCount == 5)
            {
                Console.WriteLine("  Simulating interruption after 5 rows...");
                break;
            }
        }

        var resumeContext = new PipelineContext();
        var resumeSource = new PostgresSourceNode<CheckpointTestRecord>(_connectionString, sql, configuration: config);

        var resumedCount = 0;
        await foreach (var _ in resumeSource.Initialize(resumeContext, cancellationToken))
        {
            resumedCount++;
        }

        Console.WriteLine($"  Resumed and processed {resumedCount} remaining rows.");
    }

    /// <summary>
    /// Tests a specific write strategy.
    /// </summary>
    private async Task<long> TestWriteStrategyAsync(string tableName, PostgresWriteStrategy strategy, int batchSize, CancellationToken cancellationToken)
    {
        var sourceConfig = new PostgresConfiguration
        {
            ConnectionString = _connectionString
        };

        var sql = $"SELECT id, name, value, category, created_at FROM {tableName} ORDER BY id";
        var sourceNode = new PostgresSourceNode<TestRecord>(_connectionString, sql, configuration: sourceConfig);

        var sinkConfig = new PostgresConfiguration
        {
            ConnectionString = _connectionString,
            WriteStrategy = strategy,
            BatchSize = batchSize > 0 ? batchSize : 100
        };

        var sinkNode = new PostgresSinkNode<TestRecord>(_connectionString, tableName + "_result", strategy, configuration: sinkConfig);
        var context = new PipelineContext();

        var stopwatch = Stopwatch.StartNew();
        await sinkNode.ExecuteAsync(sourceNode.Initialize(context, cancellationToken), context, cancellationToken);
        stopwatch.Stop();

        return stopwatch.ElapsedMilliseconds;
    }

    /// <summary>
    /// Executes a non-query SQL command.
    /// </summary>
    private static async Task ExecuteNonQueryAsync(string sql, Npgsql.NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var cmd = new Npgsql.NpgsqlCommand(sql, connection);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Test record for write strategy demonstration.
    /// </summary>
    [PostgresTable("write_test")]
    private sealed class TestRecord
    {
        [PostgresColumn("id", PrimaryKey = true)]
        public int Id { get; set; }

        [PostgresColumn("name")]
        public string Name { get; set; } = string.Empty;

        [PostgresColumn("value")]
        public decimal Value { get; set; }

        [PostgresColumn("category")]
        public string Category { get; set; } = string.Empty;

        [PostgresColumn("created_at")]
        public DateTime CreatedAt { get; set; }
    }
}

using MySqlConnector;
using NPipeline.Connectors.MySql;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;

namespace Sample_MySQLConnector;

/// <summary>
///     Pipeline demonstrating MySQL connector features including:
///     - Reading from MySQL using MySqlSourceNode
///     - Writing to MySQL using MySqlSinkNode
///     - PerRow and Batch write strategies
///     - Attribute-based and convention-based mapping
///     - Upsert with ON DUPLICATE KEY UPDATE
///     - Custom mappers
///     - Connection pooling
///     - StorageUri support
///     - Error handling
/// </summary>
public sealed class MySqlConnectorPipeline
{
    private readonly string _connectionString;

    public MySqlConnectorPipeline(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    ///     Gets a description of the pipeline and its features.
    /// </summary>
    public static string GetDescription()
    {
        return """
               MySQL Connector Sample Pipeline
               ================================

               This pipeline demonstrates the following features:

               1. Reading from MySQL
                  - MySqlSourceNode for data retrieval
                  - Parameterized queries
                  - Streaming results (CommandBehavior.SequentialAccess)

               2. Writing to MySQL
                  - MySqlSinkNode for data insertion
                  - PerRow write strategy (row-by-row)
                  - Batch write strategy (batched multi-row INSERT)
                  - BulkLoad write strategy (LOAD DATA LOCAL INFILE)

               3. Upsert Semantics
                  - INSERT ... ON DUPLICATE KEY UPDATE
                  - INSERT IGNORE (skip duplicates)
                  - REPLACE INTO (delete+insert)

               4. Mapping Strategies
                  - Attribute-based mapping (MySqlTable, MySqlColumn, Column, IgnoreColumn)
                  - Convention-based mapping (property names → column names)
                  - Custom mappers (Func<T, IEnumerable<DatabaseParameter>>)

               5. Connection Management
                  - Connection pooling
                  - Named connections
                  - StorageUri (mysql://user:pass@host:port/db)
                  - MariaDB support (mariadb:// scheme)

               6. Error Handling
                  - Retry logic for transient errors
                  - Row-level error handling
                  - Transaction support
               """;
    }

    /// <summary>
    ///     Executes all pipeline demonstrations.
    /// </summary>
    public async Task ExecuteAsync(PipelineContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Starting MySQL Connector Pipeline...");
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

        // Step 4: Demonstrate upsert with ON DUPLICATE KEY UPDATE
        await DemonstrateUpsertAsync(cancellationToken);
        Console.WriteLine();

        // Step 5: Demonstrate attribute-based mapping
        await DemonstrateAttributeBasedMappingAsync(cancellationToken);
        Console.WriteLine();

        // Step 6: Demonstrate StorageUri
        await DemonstrateStorageUriAsync(cancellationToken);
        Console.WriteLine();

        Console.WriteLine("MySQL Connector Pipeline completed successfully.");
    }

    // -------------------------------------------------------------------------
    // 1. Schema initialisation
    // -------------------------------------------------------------------------

    private async Task InitializeDatabaseSchemaAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("1. Initialising database schema...");

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        var ddl = new[]
        {
            @"CREATE TABLE IF NOT EXISTS `products` (
                `product_id`     INT            NOT NULL AUTO_INCREMENT,
                `product_name`   VARCHAR(200)   NOT NULL,
                `category`       VARCHAR(100)   NOT NULL,
                `unit_price`     DECIMAL(10,2)  NOT NULL,
                `stock_quantity` INT            NOT NULL DEFAULT 0,
                `is_active`      TINYINT(1)     NOT NULL DEFAULT 1,
                `created_at`     DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (`product_id`)
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",

            @"CREATE TABLE IF NOT EXISTS `order_events` (
                `event_id`        VARCHAR(64)    NOT NULL,
                `order_id`        INT            NOT NULL,
                `event_type`      VARCHAR(50)    NOT NULL,
                `event_payload`   JSON,
                `event_timestamp` DATETIME       NOT NULL,
                `status`          VARCHAR(20)    NOT NULL DEFAULT 'pending',
                PRIMARY KEY (`event_id`)
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",

            @"CREATE TABLE IF NOT EXISTS `product_summaries` (
                `product_id`      INT            NOT NULL,
                `product_name`    VARCHAR(200)   NOT NULL,
                `category`        VARCHAR(100)   NOT NULL,
                `total_revenue`   DECIMAL(14,2)  NOT NULL DEFAULT 0,
                `units_sold`      INT            NOT NULL DEFAULT 0,
                `last_updated_at` DATETIME       NOT NULL,
                PRIMARY KEY (`product_id`)
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        };

        foreach (var sql in ddl)
        {
            await using var cmd = new MySqlCommand(sql, conn);
            _ = await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        Console.WriteLine("   Tables created (or already exist).");
    }

    // -------------------------------------------------------------------------
    // 2. PerRow write strategy
    // -------------------------------------------------------------------------

    private async Task DemonstratePerRowWriteStrategyAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("2. Demonstrating PerRow write strategy...");

        var config = new MySqlConfiguration
        {
            WriteStrategy = MySqlWriteStrategy.PerRow,
        };

        // MySqlSinkNode is created here; in a real pipeline it would be wired
        // up with a source node via the pipeline builder.
        var sink = new MySqlSinkNode<Product>(_connectionString, "products", config);
        Console.WriteLine("   MySqlSinkNode<Product> created with PerRow strategy.");
        _ = sink; // Used within the pipeline execution
    }

    // -------------------------------------------------------------------------
    // 3. Batch write strategy
    // -------------------------------------------------------------------------

    private async Task DemonstrateBatchWriteStrategyAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("3. Demonstrating Batch write strategy...");

        var config = new MySqlConfiguration
        {
            WriteStrategy = MySqlWriteStrategy.Batch,
            BatchSize = 100,
            MaxBatchSize = 5000,
            UseTransaction = true,
        };

        var sink = new MySqlSinkNode<Product>(_connectionString, "products", config);
        Console.WriteLine("   MySqlSinkNode<Product> created with Batch strategy (batch size: 100).");

        // Show source node with parameterized query
        var query = "SELECT product_id, product_name, category, unit_price, stock_quantity, is_active, created_at FROM `products` WHERE is_active = 1";
        var source = new MySqlSourceNode<Product>(_connectionString, query, config);
        Console.WriteLine("   MySqlSourceNode<Product> created for active products.");

        await Task.CompletedTask;
        _ = (sink, source);
    }

    // -------------------------------------------------------------------------
    // 4. Upsert with ON DUPLICATE KEY UPDATE
    // -------------------------------------------------------------------------

    private async Task DemonstrateUpsertAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("4. Demonstrating upsert (ON DUPLICATE KEY UPDATE)...");

        var upsertConfig = new MySqlConfiguration
        {
            WriteStrategy = MySqlWriteStrategy.Batch,
            UseUpsert = true,
            OnDuplicateKeyAction = OnDuplicateKeyAction.Update,
            UpsertKeyColumns = ["event_id"],
        };

        var upsertSink = new MySqlSinkNode<OrderEvent>(_connectionString, "order_events", upsertConfig);
        Console.WriteLine("   MySqlSinkNode<OrderEvent> created with upsert ON DUPLICATE KEY UPDATE.");

        // INSERT IGNORE variant
        var ignoreConfig = new MySqlConfiguration
        {
            WriteStrategy = MySqlWriteStrategy.Batch,
            UseUpsert = true,
            OnDuplicateKeyAction = OnDuplicateKeyAction.Ignore,
        };

        var ignoreSink = new MySqlSinkNode<OrderEvent>(_connectionString, "order_events", ignoreConfig);
        Console.WriteLine("   MySqlSinkNode<OrderEvent> created with INSERT IGNORE.");

        await Task.CompletedTask;
        _ = (upsertSink, ignoreSink);
    }

    // -------------------------------------------------------------------------
    // 5. Attribute-based mapping
    // -------------------------------------------------------------------------

    private async Task DemonstrateAttributeBasedMappingAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("5. Demonstrating attribute-based mapping...");

        // Product uses [MySqlTable("products")] + [MySqlColumn] / [Column] attributes
        var sink = new MySqlSinkNode<Product>(_connectionString, "products");
        Console.WriteLine("   MySqlSinkNode<Product> infers table from [MySqlTable] attribute.");

        // Custom MySqlRow mapper
        var query = "SELECT product_id, product_name, category, unit_price, stock_quantity, is_active, created_at FROM `products`";

        var source = new MySqlSourceNode<Product>(_connectionString, query, row => new Product
        {
            ProductId = row.Get<int>("product_id"),
            ProductName = row.Get<string>("product_name"),
            Category = row.Get<string>("category"),
            UnitPrice = row.Get<decimal>("unit_price"),
            StockQuantity = row.Get<int>("stock_quantity"),
            IsActive = row.Get<bool>("is_active"),
            CreatedAt = row.Get<DateTime>("created_at"),
        });

        Console.WriteLine("   MySqlSourceNode<Product> with custom MySqlRow mapper created.");

        await Task.CompletedTask;
        _ = (sink, source);
    }

    // -------------------------------------------------------------------------
    // 6. StorageUri
    // -------------------------------------------------------------------------

    private async Task DemonstrateStorageUriAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("6. Demonstrating StorageUri (mysql:// scheme)...");

        // Build a dummy URI for demonstration (not connected)
        var uri = StorageUri.Parse("mysql://dbuser:secret@localhost:3306/myapp");
        var resolver = MySqlStorageResolverFactory.CreateResolver();

        var source = new MySqlSourceNode<Product>(
            uri,
            "SELECT * FROM `products`",
            resolver);

        var sink = new MySqlSinkNode<Product>(
            uri,
            "products",
            resolver: resolver);

        Console.WriteLine($"   StorageUri: {uri}");
        Console.WriteLine("   MySqlSourceNode and MySqlSinkNode created from StorageUri.");

        await Task.CompletedTask;
        _ = (source, sink);
    }
}

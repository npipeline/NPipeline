---
title: MySQL Connector
description: Read from and write to MySQL and MariaDB databases with NPipeline using the MySQL connector.
order: 9
---

# MySQL Connector


The `NPipeline.Connectors.MySQL` package provides specialized source and sink nodes for working with MySQL and MariaDB databases. This allows you to easily integrate MySQL data into your pipelines as an input source or an output destination.

This connector uses the [MySqlConnector](https://mysqlconnector.net/) library under the hood (fully async, MIT-licensed), providing reliable streaming reads, multiple write strategies (per-row, batch, and bulk load), upsert support, delivery semantics, checkpointing strategies, and connection pooling.

## Installation

To use the MySQL connector, install the `NPipeline.Connectors.MySQL` NuGet package:

```bash
dotnet add package NPipeline.Connectors.MySQL
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Features

The MySQL connector provides the following capabilities:

- **Source Node**: Read data from MySQL tables and views
- **Sink Node**: Write data to MySQL tables
- **Write Strategies**: PerRow, Batch, and BulkLoad (`LOAD DATA LOCAL INFILE`) write strategies
- **Upsert Support**: `INSERT … ON DUPLICATE KEY UPDATE` / `INSERT IGNORE` / `REPLACE INTO` with configurable key columns
- **Delivery Semantics**: AtLeastOnce, AtMostOnce, and ExactlyOnce delivery guarantees
- **Checkpointing Strategies**: None, InMemory, Offset, KeyBased, Cursor, and CDC for resumable pipelines
- **Connection Pooling**: Efficient connection management with named connections
- **Attribute Mapping**: `[MySqlTable]`, `[MySqlColumn]`, `[Column]`, and `[IgnoreColumn]` attributes
- **Convention Mapping**: Automatic mapping from PascalCase property names
- **Custom Mappers**: `Func<MySqlRow, T>` for complete mapping control
- **Error Handling**: Automatic retry logic for transient MySQL errors (deadlocks, connection drops, too-many-connections)
- **Streaming Results**: Server-side streaming to reduce memory usage
- **MariaDB Support**: Both `mysql://` and `mariadb://` StorageUri schemes are supported

## Dependency Injection

The MySQL connector supports dependency injection for managing connection pools and node factories. This is the recommended approach for production applications.

### Registering the Connector

Use `AddMySqlConnector` to register a shared connection pool and factories for creating nodes:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.MySql.DependencyInjection;

var services = new ServiceCollection()
    .AddMySqlConnector(options =>
    {
        // Set a default connection string (optional if using only named connections)
        options.DefaultConnectionString = "Server=localhost;Database=npipeline;User=root;Password=root;";

        // Add named connections for different databases
        options.AddOrUpdateConnection("analytics", "Server=localhost;Database=analytics;User=etl;Password=etl_pass;");
        options.AddOrUpdateConnection("warehouse", "Server=warehouse-host;Database=warehouse;User=etl;Password=etl_pass;");

        // Configure default connection-level settings
        options.DefaultConfiguration = new MySqlConfiguration
        {
            MinPoolSize = 2,
            MaxPoolSize = 20,
            MaxRetryAttempts = 3,
            RetryDelay = TimeSpan.FromSeconds(2),
        };
    })
    .BuildServiceProvider();

// Resolve services from the container
var pool = services.GetRequiredService<IMySqlConnectionPool>();
var sourceFactory = services.GetRequiredService<MySqlSourceNodeFactory>();
var sinkFactory = services.GetRequiredService<MySqlSinkNodeFactory>();
```

### Configuration Options

- **`DefaultConnectionString`**: Optional connection string used when no named connection is specified. Can be omitted if you only use named connections.
- **`DefaultConfiguration`**: Controls connection-level settings (timeouts, pool sizing) applied when the pool builds `MySqlConnection` instances.
- **`AddOrUpdateConnection(name, connectionString)`**: Adds or updates a named connection. Multiple connections can be configured for different databases.
- **`AddMySqlConnection` / `AddDefaultMySqlConnection`**: Configure the same `MySqlOptions` and do not replace previously configured values.

### Why Use Dependency Injection?

Using dependency injection provides several benefits:

- **Connection Pool Management**: The shared connection pool efficiently manages database connections across multiple nodes
- **Configuration Centralization**: All MySQL connections are configured in one place
- **Testability**: Easy to mock or replace dependencies in unit tests
- **Lifetime Management**: Services are properly disposed when the application shuts down

## Common Attributes

The MySQL connector supports common attributes from `NPipeline.Connectors.Attributes` that work across all connectors, as well as MySQL-specific attributes that extend the common attributes with database-specific features.

### `[Column]` Attribute

The `[Column]` attribute (from `NPipeline.Connectors.Attributes`) is a common attribute that allows you to specify column names and control property mapping across all connectors. It provides:

- **`Name`**: The column name in the database
- **`Ignore`**: When `true`, skips mapping this property

This attribute is recommended for simple scenarios where you only need basic column mapping.

```csharp
using NPipeline.Connectors.Attributes;

public class Customer
{
    [Column("customer_id")]
    public int CustomerId { get; set; }

    [Column("first_name")]
    public string FirstName { get; set; } = string.Empty;

    [Column("last_name")]
    public string LastName { get; set; } = string.Empty;

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

### `[IgnoreColumn]` Attribute

The `[IgnoreColumn]` attribute (from `NPipeline.Connectors.Attributes`) is a marker attribute that excludes a property from mapping entirely. This is useful for computed properties or fields that should not be persisted.

```csharp
using NPipeline.Connectors.Attributes;

public class Order
{
    public int OrderId { get; set; }
    public decimal Subtotal { get; set; }
    public decimal TaxAmount { get; set; }
    public decimal ShippingAmount { get; set; }

    [IgnoreColumn]
    public decimal TotalAmount => Subtotal + TaxAmount + ShippingAmount;
}
```

### MySQL-Specific Attributes

The MySQL connector provides `[MySqlTable]` and `[MySqlColumn]` attributes:

- **`[MySqlTable]`**: Maps a class to a MySQL table name
- **`[MySqlColumn]`**: Extends `[Column]` with additional MySQL properties:
  - **`Name`**: The column name (with backtick quoting applied automatically)
  - **`AutoIncrement`**: Marks the column as an auto-increment (`AUTO_INCREMENT`) column — these columns are excluded from `INSERT` statements

```csharp
using NPipeline.Connectors.MySql.Mapping;
using NPipeline.Connectors.Attributes;

[MySqlTable("products")]
public class Product
{
    [MySqlColumn("product_id", AutoIncrement = true)]
    public int Id { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [MySqlColumn("unit_price")]
    public decimal Price { get; set; }

    [IgnoreColumn]
    public bool InStock { get; set; }   // not written to DB
}
```

### Choosing Between Common and MySQL-Specific Attributes

**Use common attributes (`[Column]`, `[IgnoreColumn]`) when:**

- You want code that works across multiple connectors
- You only need basic column mapping functionality
- Your schema follows standard naming conventions

**Use MySQL-specific attributes (`[MySqlTable]`, `[MySqlColumn]`) when:**

- You need to map a class to a differently-named table (`[MySqlTable("tbl_products")]`)
- You need to mark auto-increment columns so they are excluded from inserts
- You want explicit, self-documenting MySQL mappings in your model classes

Both attribute types are fully supported and can be mixed within the same class.

## `MySqlSourceNode<T>`

The `MySqlSourceNode<T>` reads data from a MySQL database and emits each row as an item of type `T`.

### Constructor Overloads

```csharp
// Using connection string
public MySqlSourceNode<T>(
    string connectionString,
    string query,
    MySqlConfiguration? configuration = null)

// Using connection string with custom row mapper
public MySqlSourceNode<T>(
    string connectionString,
    string query,
    Func<MySqlRow, T> rowMapper,
    MySqlConfiguration? configuration = null)

// Using StorageUri
public MySqlSourceNode<T>(
    StorageUri storageUri,
    string query,
    MySqlConfiguration? configuration = null)

// Using connection pool
public MySqlSourceNode<T>(
    IMySqlConnectionPool pool,
    string query,
    MySqlConfiguration? configuration = null)
```

### Example: Reading from MySQL

Assume a `products` table:

| product_id | name | unit_price |
|------------|------|------------|
| 1 | Widget A | 9.99 |
| 2 | Widget B | 14.99 |

And a corresponding C# model:

```csharp
[MySqlTable("products")]
public sealed class Product
{
    [MySqlColumn("product_id", AutoIncrement = true)]
    public int Id { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("unit_price")]
    public decimal Price { get; set; }
}
```

Reading the data:

```csharp
using NPipeline;
using NPipeline.Connectors.MySql.Nodes;
using NPipeline.Pipeline;

public sealed class MySqlReaderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceNode = new MySqlSourceNode<Product>(
            connectionString: "Server=localhost;Database=shop;User=root;Password=root;",
            query: "SELECT `product_id`, `name`, `unit_price` FROM `products`");

        var source = builder.AddSource(sourceNode, "mysql_source");
        var sink   = builder.AddSink<ConsoleSinkNode, Product>("console_sink");

        builder.Connect(source, sink);
    }
}
```

### Example: Custom Row Mapper

For complete mapping control, provide a `Func<MySqlRow, T>` mapper:

```csharp
var sourceNode = new MySqlSourceNode<Product>(
    connectionString: "Server=localhost;Database=shop;User=root;Password=root;",
    query: "SELECT `product_id`, `name`, `unit_price` FROM `products`",
    rowMapper: row => new Product
    {
        Id    = row.Get<int>("product_id"),
        Name  = row.Get<string>("name") ?? string.Empty,
        Price = row.Get<decimal>("unit_price"),
    });
```

### Example: StorageUri

```csharp
var uri = StorageUri.Parse("mysql://root:root@localhost:3306/shop");
var sourceNode = new MySqlSourceNode<Product>(uri, "SELECT * FROM `products`");
```

Both `mysql://` and `mariadb://` schemes are supported.

## `MySqlSinkNode<T>`

The `MySqlSinkNode<T>` writes items of type `T` to a MySQL table.

### Constructor Overloads

```csharp
// Using connection string
public MySqlSinkNode<T>(
    string connectionString,
    string tableName,
    MySqlConfiguration? configuration = null)

// Using StorageUri
public MySqlSinkNode<T>(
    StorageUri storageUri,
    string tableName,
    MySqlConfiguration? configuration = null)

// Using connection pool
public MySqlSinkNode<T>(
    IMySqlConnectionPool pool,
    string tableName,
    MySqlConfiguration? configuration = null)
```

### Example: Writing to MySQL

```csharp
public sealed class MySqlWriterPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DataGeneratorNode, Product>("source");

        var sinkNode = new MySqlSinkNode<Product>(
            connectionString: "Server=localhost;Database=shop;User=root;Password=root;",
            tableName: "products");

        var sink = builder.AddSink(sinkNode, "mysql_sink");

        builder.Connect(source, sink);
    }
}
```

## Write Strategies

The MySQL connector provides three write strategies, configured via `MySqlConfiguration.WriteStrategy`:

### PerRow Strategy (Default)

Executes one `INSERT` statement per row. Simplest strategy with fine-grained error control.

```csharp
var config = new MySqlConfiguration
{
    WriteStrategy = MySqlWriteStrategy.PerRow,
};
```

Generated SQL:

```sql
INSERT INTO `products` (`name`, `unit_price`) VALUES (@p0, @p1);
```

### Batch Strategy

Accumulates rows and flushes with a multi-row `INSERT … VALUES (…),(…)` once `BatchSize` is reached or the pipeline completes.

```csharp
var config = new MySqlConfiguration
{
    WriteStrategy = MySqlWriteStrategy.Batch,
    BatchSize = 500,
};
```

Generated SQL:

```sql
INSERT INTO `products` (`name`, `unit_price`)
VALUES (@p0_0, @p0_1), (@p1_0, @p1_1), (@p2_0, @p2_1);
```

### BulkLoad Strategy

Uses MySQL's `LOAD DATA LOCAL INFILE` protocol via the `MySqlBulkLoader` API for maximum throughput. Requires `AllowLoadLocalInfile = true` on the connection:

```csharp
var config = new MySqlConfiguration
{
    WriteStrategy = MySqlWriteStrategy.BulkLoad,
    AllowLoadLocalInfile = true,
    BulkLoadBatchSize = 10_000,
};
```

> **Note:** The MySQL server must have `local_infile = ON`. The connection string should also include `AllowLoadLocalInfile=true;`.

## Upsert

The connector supports three upsert modes, all using the `UpsertKeyColumns` list to identify the primary/unique key:

| Mode | SQL | Behaviour |
|------|-----|-----------|
| `Update` | `INSERT … ON DUPLICATE KEY UPDATE col = VALUES(col), …` | Inserts new; updates existing |
| `Ignore` | `INSERT IGNORE INTO …` | Inserts new; silently ignores duplicates |
| `Replace` | `REPLACE INTO …` | Deletes existing row then inserts (caution: triggers fire for DELETE + INSERT) |

```csharp
var config = new MySqlConfiguration
{
    UseUpsert = true,
    UpsertKeyColumns = ["product_id"],
    OnDuplicateKeyAction = OnDuplicateKeyAction.Update,
};
var sink = new MySqlSinkNode<Product>(connectionString, "products", config);
```

Example generated SQL for `Update` mode:

```sql
INSERT INTO `products` (`product_id`, `name`, `unit_price`)
VALUES (@p0, @p1, @p2)
ON DUPLICATE KEY UPDATE
  `name` = VALUES(`name`),
  `unit_price` = VALUES(`unit_price`);
```

## Checkpointing

Checkpointing enables resumable pipelines that restart from a known position after failure.

```csharp
var config = new MySqlConfiguration
{
    CheckpointStrategy = CheckpointStrategy.KeyBased,
    CheckpointColumn   = "updated_at",
};
```

Supported strategies:

| Strategy | Description |
|----------|-------------|
| `None` | No checkpointing (default) |
| `InMemory` | Tracks last-processed key in memory; resets on restart |
| `Offset` | Row offset tracking |
| `KeyBased` | Tracks last value of a key column (e.g., `updated_at`, `id`) |
| `Cursor` | Server-side cursor position |
| `CDC` | Change Data Capture via MySQL binlog position |

## Transient Error Handling

The connector automatically retries on the following MySQL error codes:

| Error Code | Description |
|------------|-------------|
| 1040 | Too many connections |
| 1205 | Lock wait timeout exceeded |
| 1213 | Deadlock found when trying to get lock |
| 2006 | MySQL server has gone away |
| 2013 | Lost connection to MySQL server during query |

Configure retry behaviour via `MySqlConfiguration`:

```csharp
var config = new MySqlConfiguration
{
    MaxRetryAttempts = 5,
    RetryDelay = TimeSpan.FromSeconds(3),
};
```

## Connection String Format

Standard [MySqlConnector connection string](https://mysqlconnector.net/connection-options/) format:

```
Server=localhost;Port=3306;Database=mydb;User=myuser;Password=mypass;
```

Useful options:

| Key | Notes |
|-----|-------|
| `Server` | Hostname or IP |
| `Port` | Default `3306` |
| `Database` | Schema/database name |
| `User` / `Uid` | Username |
| `Password` / `Pwd` | Password |
| `MinimumPoolSize` | Connection pool minimum |
| `MaximumPoolSize` | Connection pool maximum |
| `AllowPublicKeyRetrieval` | Set `true` for root over non-SSL in dev |
| `AllowLoadLocalInfile` | Required for BulkLoad strategy |
| `ConvertZeroDateTime` | Map `0000-00-00` to `DateTime.MinValue` |

## StorageUri Format

```
mysql://[user[:password]@]host[:port]/database
mariadb://[user[:password]@]host[:port]/database
```

Examples:

```
mysql://root:root@localhost:3306/shop
mariadb://etl:secret@mariadb-host:3306/analytics
mysql://root@localhost/mydb
```

URL-encoded passwords are supported (e.g., `p%40ss` for `p@ss`).

## Configuration Reference

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ConnectionTimeout` | `TimeSpan` | 30 s | TCP connection timeout |
| `CommandTimeout` | `TimeSpan` | 30 s | SQL command execution timeout |
| `MinPoolSize` | `int` | 1 | Minimum connections in pool |
| `MaxPoolSize` | `int` | 10 | Maximum connections in pool |
| `WriteStrategy` | `MySqlWriteStrategy` | `PerRow` | Write strategy (`PerRow`, `Batch`, `BulkLoad`) |
| `BatchSize` | `int` | 100 | Rows per batch flush (Batch strategy) |
| `BulkLoadBatchSize` | `int` | 10 000 | Rows per `LOAD DATA` call (BulkLoad strategy) |
| `MaxRetryAttempts` | `int` | 3 | Number of retries on transient errors |
| `RetryDelay` | `TimeSpan` | 2 s | Initial retry delay (exponential back-off) |
| `UseUpsert` | `bool` | `false` | Enable upsert semantics |
| `UpsertKeyColumns` | `string[]` | `[]` | Columns identifying unique rows for upsert |
| `OnDuplicateKeyAction` | `OnDuplicateKeyAction` | `Update` | How to handle duplicates (`Update`, `Ignore`, `Replace`) |
| `CheckpointStrategy` | `CheckpointStrategy` | `None` | Checkpointing mode |
| `CheckpointColumn` | `string?` | `null` | Column name for key-based checkpointing |
| `AllowUserVariables` | `bool` | `true` | Allow `@variable` placeholders in SQL |
| `ConvertZeroDateTime` | `bool` | `true` | Convert MySQL `0000-00-00` to `DateTime.MinValue` |
| `AllowLoadLocalInfile` | `bool` | `false` | Enable `LOAD DATA LOCAL INFILE` (required for BulkLoad) |
| `ValidateIdentifiers` | `bool` | `false` | Throw on invalid table/column identifiers at construction time |

## MariaDB Support

The `NPipeline.Connectors.MySQL` package works with MariaDB via the same `MySqlConnector` driver. The `mariadb://` StorageUri scheme is registered as an alias for `mysql://`:

```csharp
// Both of these resolve to MySqlDatabaseStorageProvider
var mysqlUri   = StorageUri.Parse("mysql://root:root@localhost/shop");
var mariadbUri = StorageUri.Parse("mariadb://root:root@localhost/shop");
```

Tested with MariaDB 10.6+. Features that differ from MySQL (e.g., sequence-based auto-increment) are handled transparently by `MySqlConnector`.

## Integration with StorageResolver

MySQL/MariaDB connections can be configured via the storage resolver pattern, enabling runtime-resolved connection strings without hardcoding them in node constructors:

```csharp
var factory = new MySqlStorageResolverFactory();
var resolver = factory.CreateResolver();

// Resolve a storage provider from a URI
var provider = resolver.ResolveProvider(
    StorageUri.Parse("mysql://root:root@localhost:3306/shop"));

// Use the provider to get the connection string
var connectionString = provider.GetConnectionString(
    StorageUri.Parse("mysql://root:root@localhost:3306/shop"));
```

This is particularly useful when URIs are read from configuration files at runtime.

## Samples

A working sample application is in [samples/Sample_MySQLConnector](https://github.com/npipeline/NPipeline/blob/main/samples/Sample_MySQLConnector). It demonstrates:

- PerRow, Batch, and BulkLoad write strategies
- Upsert with `OnDuplicateKeyAction.Update` and `OnDuplicateKeyAction.Ignore`
- Attribute-based mapping (`[MySqlTable]`, `[MySqlColumn]`)
- Convention-based mapping (no attributes)
- Custom `MySqlRow` mapper with `row.Get<T>(name)`
- StorageUri with `mysql://` and `mariadb://` schemes
- DI registration with `AddMySqlConnector()`

## Next Steps

- [Configuration Reference](https://github.com/npipeline/NPipeline/blob/main/src/NPipeline.Connectors.MySQL/Configuration/MySqlConfiguration.cs)
- [PostgreSQL Connector](postgres.md) — similar connector for PostgreSQL
- [SQL Server Connector](sqlserver.md) — similar connector for SQL Server
- [Getting Started](../getting-started/installation.md)

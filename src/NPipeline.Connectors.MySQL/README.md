# NPipeline.Connectors.MySQL

A fully-async MySQL and MariaDB connector for [NPipeline](https://github.com/npipeline/npipeline), built on [MySqlConnector](https://mysqlconnector.net/) (MIT).

## Installation

```bash
dotnet add package NPipeline.Connectors.MySQL
```

## Quick Start

```csharp
using NPipeline.Connectors.MySql.Nodes;

// Source: read rows from MySQL
var source = new MySqlSourceNode<Product>(
    connectionString: "Server=localhost;Database=shop;User=root;Password=root;",
    query: "SELECT * FROM `products`");

// Sink: write rows to MySQL
var sink = new MySqlSinkNode<Product>(
    connectionString: "Server=localhost;Database=shop;User=root;Password=root;",
    tableName: "products");
```

## Dependency Injection

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.MySql.DependencyInjection;

services.AddMySqlConnector(options =>
{
    options.DefaultConnectionString =
        "Server=localhost;Database=shop;User=root;Password=root;";

    options.AddOrUpdateConnection("analytics",
        "Server=analytics-host;Database=analytics;User=etl;Password=secret;");

    options.DefaultConfiguration = new MySqlConfiguration
    {
        MinPoolSize = 2,
        MaxPoolSize = 20,
        MaxRetryAttempts = 3,
        RetryDelay = TimeSpan.FromSeconds(2),
    };
});
```

## Attribute Mapping

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
    public bool InStock { get; set; }
}
```

## Write Strategies

| Strategy | Class | Notes |
|----------|-------|-------|
| PerRow | `MySqlPerRowWriter` | One `INSERT` per row; simplest, lowest throughput |
| Batch | `MySqlBatchWriter` | Multi-row `INSERT VALUES (…),(…)` |
| BulkLoad | `MySqlBulkLoadWriter` | `LOAD DATA LOCAL INFILE` — highest throughput |

Configure via `MySqlConfiguration.WriteStrategy`:

```csharp
var config = new MySqlConfiguration
{
    WriteStrategy = MySqlWriteStrategy.Batch,
    BatchSize = 500,
};
var sink = new MySqlSinkNode<Product>(connectionString, "products", config);
```

## Upsert

```csharp
var config = new MySqlConfiguration
{
    UseUpsert = true,
    UpsertKeyColumns = ["product_id"],
    OnDuplicateKeyAction = OnDuplicateKeyAction.Update,   // or .Ignore / .Replace
};
var sink = new MySqlSinkNode<Product>(connectionString, "products", config);
```

Generated SQL example:

```sql
INSERT INTO `products` (`product_id`, `name`, `unit_price`)
VALUES (@p0, @p1, @p2)
ON DUPLICATE KEY UPDATE `name` = VALUES(`name`), `unit_price` = VALUES(`unit_price`);
```

## StorageUri

```csharp
// mysql:// or mariadb:// schemes are both supported
var uri = StorageUri.Parse("mysql://root:root@localhost:3306/shop");
var source = new MySqlSourceNode<Product>(uri, "SELECT * FROM `products`");
var sink   = new MySqlSinkNode<Product>(uri, "products");
```

## Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `ConnectionTimeout` | 30 s | TCP connect timeout |
| `CommandTimeout` | 30 s | SQL execution timeout |
| `MinPoolSize` | 1 | Minimum open connections in pool |
| `MaxPoolSize` | 10 | Maximum open connections in pool |
| `WriteStrategy` | `PerRow` | `PerRow`, `Batch`, `BulkLoad` |
| `BatchSize` | 100 | Rows per batch (Batch strategy) |
| `MaxRetryAttempts` | 3 | Retry count on transient errors |
| `RetryDelay` | 2 s | Initial retry back-off |
| `UseUpsert` | `false` | Enable upsert semantics |
| `UpsertKeyColumns` | `[]` | Columns forming the upsert key |
| `OnDuplicateKeyAction` | `Update` | `Update`, `Ignore`, `Replace` |
| `AllowUserVariables` | `true` | Allow `@variable` syntax |
| `ConvertZeroDateTime` | `true` | Map MySQL `0000-00-00` to `DateTime.MinValue` |
| `AllowLoadLocalInfile` | `false` | Enable `LOAD DATA LOCAL INFILE` (BulkLoad) |

## Transient Error Handling

The connector automatically retries on the following MySQL error codes:

| Code | Description |
|------|-------------|
| 1040 | Too many connections |
| 1205 | Lock wait timeout exceeded |
| 1213 | Deadlock found |
| 2006 | MySQL server has gone away |
| 2013 | Lost connection to MySQL server |

## Checkpointing

```csharp
var config = new MySqlConfiguration
{
    CheckpointStrategy = CheckpointStrategy.KeyBased,
    CheckpointColumn  = "updated_at",
};
```

Supported strategies: `None`, `InMemory`, `Offset`, `KeyBased`, `Cursor`, `CDC`.

## Custom Row Mapper

```csharp
var source = new MySqlSourceNode<Product>(
    connectionString,
    "SELECT product_id, name FROM `products`",
    row => new Product
    {
        Id   = row.Get<int>("product_id"),
        Name = row.Get<string>("name") ?? string.Empty,
    });
```

## MariaDB Support

Both `mysql://` and `mariadb://` StorageUri schemes resolve to `MySqlDatabaseStorageProvider`. The `MySqlConnector` driver is fully compatible with MariaDB 10.5+.

## License

MIT

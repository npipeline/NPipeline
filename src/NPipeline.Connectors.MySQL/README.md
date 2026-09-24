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
        Resilience = MySqlConnectorResilience.Default,
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

| Strategy | Class                 | Notes                                             |
|----------|-----------------------|---------------------------------------------------|
| PerRow   | `MySqlPerRowWriter`   | One `INSERT` per row; simplest, lowest throughput |
| Batch    | `MySqlBatchWriter`    | Multi-row `INSERT VALUES (…),(…)`                 |
| BulkLoad | `MySqlBulkLoadWriter` | `LOAD DATA LOCAL INFILE` - highest throughput     |

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

| Property               | Default  | Description                                   |
|------------------------|----------|-----------------------------------------------|
| `ConnectionTimeout`    | 30 s     | TCP connect timeout                           |
| `CommandTimeout`       | 30 s     | SQL execution timeout                         |
| `MinPoolSize`          | 1        | Minimum open connections in pool              |
| `MaxPoolSize`          | 10       | Maximum open connections in pool              |
| `WriteStrategy`        | `PerRow` | `PerRow`, `Batch`, `BulkLoad`                 |
| `BatchSize`            | 100      | Rows per batch (Batch strategy)               |
| `Resilience` | `MySqlConnectorResilience.Default` | How transient failures are retried (see Resilience) |
| `UseUpsert`            | `false`  | Enable upsert semantics                       |
| `UpsertKeyColumns`     | `[]`     | Columns forming the upsert key                |
| `OnDuplicateKeyAction` | `Update` | `Update`, `Ignore`, `Replace`                 |
| `AllowUserVariables`   | `true`   | Allow `@variable` syntax                      |
| `ConvertZeroDateTime`  | `true`   | Map MySQL `0000-00-00` to `DateTime.MinValue` |
| `AllowLoadLocalInfile` | `false`  | Enable `LOAD DATA LOCAL INFILE` (BulkLoad)    |

## Resilience

The sink retries transient failures with [NResilience](https://github.com/nresilience/NResilience). The `Resilience`
property on `MySqlConfiguration` configures it. The default, `MySqlConnectorResilience.Default`, does the following:

- Makes up to four attempts (three retries). It replaces `MaxRetryAttempts = 3` and `RetryDelay = 2 s`.
- Retries lock wait timeouts (1205), deadlocks (1213), and lost connections (2006, 2013).
- Treats too many connections (1040 and 1203) as throttling, which waits longer: backoff starts at 5 seconds.
- Doesn't retry other errors, such as a duplicate key or a missing table.
- Waits with exponential backoff and full jitter, from 2 seconds up to 30 seconds.
- Has no attempt timeout and no deadline. The driver's own timeout bounds each attempt: `CommandTimeout` for rows and batches, and `BulkLoadTimeout` for bulk loads.
  A long bulk write isn't cut off by a retry policy's timeout.

Each write strategy retries one unit of work that commits all or nothing, so a retry never inserts rows that an
earlier attempt committed:

- `PerRow`: one `INSERT` per row.
- `Batch`: one multi-row statement per flush.
- `BulkLoad`: one `LOAD DATA LOCAL INFILE` per flush.

Each unit commits all or nothing on a transactional engine such as InnoDB. A non-transactional engine such as MyISAM keeps the rows a failed statement wrote before it failed, so a retry could insert them again. For such tables, turn retries off with `Resilience.None`.

With `DeliverySemantic.ExactlyOnce`, the sink wraps all writes in one transaction. A failure can abort that whole
transaction, so the writers make one attempt and the sink rolls the transaction back. A batch that fails isn't
written again when the writer is disposed.

To change a setting, derive a policy with a `with` expression:

```csharp
var config = new MySqlConfiguration
{
    Resilience = MySqlConnectorResilience.Default with { Attempts = 6 },
};
```

To turn retries off, use `Resilience.None`.

The connector is the only layer that retries; MySqlConnector doesn't retry commands. Retries aren't logged by the
connector. To observe them, attach a listener: `MySqlConnectorResilience.Default.WithListener(e => ...)`.

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

Both `mysql://` and `mariadb://` StorageUri schemes resolve to `MySqlDatabaseStorageProvider`. The `MySqlConnector` driver is fully compatible with MariaDB
10.5+.

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.

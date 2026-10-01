# NPipeline MySQL Connector Sample

This sample demonstrates the **NPipeline MySQL Connector** - a fully-async MySQL/MariaDB connector built on top
of [MySqlConnector](https://mysqlconnector.net/) (MIT).

## Prerequisites

- .NET 8, 9 or 10 SDK
- A running MySQL 8.x (or MariaDB 10.6+) instance
- Optionally: Docker for a quick local MySQL instance

### Quick Start with Docker

```bash
docker run --rm -d \
  --name mysql-npipeline \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=npipeline_sample \
  -p 3306:3306 \
  mysql:8.4
```

## Running the Sample

```bash
cd samples/Sample_MySQLConnector

# Using the default connection string (root/root on localhost:3306)
dotnet run

# Or provide your own
dotnet run -- --connection-string "Server=myhost;Port=3306;Database=mydb;User=myuser;Password=mypass;"
```

## What This Sample Demonstrates

The sample creates the tables, then builds the nodes for each feature and reports what each is configured to do.

| Feature               | Description                                                                                         |
|-----------------------|-----------------------------------------------------------------------------------------------------|
| **PerRow strategy**   | `WriteStrategy = MySqlWriteStrategy.PerRow`: one statement per row, simplest to debug               |
| **Batch strategy**    | The default: multi-row `INSERT … VALUES (…),(…)` for high throughput; `BulkLoad` uses `LOAD DATA`    |
| **Upsert**            | `SqlUpsert.On("event_id")` writes `INSERT … ON DUPLICATE KEY UPDATE`; `SqlUpsertAction.Ignore` keeps existing rows |
| **Attribute mapping** | `[MySqlColumn]` (including `AutoIncrement`, which is read but never written), `[Column]`, `[IgnoreColumn]` |
| **Manual mapper**     | `MySqlNodes.Source(connectionString, query, row => …)` maps each `SqlRow` yourself                  |
| **StorageUri**        | `mysql://user:pass@host:port/db` and `mariadb://…` schemes                                          |

The factory is `MySqlNodes`, because the MySqlConnector driver already uses `MySqlConnector` for its namespace.

## Models

- **`Product`** - uses `[MySqlColumn]` / `[Column]`, with `AutoIncrement` on the id
- **`OrderEvent`** - demonstrates upsert on the `event_id` primary key
- **`ProductSummary`** - shows convention-based mapping (no attributes required)

## Key NPipeline APIs Used

```csharp
// Create a source node
var source = MySqlNodes.Source<Product>(connectionString, "SELECT * FROM `products`");

// Create a sink node, with options adjusted by a `with` expression
var sink = MySqlNodes.Sink<Product>(connectionString, "products", o => o with { BatchSize = 100 });

// Upsert: INSERT … ON DUPLICATE KEY UPDATE on the key columns
var upsert = MySqlNodes.Sink<OrderEvent>(connectionString, "order_events", o => o with { Upsert = SqlUpsert.On("event_id") });

// A manual mapper over SqlRow
var mapped = MySqlNodes.Source(connectionString, "SELECT product_id, product_name FROM `products`",
    row => (Id: row.Get<int>("product_id"), Name: row.Get<string>("product_name")));

// StorageUri
var uri = StorageUri.Parse("mysql://root:root@localhost:3306/npipeline_sample");
var fromUri = MySqlNodes.Source<Product>(uri, "SELECT * FROM `products`");
```

Bulk loads (`WriteStrategy = MySqlWriteStrategy.BulkLoad`) need `AllowLoadLocalInfile=true` in the connection string and
`local_infile` enabled on the server.

## Further Documentation

- [MySQL Connector Guide](../../docs/connectors/mysql.md)
- [SQL Connectors: Shared Behaviour](../../docs/connectors/sql-connectors.md)
- [NPipeline Documentation](../../docs/index.md)

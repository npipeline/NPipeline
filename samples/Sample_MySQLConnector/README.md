# NPipeline MySQL Connector Sample

This sample demonstrates the **NPipeline MySQL Connector** — a fully-async MySQL/MariaDB connector built on top of [MySqlConnector](https://mysqlconnector.net/) (MIT).

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

| Feature | Description |
|---------|-------------|
| **PerRow strategy** | Inserts one row at a time — useful for small writes with rich error control |
| **Batch strategy** | Builds multi-row `INSERT VALUES (…),(…)` for high throughput |
| **Upsert** | `INSERT … ON DUPLICATE KEY UPDATE`, `INSERT IGNORE`, `REPLACE INTO` |
| **Attribute mapping** | `[MySqlTable]`, `[MySqlColumn]`, `[Column]`, `[IgnoreColumn]` |
| **Custom mapper** | `Func<MySqlRow, T>` mapper passed directly to `MySqlSourceNode` |
| **StorageUri** | `mysql://user:pass@host:port/db` and `mariadb://…` schemes |

## Models

- **`Product`** — uses `[MySqlTable]` + `[MySqlColumn]` / `[Column]` with `AutoIncrement`
- **`OrderEvent`** — demonstrates upsert on `event_id` primary key
- **`ProductSummary`** — shows convention-based mapping (no attributes required)

## Key NPipeline APIs Used

```csharp
// Create a source node
var source = new MySqlSourceNode<Product>(connectionString, "SELECT * FROM `products`");

// Create a sink node
var sink = new MySqlSinkNode<Product>(connectionString, "products");

// Upsert configuration
var config = new MySqlConfiguration
{
    UseUpsert = true,
    UpsertKeyColumns = ["product_id"],
    OnDuplicateKeyAction = OnDuplicateKeyAction.Update,
};

// StorageUri
var uri = StorageUri.Parse("mysql://root:root@localhost:3306/npipeline_sample");
var source = new MySqlSourceNode<Product>(uri, "SELECT * FROM `products`");
```

## Further Documentation

- [MySQL Connector Guide](../../docs/connectors/mysql.md)
- [Configuration Reference](../../src/NPipeline.Connectors.MySQL/Configuration/MySqlConfiguration.cs)
- [NPipeline Documentation](../../docs/index.md)

# NPipeline.Connectors.Snowflake

Snowflake data warehouse connector for NPipeline. Enables streaming reads and high-throughput writes to Snowflake using the official Snowflake .NET connector.

## Features

- **Streaming reads** with configurable fetch size and checkpoint support
- **Three write strategies**: PerRow, Batch (multi-row INSERT up to 16,384 rows), and StagedCopy (PUT + COPY INTO)
- **MERGE upsert** support with configurable key columns and merge actions
- **Attribute-based mapping** with convention-over-configuration
- **Transient error detection** with exponential backoff and jitter
- **Full DI support** with factory pattern

## Quick Start

```csharp
// Reading from Snowflake
var source = new SnowflakeSourceNode<Customer>(
    "account=myaccount;user=myuser;password=mypassword;db=MYDB;schema=PUBLIC;warehouse=COMPUTE_WH",
    "SELECT * FROM CUSTOMERS");

// Writing to Snowflake (batch)
var sink = new SnowflakeSinkNode<Customer>(
    "account=myaccount;user=myuser;password=mypassword;db=MYDB;schema=PUBLIC;warehouse=COMPUTE_WH",
    "CUSTOMERS",
    new SnowflakeConfiguration { WriteStrategy = SnowflakeWriteStrategy.Batch });
```

## Documentation

See the [full documentation](https://npipeline.dev/connectors/snowflake) for detailed usage instructions.

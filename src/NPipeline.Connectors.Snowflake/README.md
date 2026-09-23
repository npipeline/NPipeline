# NPipeline.Connectors.Snowflake

Snowflake data warehouse connector for NPipeline. Enables streaming reads and high-throughput writes to Snowflake using the official Snowflake .NET connector.

## Features

- **Streaming reads** with configurable fetch size and checkpoint support
- **Three write strategies**: PerRow, Batch (multi-row INSERT up to 16,384 rows), and StagedCopy (PUT + COPY INTO)
- **MERGE upsert** support with configurable key columns and merge actions
- **Attribute-based mapping** with convention-over-configuration
- **NResilience retries** for transient errors, with exponential backoff, full jitter, and idempotent staged copies
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

## Resilience

The sink retries transient failures with [NResilience](https://github.com/nresilience/NResilience). The `Resilience`
property on `SnowflakeConfiguration` configures it. The default, `SnowflakeConnectorResilience.Default`, does the following:

- Makes up to four attempts (three retries). It replaces `MaxRetryAttempts = 3` and `RetryDelay = 2 s`.
- Retries errors the server returns for a statement: network errors (200002), service unavailability (390144),
  statement timeouts (625), and internal errors (604).
- Treats a statement error with a throttling message as throttling, which waits longer: backoff starts at 10 seconds.
- Doesn't retry failures the driver has already retried (see below), or other errors, such as a missing object or a
  permission error.
- Waits with exponential backoff and full jitter, from 2 seconds up to 60 seconds.
- Has no attempt timeout and no deadline. The driver's own timeout bounds each attempt: `CommandTimeout`.
  A long bulk write isn't cut off by a retry policy's timeout.

Each write strategy retries one unit of work that commits all or nothing, so a retry never inserts rows that an
earlier attempt committed:

- `PerRow`: one `INSERT` per row.
- `Batch`: one multi-row `INSERT` or `MERGE` statement per flush.
- `StagedCopy`: two steps per flush, each retried on its own: the `PUT` of the staged file, which writes nothing to the table, and then `COPY INTO` from that same file. Snowflake's load metadata skips a file it has already loaded, so a `COPY INTO` retried after a failure that hid its success doesn't load the rows twice. The writer
  also checks both results: a `PUT` whose result reports a file as anything but `UPLOADED` or `SKIPPED` fails the flush
  (the driver reports an upload it gave up on this way, not as an error), and so does a first `COPY INTO` that processes
  no files, because the rows would otherwise be lost without an error. Neither failure is retried.

With `DeliverySemantic.ExactlyOnce`, the sink wraps all writes in one transaction. A failure can abort that whole
transaction, so the writers make one attempt and the sink rolls the transaction back. A batch that fails isn't
written again when the writer is disposed.

To change a setting, derive a policy with a `with` expression:

```csharp
var config = new SnowflakeConfiguration
{
    Resilience = SnowflakeConnectorResilience.Default with { Attempts = 6 },
};
```

To turn retries off, use `Resilience.None`.

Exactly one layer retries each kind of failure:

- **The driver retries HTTP requests.** Snowflake.Data retries every HTTP request of a statement on its own:
  transport errors, HTTP timeouts, and 5xx, 403, 408, and 429 responses, up to `MAXHTTPRETRIES` (default 7) times
  within `RETRY_TIMEOUT` (default 300 seconds). That includes the polling for a running query's result, which the
  connector could only repeat by running the whole statement again. Configure these in the connection string.
- **The connector retries statements.** It retries only errors the server returns for the statement itself, as listed
  above. What the driver reports after giving up on its own retries is permanent to the connector: an
  `HttpRequestException`, a request timeout (270007, which can arrive wrapped in an `OperationCanceledException`), an I/O
  error on `PUT` (270058, after the driver's own upload attempts), any other driver error in the 270000 range, and a
  lost session (390111).

If you set `DISABLERETRY=true` in the connection string, failed HTTP responses aren't retried by either layer.

Retries aren't logged by the connector. To observe them, attach a listener: `SnowflakeConnectorResilience.Default.WithListener(e => ...)`.

## Documentation

See the [full documentation](https://docs.npipeline.net/connectors/snowflake) for detailed usage instructions.

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.

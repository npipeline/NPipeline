# NPipeline SQL Server Connector Analyzers

SQL Server connector-specific analyzers for NPipeline.

## Installation

This analyzer is distributed as a separate NuGet package to provide connector-specific diagnostics only to users who use the SQL Server connector.

```bash
dotnet add package NPipeline.Connectors.SqlServer.Analyzers
```

## Analyzers

### NP9502: SQL Server source with checkpointing requires ORDER BY clause

**Category:** Reliability
**Default Severity:** Warning

When using checkpointing with SQL Server source nodes, the SQL query must include an `ORDER BY` clause on a unique, monotonically increasing column. This
ensures consistent row ordering across checkpoint restarts. Without proper ordering, checkpointing may skip rows or process duplicates.

#### Example

```csharp
// ❌ Warning: Missing ORDER BY clause
var source = new SqlServerSourceNode<MyRecord>(
    connectionString,
    "SELECT id, name, created_at FROM my_table",
    configuration: new SqlServerConfiguration
    {
        CheckpointStrategy = CheckpointStrategy.Offset // Checkpointing enabled
    }
);

// ✅ Correct: Includes ORDER BY clause
var source = new SqlServerSourceNode<MyRecord>(
    connectionString,
    "SELECT id, name, created_at FROM my_table ORDER BY id",
    configuration: new SqlServerConfiguration
    {
        CheckpointStrategy = CheckpointStrategy.Offset
    }
);
```

#### Why This Matters

Checkpointing tracks the position of processed rows to enable recovery from failures. Without a consistent `ORDER BY` clause:

- **Data Loss:** Rows may be skipped during recovery
- **Data Duplication:** Rows may be processed multiple times
- **Inconsistent State:** Checkpoint positions become unreliable

#### Recommended Ordering Columns

Use a unique, monotonically increasing column such as:

- `id` (primary key)
- `created_at` (timestamp)
- `updated_at` (timestamp)
- `timestamp` (timestamp column)
- Any auto-incrementing or sequential column

## Configuration

This analyzer is automatically enabled when the package is referenced. No additional configuration is required.

## Scope

This analyzer only applies to:

- `SqlServerSourceNode<T>` instantiations
- Queries with checkpointing enabled (any strategy except `CheckpointStrategy.None`)
- String literal queries (not interpolated strings or dynamic queries)

## Related Documentation

- [SQL Server Connector Documentation](https://npipeline.dev/docs/connectors/sqlserver)
- [Checkpointing Best Practices](https://npipeline.dev/docs/guides/checkpointing)
- [Production Recommendations](https://npipeline.dev/docs/connectors/sqlserver-production-recommendations)

## License

MIT License - see [LICENSE](https://github.com/npipeline/NPipeline/blob/main/LICENSE) for details.

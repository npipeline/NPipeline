# Sample_SnowflakeConnector

This sample demonstrates the usage of the Snowflake connector in NPipeline for reading from and writing to Snowflake databases.

## Overview

The Snowflake Connector sample showcases how to use NPipeline's Snowflake connector to perform various database operations including reading data, writing data
with different strategies (PerRow, Batch, StagedCopy), attribute-based and convention-based mapping, upsert (MERGE) operations, and data transformations.

## Features Demonstrated

### 1. Reading from Snowflake

- **`SnowflakeConnector.Source<T>(connectionString, query, configure)`**: read tables, views and joins as records
- Parameterized queries (`Parameters = [new DatabaseParameter(":since", value)]`)
- Streaming results, with nothing buffered
- Queries with JOINs and aggregates mapped to a record type

### 2. Writing to Snowflake

- **`SnowflakeConnector.Sink<T>(connectionString, table, configure)`**: write records to Snowflake tables
- **PerRow Write Strategy**: one statement per row (simplest to debug)
- **Batch Write Strategy** (default): multi-row `INSERT` statements
- **StagedCopy Write Strategy**: bulk load via `PUT` + `COPY INTO` (best for large volumes)
- **`Transaction`**: `PerBatch` by default, so each batch lands whole and a transient failure is retried safely

### 3. Mapping Strategies

#### Attribute-Based Mapping

- **`[SnowflakeColumn]`**: Snowflake-specific features (`DbType`, `NativeTypeName`, `Size`, `Identity`; identity columns are read but never written)
- **`[Column]`**: the common attribute for simple column name mappings
- **`[IgnoreColumn]`**: exclude computed properties from mapping

#### Convention-Based Mapping

- Members map to UPPER_SNAKE_CASE columns (`CustomerId` to `CUSTOMER_ID`), as Snowflake stores unquoted names
- No attributes required for simple scenarios
- Case-insensitive column matching

### 4. Upsert (MERGE) Operations

- `Upsert = SqlUpsert.On("CUSTOMER_ID")` writes `MERGE INTO … USING (SELECT … FROM VALUES …)`
- `SqlUpsertAction.Update` (the default) updates the other columns of a row whose key exists; `Ignore` inserts only new keys

### 5. Connections

- Account, warehouse, role and database are set in the connection string
- Password and key-pair authentication (`authenticator=snowflake_jwt;private_key_file=…`)
- `snowflake://` storage URIs, and named connections through `ISnowflakeConnectionPool`

### 6. Error Handling

- Transient statement errors are retried with `SnowflakeConnectorResilience`
- A row that fails to map goes through `RowErrorHandler` (`Fail`, `Skip`, `DeadLetter`)
- A batch that fails can fail the write or go to the dead-letter sink (`FailedBatches`)

## Expected Output

```
=== NPipeline Sample: Snowflake Connector ===

This sample demonstrates reading from and writing to Snowflake using NPipeline.

Registered NPipeline services and scanned assemblies for nodes.

Pipeline Description:
Snowflake Connector Sample Pipeline
====================================
...

Connection Information:
  - Account: myaccount
  - Database: mydb

Starting pipeline execution...

Step 1: Setting up schema...
  ✓ Created CUSTOMERS, ORDERS, and ENRICHED_CUSTOMERS tables

Step 2: Writing customers (Batch strategy)...
  ✓ Inserted 5 customers in 1.23s

Step 3: Writing additional orders (PerRow strategy)...
  ✓ Inserted 3 orders in 0.45s

Step 4: Bulk loading orders (Staged COPY strategy)...
  ✓ Loaded 100 orders via PUT+COPY in 3.45s

Step 5: Reading and transforming customers...
  ✓ Read 5 customers, enriched with computed fields

Step 6: Querying order summaries...
  ✓ Retrieved 5 aggregated order summaries

Step 7: Upserting updated customers (MERGE)...
  ✓ Merged 3 customer updates in 0.78s

Step 8: Cleaning up...
  ✓ Dropped test tables

Sample completed successfully!
```

## Database Schema

The sample creates the following tables in the `PUBLIC` schema:

### CUSTOMERS

| Column       | Type                 | Description             |
|--------------|----------------------|-------------------------|
| ID           | NUMBER AUTOINCREMENT | Primary key             |
| FIRST_NAME   | VARCHAR(100)         | Customer first name     |
| LAST_NAME    | VARCHAR(100)         | Customer last name      |
| EMAIL        | VARCHAR(255)         | Email address           |
| PHONE_NUMBER | VARCHAR(50)          | Phone number (nullable) |
| CREATED_AT   | TIMESTAMP_NTZ        | Registration date       |
| STATUS       | VARCHAR(50)          | Customer status         |

### ORDERS

| Column           | Type                 | Description                 |
|------------------|----------------------|-----------------------------|
| ORDER_ID         | NUMBER AUTOINCREMENT | Primary key                 |
| CUSTOMER_ID      | NUMBER               | Foreign key to CUSTOMERS    |
| ORDER_DATE       | TIMESTAMP_NTZ        | Order date                  |
| AMOUNT           | NUMBER(18,2)         | Order amount                |
| STATUS           | VARCHAR(50)          | Order status                |
| SHIPPING_ADDRESS | VARCHAR(500)         | Shipping address (nullable) |
| NOTES            | VARCHAR(1000)        | Notes (nullable)            |

### ENRICHED_CUSTOMERS

| Column              | Type          | Description                       |
|---------------------|---------------|-----------------------------------|
| CUSTOMER_ID         | NUMBER        | Primary key                       |
| FULL_NAME           | VARCHAR(255)  | Full name                         |
| EMAIL               | VARCHAR(255)  | Email address                     |
| PHONE_NUMBER        | VARCHAR(50)   | Phone number (nullable)           |
| CREATED_AT          | TIMESTAMP_NTZ | Registration date                 |
| STATUS              | VARCHAR(50)   | Status                            |
| TOTAL_ORDERS        | NUMBER        | Total order count                 |
| TOTAL_SPENT         | NUMBER(18,2)  | Total spending                    |
| AVERAGE_ORDER_VALUE | NUMBER(18,2)  | Average per order                 |
| CUSTOMER_TIER       | VARCHAR(50)   | Tier: Bronze/Silver/Gold/Platinum |
| LAST_ORDER_DATE     | TIMESTAMP_NTZ | Last order date (nullable)        |
| ENRICHMENT_DATE     | TIMESTAMP_NTZ | When enrichment was calculated    |

## Key Concepts

### Write Strategies

| Strategy       | Best For                        | Throughput |
|----------------|---------------------------------|------------|
| **PerRow**     | Small volumes, debugging        | Low        |
| **Batch**      | Moderate volumes (100-10K rows) | Medium     |
| **StagedCopy** | Large volumes (10K+ rows)       | High       |

`StagedCopy` writes each batch as a gzipped CSV file, uploads it with `PUT` and loads it with `COPY INTO … ON_ERROR = ABORT_STATEMENT`,
so a row Snowflake cannot load fails the batch. A retried batch uploads and loads the same file name, so Snowflake's load
metadata skips a file it has already loaded. It inserts only; use `Batch` for upserts.

### Snowflake-Specific Considerations

- **Uppercase Identifiers**: Snowflake uppercases unquoted identifiers. The connector quotes every identifier, so members map to UPPER_SNAKE_CASE columns by default (set `Naming` for another convention).
- **TIMESTAMP_NTZ**: Use `NativeTypeName = "TIMESTAMP_NTZ"` for timezone-naive timestamps.
- **NUMBER Type**: Snowflake uses NUMBER for all numeric types. Specify precision with `NativeTypeName = "NUMBER(18,2)"`.
- **Internal Staging**: StagedCopy uses Snowflake's internal user stage (`~`) by default; set `Stage` for a named stage, which must exist and be writable by the role.

## Troubleshooting

### Connection Issues

- Verify your account identifier matches the full Snowflake account locator
- Check that your warehouse is not suspended (auto-resume may need a moment)
- Verify network access (Snowflake IP allowlisting if configured)

### Permission Issues

- Ensure your user has `USAGE` on the warehouse
- Ensure your user has `CREATE TABLE`, `INSERT`, `SELECT` on the schema
- For StagedCopy, ensure your user can use the internal stage

### Performance Tips

- Use `StagedCopy` for bulk loads over 10,000 rows
- Use `Batch` with appropriate `BatchSize` for moderate volumes
- Reuse one connection string: opening a Snowflake session takes seconds, and Snowflake.Data pools sessions per connection string
- Size the warehouse for the load

## Further Documentation

- [Snowflake Connector Guide](../../docs/connectors/snowflake.md)
- [SQL Connectors: Shared Behaviour](../../docs/connectors/sql-connectors.md)

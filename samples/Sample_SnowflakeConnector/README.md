# Sample_SnowflakeConnector

This sample demonstrates the usage of the Snowflake connector in NPipeline for reading from and writing to Snowflake databases.

## Overview

The Snowflake Connector sample showcases how to use NPipeline's Snowflake connector to perform various database operations including reading data, writing data
with different strategies (PerRow, Batch, StagedCopy), attribute-based and convention-based mapping, upsert (MERGE) operations, and data transformations.

## Features Demonstrated

### 1. Reading from Snowflake

- **SnowflakeSourceNode**: Read data from Snowflake tables and views
- Parameterized queries for security
- Streaming results for efficient memory usage
- Support for custom queries and JOINs

### 2. Writing to Snowflake

- **SnowflakeSinkNode**: Write data to Snowflake tables
- **PerRow Write Strategy**: Write one row at a time (best for small batches)
- **Batch Write Strategy**: Write in batches using multi-row INSERT (best for moderate volumes)
- **StagedCopy Write Strategy**: Bulk load via PUT + COPY INTO (best for large volumes)
- Transaction support for atomicity (PerRow and Batch)

### 3. Mapping Strategies

#### Attribute-Based Mapping

- **SnowflakeTableAttribute**: Specify table name and schema
- **SnowflakeColumnAttribute**: Snowflake-specific features (DbType, NativeTypeName, Size, PrimaryKey, Identity)
- **ColumnAttribute**: Common attribute for simple column name mappings
- **IgnoreColumnAttribute**: Exclude computed properties from mapping

#### Convention-Based Mapping

- Automatic PascalCase to UPPER_SNAKE_CASE mapping
- No attributes required for simple scenarios
- Case-insensitive column matching

### 4. Upsert (MERGE) Operations

- MERGE-based insert-or-update semantics
- Configurable key columns and merge actions
- OnMergeAction: Update, Ignore, or Delete

### 5. Connection Management

- Connection pooling for efficiency
- Snowflake cloud connectivity
- Query tagging for observability
- Password and key-pair authentication support

### 6. Error Handling

- Retry logic for transient errors
- Row-level error handling
- Continue-on-error mode

## Prerequisites

### Snowflake Account

You need access to a Snowflake account with:

- A warehouse with compute credits
- A database and schema for test tables
- A user with appropriate permissions (CREATE TABLE, INSERT, SELECT, DROP TABLE)

Sign up for a [Snowflake Free Trial](https://signup.snowflake.com/) to get started.

### .NET SDK

- .NET 8.0 SDK or later
- [Download .NET SDK](https://dotnet.microsoft.com/download)

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd NPipeline
```

### 2. Restore Dependencies

```bash
dotnet restore
```

### 3. Set Up Connection String

Set the `NPIPELINE_SNOWFLAKE_CONNECTION_STRING` environment variable:

```bash
# macOS / Linux
export NPIPELINE_SNOWFLAKE_CONNECTION_STRING="account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;password=mypassword;db=mydb;schema=PUBLIC;warehouse=COMPUTE_WH"

# Windows (PowerShell)
$env:NPIPELINE_SNOWFLAKE_CONNECTION_STRING = "account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;password=mypassword;db=mydb;schema=PUBLIC;warehouse=COMPUTE_WH"

# Windows (Command Prompt)
set NPIPELINE_SNOWFLAKE_CONNECTION_STRING=account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;password=mypassword;db=mydb;schema=PUBLIC;warehouse=COMPUTE_WH
```

For key-pair authentication:

```bash
export NPIPELINE_SNOWFLAKE_CONNECTION_STRING="account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;authenticator=snowflake_jwt;private_key_file=/path/to/rsa_key.p8;db=mydb;schema=PUBLIC;warehouse=COMPUTE_WH"
```

## How to Run

### Option 1: Using Environment Variable

```bash
dotnet run --project samples/Sample_SnowflakeConnector
```

### Option 2: Using Command Line Argument

```bash
dotnet run --project samples/Sample_SnowflakeConnector "account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;password=mypassword;db=mydb;schema=PUBLIC;warehouse=COMPUTE_WH"
```

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

| Strategy       | Best For                        | Throughput | Transactional |
|----------------|---------------------------------|------------|---------------|
| **PerRow**     | Small batches, debugging        | Low        | Yes           |
| **Batch**      | Moderate volumes (100-10K rows) | Medium     | Yes           |
| **StagedCopy** | Large volumes (10K+ rows)       | High       | No*           |

*StagedCopy uses PUT + COPY INTO which is not wrapped in a transaction. Use PerRow or Batch for ExactlyOnce semantics.

### Snowflake-Specific Considerations

- **Uppercase Identifiers**: Snowflake uppercases unquoted identifiers. The connector quotes all identifiers with double quotes.
- **TIMESTAMP_NTZ**: Use `NativeTypeName = "TIMESTAMP_NTZ"` for timezone-naive timestamps.
- **NUMBER Type**: Snowflake uses NUMBER for all numeric types. Specify precision with `NativeTypeName = "NUMBER(18,2)"`.
- **Internal Staging**: StagedCopy uses Snowflake's internal user stage (`~`) by default for file staging.
- **Query Tagging**: The connector automatically sets `QUERY_TAG` for observability in Snowflake's query history.

## Troubleshooting

### Connection Issues

- Verify your account identifier matches the full Snowflake account locator
- Ensure the host includes `.snowflakecomputing.com`
- Check that your warehouse is not suspended (auto-resume may need a moment)
- Verify network access (Snowflake IP allowlisting if configured)

### Permission Issues

- Ensure your user has `USAGE` on the warehouse
- Ensure your user has `CREATE TABLE`, `INSERT`, `SELECT` on the schema
- For StagedCopy, ensure your user can use the internal stage

### Performance Tips

- Use `StagedCopy` for bulk loads over 10,000 rows
- Use `Batch` with appropriate `BatchSize` for moderate volumes
- Set `StreamResults = true` on source nodes for large result sets
- Increase `FetchSize` (default 10,000) for read-heavy workloads
- Use a properly sized warehouse for compute-intensive operations

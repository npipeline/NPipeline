# Sample: MongoDB Connector

This sample demonstrates the NPipeline MongoDB connector for reading and writing data. It runs entirely locally against **MongoDB 8** via Docker using a
single-node replica set configuration (required for change streams).

## Overview

The sample implements a complete ETL (Extract, Transform, Load) pipeline that:

1. **Reads** pending orders from the `orders` collection using `MongoSourceNode<Order>` with a filter.
2. **Transforms** orders into processed orders with calculated tax and total amounts.
3. **Writes** processed orders using `MongoSinkNode<ProcessedOrder>` with the InsertMany strategy.
4. **Demonstrates BulkWrite** strategy for high-throughput scenarios with `OrderedWrites = false`.
5. **Demonstrates Upsert** strategy for idempotent writes that update existing or insert new documents.

## Key Concepts Demonstrated

### MongoDB Connector Components

| Class                | Role                                                                              |
|----------------------|-----------------------------------------------------------------------------------|
| `MongoSourceNode<T>` | Reads documents from a MongoDB collection using filters, projections, and sorting |
| `MongoSinkNode<T>`   | Writes documents to a MongoDB collection using various write strategies           |
| `MongoConfiguration` | Configuration settings for connection, read/write behavior, and resilience        |
| `MongoWriteStrategy` | Enum controlling write semantics (`InsertMany`, `Upsert`, `BulkWrite`)            |

### Attribute-Based Mapping

`[MongoCollection]` and `[MongoField]` attributes map C# classes to MongoDB collections and fields:

```csharp
[MongoCollection("orders")]
public sealed record Order
{
    [MongoField("_id")]
    public string Id { get; set; } = string.Empty;

    [MongoField("customer")]
    public string Customer { get; set; } = string.Empty;

    [MongoField("amount")]
    public decimal Amount { get; set; }
}
```

### Write Strategies

| Strategy     | Description                                                                                      |
|--------------|--------------------------------------------------------------------------------------------------|
| `InsertMany` | Batch inserts using MongoDB's InsertMany. Fastest for new documents but fails on duplicate keys. |
| `Upsert`     | Uses ReplaceOne with upsert enabled. Updates existing documents or inserts new ones.             |
| `BulkWrite`  | Uses MongoDB's BulkWrite API for maximum flexibility and throughput.                             |

### Replica Set Requirement

MongoDB change streams require a replica set configuration. This sample uses a single-node replica set configured via Docker Compose:

```yaml
services:
  mongo:
    image: mongo:8
    command: ["--replSet", "rs0", "--bind_ip_all"]
```

## Prerequisites

| Requirement                                         | Notes                               |
|-----------------------------------------------------|-------------------------------------|
| [.NET 8 SDK](https://dotnet.microsoft.com/download) | Target framework `net8.0`           |
| [Docker](https://docs.docker.com/get-docker/)       | For MongoDB                         |
| Docker Compose                                      | Usually bundled with Docker Desktop |

## Running the Sample

### 1. Start MongoDB

```bash
cd samples/Sample_MongoDbConnector
docker-compose up -d
```

MongoDB takes **10-15 seconds** to initialize the replica set and seed data. Check readiness with:

```bash
docker-compose logs mongo-init
```

Wait until you see "MongoDB initialization complete!" in the logs.

### 2. Run the Sample

```bash
# From the repository root
dotnet run --project samples/Sample_MongoDbConnector

# Or from inside the sample folder
dotnet run
```

Using a **remote MongoDB instance**:

```bash
dotnet run "mongodb://username:password@host:27017/?authSource=admin"
```

### 3. Stop MongoDB

```bash
docker-compose down
```

## Expected Output

```
=== NPipeline Sample: MongoDB Connector ===

No connection string provided — using local MongoDB defaults:
  Connection: mongodb://localhost:27017/?replicaSet=rs0

...
Step 1: Source Read (MongoSourceNode<Order>)
  Reading pending orders from 'orders' collection...

  [pending     ] Alice Johnson         $  150.00  ID: order-001
  [pending     ] Bob Smith             $   75.50  ID: order-002
  [pending     ] David Brown           $  320.75  ID: order-004

  ✓ Read 3 pending order(s)

Step 2: Transform & Write (InsertMany Strategy)
  Transformed 5 order(s) with tax and total calculations.
  ✓ Wrote 5 processed order(s) in 45 ms

  Sample processed orders:
    Alice Johnson        Amount: $  150.00 Tax: $ 15.00 Total: $  165.00
    ...

Step 3: BulkWrite Strategy (High Throughput)
  Writing 200 orders using BulkWrite strategy (OrderedWrites = false)...
  ✓ BulkWrite completed in 78 ms
  - Items written : 200
  - Throughput    : 2564 items/sec

Step 4: Upsert Strategy (Idempotent Writes)
  Upserting 2 order(s) (will update existing or insert new)...
  ✓ Upsert completed in 23 ms
  - 'order-001' updated with new values
  - 'order-new-001' inserted as new document

Pipeline execution completed successfully!
```

## MongoDB Connection Details

| Property    | Value                                       |
|-------------|---------------------------------------------|
| Connection  | `mongodb://localhost:27017/?replicaSet=rs0` |
| Database    | `shop`                                      |
| Collections | `orders`, `processed_orders`, `bulk_orders` |
| Replica Set | `rs0` (single-node)                         |

## Project Structure

| File                             | Purpose                                                            |
|----------------------------------|--------------------------------------------------------------------|
| `Program.cs`                     | Entry point — wires up the host and runs the pipeline              |
| `MongoDbConnectorPipeline.cs`    | Step-by-step pipeline demonstrations                               |
| `Models.cs`                      | `Order` and `ProcessedOrder` model records with mapping attributes |
| `docker-compose.yml`             | MongoDB 8 replica set service definition                           |
| `init-mongo.js`                  | Initialization script for database, collections, and seed data     |
| `Sample_MongoDbConnector.csproj` | Project file with `ProjectReference` to the connector              |

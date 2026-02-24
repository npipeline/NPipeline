# Sample: Azure Cosmos DB Connector

This sample demonstrates the NPipeline Cosmos DB connector against the NoSQL (SQL API) endpoint of Azure Cosmos DB.
It runs entirely locally against the official **Azure Cosmos DB Linux Emulator** via Docker.

## Overview

The sample implements a complete read/write pipeline that:

1. **Bootstraps** the `NPipelineSample` database and two containers (`Products`, `BulkProducts`)
   using the Cosmos SDK directly.
2. **Batch-writes** 10 seed product documents using `CosmosWriteStrategy.Batch`.
3. **Reads** documents back using `CosmosSourceNode<ProductSummary>` with a SQL projection query.
4. **Upserts** updated products (10 % price increase) using `CosmosWriteStrategy.Upsert`.
5. **Bulk-ingests** 200 products using `CosmosWriteStrategy.Bulk` for high-throughput scenarios.
6. **Demonstrates error tolerance** — attempts `Insert` with duplicate IDs and shows the pipeline
   continues cleanly with `ContinueOnError = true`.

## Key Concepts Demonstrated

### Cosmos DB Connector Components

| Class                 | Role                                                                                         |
|-----------------------|----------------------------------------------------------------------------------------------|
| `CosmosSourceNode<T>` | Runs a SQL query against a container and materialises typed objects                          |
| `CosmosSinkNode<T>`   | Writes objects to a container with a configurable write strategy                             |
| `CosmosConfiguration` | Connection settings, write strategy options, error handling                                  |
| `CosmosWriteStrategy` | Enum controlling write semantics (`Insert`, `Upsert`, `Batch`, `TransactionalBatch`, `Bulk`) |

### Attribute-Based Partition Key Mapping

`[CosmosPartitionKey]` on a model property tells the sink node which value to send as the
partition key, removing the need for a runtime-supplied `partitionKeySelector` delegate:

```csharp
public sealed record Product
{
    public string id { get; set; } = string.Empty;   // Cosmos document ID (convention)

    [CosmosPartitionKey]
    public string Category { get; set; } = string.Empty;  // partition key

    public decimal Price  { get; set; }
    // ...
}
```

### Write Strategies

| Strategy             | Description                                                   |
|----------------------|---------------------------------------------------------------|
| `Insert` / `PerRow`  | Sequential inserts; HTTP 409 Conflict on duplicates           |
| `Upsert`             | Idempotent create-or-replace; safe for re-runs                |
| `Batch`              | Parallel-concurrent batch writes; non-transactional but fast  |
| `TransactionalBatch` | Atomic all-or-nothing within a single partition (≤ 100 items) |
| `Bulk`               | High-throughput bulk executor; best for large data loads      |

### Error Tolerance

Set `ContinueOnError = true` in `CosmosConfiguration` to suppress per-item errors (e.g. HTTP 409
Conflict, throttling) and keep the pipeline running instead of propagating an exception.

### Connecting to the Emulator

The emulator uses a **self-signed TLS certificate**. Set `UseGatewayMode = true` and supply an
`HttpClientFactory` that bypasses certificate validation — the `CosmosConfiguration` supports this
natively:

```csharp
var configuration = new CosmosConfiguration
{
    UseGatewayMode = true,
    HttpClientFactory = () => new HttpClient(new HttpClientHandler
    {
        ServerCertificateCustomValidationCallback =
            HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
    })
};
```

> ⚠ Do **not** use `DangerousAcceptAnyServerCertificateValidator` in production environments.
> For real Azure Cosmos DB accounts, remove both `UseGatewayMode` and `HttpClientFactory`.

## Prerequisites

| Requirement                                          | Notes                               |
|------------------------------------------------------|-------------------------------------|
| [.NET 10 SDK](https://dotnet.microsoft.com/download) | Target framework `net10.0`          |
| [Docker](https://docs.docker.com/get-docker/)        | For the Cosmos DB Emulator          |
| Docker Compose                                       | Usually bundled with Docker Desktop |

## Running the Sample

### 1. Start the Emulator

```bash
cd samples/Sample_CosmosDbConnector
docker-compose up -d
```

The emulator can take **30–60 seconds** to become healthy on first run.
Check readiness with:

```bash
docker-compose ps
```

Wait until the `cosmos-emulator` service shows `healthy`.

### 2. Run the Sample

```bash
# From the repository root
dotnet run --project samples/Sample_CosmosDbConnector

# Or from inside the sample folder
dotnet run
```

Using a **real Azure Cosmos DB account** (or a remote emulator):

```bash
dotnet run "AccountEndpoint=https://<account>.documents.azure.com:443/;AccountKey=<key>;"
```

### 3. Stop the Emulator

```bash
docker-compose down
```

## Expected Output

```
=== NPipeline Sample: Cosmos DB Connector ===

No connection string provided — using Azure Cosmos DB Emulator defaults:
  Endpoint : https://localhost:8081/

...
Step 2: Batch Write  (CosmosWriteStrategy.Batch)
  Writing 10 products using Batch strategy (batch size 5)...
  ✓ Batch write completed in 312 ms

Step 3: Source Read  (CosmosSourceNode<ProductSummary>)
  Query: SELECT c.id, c.Name, c.Category, c.Price, c.Stock FROM c ORDER BY c.Category
  [Books          ] Clean Code                  $  34.99  stock: 500
  ...
  ✓ Read 10 product(s)

Step 4: Upsert Update  (CosmosWriteStrategy.Upsert)
  ✓ Upsert completed — 10 document(s) created-or-replaced

Step 5: Bulk Write  (CosmosWriteStrategy.Bulk)
  ✓ Bulk write completed in 1842 ms  (200 items)

Step 6: Error Tolerance  (ContinueOnError = true)
  ✓ Pipeline continued despite individual item conflicts — no exception was thrown
```

## Emulator Connection Details

| Property             | Value                                                                                      |
|----------------------|--------------------------------------------------------------------------------------------|
| NoSQL endpoint       | `https://localhost:8081/`                                                                  |
| Default account key  | `C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==` |
| Cosmos Data Explorer | `https://localhost:8081/_explorer/index.html` (browser, accept cert warning)               |

## Project Structure

| File                              | Purpose                                                              |
|-----------------------------------|----------------------------------------------------------------------|
| `Program.cs`                      | Entry point — wires up the host and runs the pipeline                |
| `CosmosDbConnectorPipeline.cs`    | Step-by-step pipeline demonstrations                                 |
| `Models.cs`                       | `Product` and `ProductSummary` model records with mapping attributes |
| `docker-compose.yml`              | Cosmos DB Linux Emulator service definition                          |
| `Sample_CosmosDbConnector.csproj` | Project file with `ProjectReference` to the connector                |

using System.Diagnostics;
using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Nodes;
using NPipeline.DataFlow.DataStreams;

namespace Sample_CosmosDbConnector;

/// <summary>
///     Demonstrates the NPipeline Cosmos DB connector for reading and writing data
///     against the NoSQL (SQL API) endpoint of Azure Cosmos DB.
/// </summary>
/// <remarks>
///     The pipeline walks through five representative scenarios in sequence:
///     <list type="number">
///         <item>Bootstrap — create the database and container via the Cosmos SDK.</item>
///         <item>Batch write — seed the catalog using <see cref="CosmosWriteStrategy.Batch" />.</item>
///         <item>Source read — query the container with a SQL projection.</item>
///         <item>Upsert update — apply a price increase and write back idempotently.</item>
///         <item>Bulk write — high-throughput ingestion of a large product set.</item>
///         <item>Error tolerance — Insert with duplicate IDs while continuing on conflict errors.</item>
///     </list>
/// </remarks>
public sealed class CosmosDbConnectorPipeline
{
    private const string DatabaseId = "NPipelineSample";
    private const string CatalogContainerId = "Products";
    private const string BulkContainerId = "BulkProducts";

    private readonly string _connectionString;

    public CosmosDbConnectorPipeline(string connectionString)
    {
        _connectionString = connectionString;
    }

    // -----------------------------------------------------------------------------------------
    // Description
    // -----------------------------------------------------------------------------------------

    public static string GetDescription()
    {
        return """
               NPipeline Cosmos DB Connector Sample
               =====================================

               This sample demonstrates core Cosmos DB connector features using the NoSQL (SQL) API:

                 Step 1 – Bootstrap
                          Creates the NPipelineSample database and Products / BulkProducts containers
                          using the Cosmos SDK directly.  This is a one-time setup step performed
                          outside NPipeline so that all later reads and writes have somewhere to land.

                 Step 2 – Batch Write  (CosmosWriteStrategy.Batch)
                          Seeds 10 Product documents concurrently.  Items are grouped into parallel
                          batches for improved throughput compared to sequential per-row inserts.

                 Step 3 – Source Read  (CosmosSourceNode<ProductSummary>)
                          Executes a SQL query against the Products container and materialises the
                          results as a strongly-typed stream of ProductSummary objects.

                 Step 4 – Upsert Update  (CosmosWriteStrategy.Upsert)
                          Applies a 10 % price increase to every product and writes the updated
                          documents back using Upsert — creating or replacing as needed.

                 Step 5 – Bulk Write  (CosmosWriteStrategy.Bulk)
                          Ingests 200 products into the BulkProducts container using the bulk-executor
                          path — the highest-throughput option for large-scale data loads.

                 Step 6 – Error Tolerance  (ContinueOnError = true)
                          Attempts to Insert products that already exist (HTTP 409 Conflict) and then
                          shows that the pipeline continued despite individual item failures.

               Connection string:
                 Uses the emulator default key.  Start the emulator with:

                   docker-compose up -d

                 Then run:

                   dotnet run
               """;
    }

    // -----------------------------------------------------------------------------------------
    // Main entry point
    // -----------------------------------------------------------------------------------------

    public async Task ConsumeAsync(IServiceProvider _, CancellationToken cancellationToken)
    {
        await BootstrapAsync(cancellationToken);
        await DemonstrateBatchWriteAsync(cancellationToken);
        await DemonstrateSourceReadAsync(cancellationToken);
        await DemonstrateUpsertAsync(cancellationToken);
        await DemonstrateBulkWriteAsync(cancellationToken);
        await DemonstrateErrorToleranceAsync(cancellationToken);
    }

    // -----------------------------------------------------------------------------------------
    // Step 1 – Bootstrap
    // -----------------------------------------------------------------------------------------

    private async Task BootstrapAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 1: Bootstrap — creating database and containers");
        Console.WriteLine("------------------------------------------------------");

        // The emulator uses a self-signed TLS certificate.  HttpClientHandler with
        // ServerCertificateCustomValidationCallback is required for local development.
        var cosmosClientOptions = new CosmosClientOptions
        {
            HttpClientFactory = () => new HttpClient(new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator,
            }),
            ConnectionMode = ConnectionMode.Gateway,
        };

        using var client = new CosmosClient(_connectionString, cosmosClientOptions);

        var dbResponse = await client.CreateDatabaseIfNotExistsAsync(DatabaseId, cancellationToken: cancellationToken);
        Console.WriteLine($"  Database        : {dbResponse.Resource.Id}");

        var catalogResponse = await dbResponse.Database.CreateContainerIfNotExistsAsync(
            new ContainerProperties(CatalogContainerId, "/Category"),
            cancellationToken: cancellationToken);

        Console.WriteLine($"  Container       : {catalogResponse.Resource.Id}");

        var bulkResponse = await dbResponse.Database.CreateContainerIfNotExistsAsync(
            new ContainerProperties(BulkContainerId, "/Category"),
            cancellationToken: cancellationToken);

        Console.WriteLine($"  Container       : {bulkResponse.Resource.Id}");

        Console.WriteLine();
    }

    // -----------------------------------------------------------------------------------------
    // Step 2 – Batch Write
    // -----------------------------------------------------------------------------------------

    private async Task DemonstrateBatchWriteAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 2: Batch Write  (CosmosWriteStrategy.Batch)");
        Console.WriteLine("--------------------------------------------------");

        var products = CreateSeedProducts();

        // CosmosSinkNode requires an HttpClientFactory when talking to the emulator so that
        // the self-signed certificate is accepted.
        var configuration = new CosmosConfiguration
        {
            HttpClientFactory = () => new HttpClient(new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator,
            }),
            UseGatewayMode = true,
            WriteBatchSize = 5,
        };

        var sinkNode = new CosmosSinkNode<Product>(
            _connectionString,
            DatabaseId,
            CatalogContainerId,
            configuration: configuration);

        Console.WriteLine($"  Writing {products.Count} products using Batch strategy (batch size 5)...");

        var sw = Stopwatch.StartNew();
        var dataPipe = new InMemoryDataStream<Product>(products);
        await sinkNode.ConsumeAsync(dataPipe, null!, cancellationToken);
        sw.Stop();

        Console.WriteLine($"  ✓ Batch write completed in {sw.ElapsedMilliseconds} ms");
        Console.WriteLine($"  - Items written : {products.Count}");
        Console.WriteLine($"  - Batches used  : {(int)Math.Ceiling(products.Count / 5.0)}");
        Console.WriteLine();
    }

    // -----------------------------------------------------------------------------------------
    // Step 3 – Source Read
    // -----------------------------------------------------------------------------------------

    private async Task DemonstrateSourceReadAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 3: Source Read  (CosmosSourceNode<ProductSummary>)");
        Console.WriteLine("---------------------------------------------------------");

        const string query = "SELECT c.id, c.Name, c.Category, c.Price, c.Stock FROM c ORDER BY c.Category";

        var configuration = new CosmosConfiguration
        {
            HttpClientFactory = () => new HttpClient(new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator,
            }),
            UseGatewayMode = true,
        };

        var sourceNode = new CosmosSourceNode<ProductSummary>(
            _connectionString,
            DatabaseId,
            CatalogContainerId,
            query,
            configuration: configuration);

        Console.WriteLine($"  Query: {query}");
        Console.WriteLine();

        var pipe = sourceNode.OpenStream(null!, cancellationToken);

        var count = 0;

        await foreach (var summary in pipe.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"  [{summary.Category,-15}] {summary.Name,-25} ${summary.Price,8:F2}  stock: {summary.Stock}");
            count++;
        }

        Console.WriteLine();
        Console.WriteLine($"  ✓ Read {count} product(s)");
        Console.WriteLine();
    }

    // -----------------------------------------------------------------------------------------
    // Step 4 – Upsert Update
    // -----------------------------------------------------------------------------------------

    private async Task DemonstrateUpsertAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 4: Upsert Update  (CosmosWriteStrategy.Upsert)");
        Console.WriteLine("------------------------------------------------------");

        // Read existing products, apply a 10 % price increase, then write back.
        var configuration = new CosmosConfiguration
        {
            HttpClientFactory = () => new HttpClient(new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator,
            }),
            UseGatewayMode = true,
        };

        var sourceNode = new CosmosSourceNode<Product>(
            _connectionString,
            DatabaseId,
            CatalogContainerId,
            "SELECT * FROM c",
            configuration: configuration);

        var pipe = sourceNode.OpenStream(null!, cancellationToken);

        var updated = new List<Product>();

        await foreach (var product in pipe.WithCancellation(cancellationToken))
        {
            updated.Add(product with { Price = Math.Round(product.Price * 1.10m, 2), LastUpdated = DateTime.UtcNow });
        }

        var sinkNode = new CosmosSinkNode<Product>(
            _connectionString,
            DatabaseId,
            CatalogContainerId,
            CosmosWriteStrategy.Upsert,
            configuration: configuration);

        Console.WriteLine($"  Applying 10 % price increase to {updated.Count} product(s) via Upsert...");

        var dataPipe = new InMemoryDataStream<Product>(updated);
        await sinkNode.ConsumeAsync(dataPipe, null!, cancellationToken);

        Console.WriteLine($"  ✓ Upsert completed — {updated.Count} document(s) created-or-replaced");
        Console.WriteLine();
    }

    // -----------------------------------------------------------------------------------------
    // Step 5 – Bulk Write
    // -----------------------------------------------------------------------------------------

    private async Task DemonstrateBulkWriteAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 5: Bulk Write  (CosmosWriteStrategy.Bulk)");
        Console.WriteLine("------------------------------------------------");

        var products = CreateBulkProducts(200);

        var configuration = new CosmosConfiguration
        {
            HttpClientFactory = () => new HttpClient(new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator,
            }),
            UseGatewayMode = true,
            AllowBulkExecution = true,
        };

        var sinkNode = new CosmosSinkNode<Product>(
            _connectionString,
            DatabaseId,
            BulkContainerId,
            CosmosWriteStrategy.Bulk,
            configuration: configuration);

        Console.WriteLine($"  Writing {products.Count} products to '{BulkContainerId}' using Bulk strategy...");

        var sw = Stopwatch.StartNew();
        var dataPipe = new InMemoryDataStream<Product>(products);
        await sinkNode.ConsumeAsync(dataPipe, null!, cancellationToken);
        sw.Stop();

        Console.WriteLine($"  ✓ Bulk write completed in {sw.ElapsedMilliseconds} ms");
        Console.WriteLine($"  - Items written : {products.Count}");
        Console.WriteLine($"  - Throughput    : {products.Count / Math.Max(sw.Elapsed.TotalSeconds, 0.001):F0} items/sec");
        Console.WriteLine();
    }

    // -----------------------------------------------------------------------------------------
    // Step 6 – Error Tolerance
    // -----------------------------------------------------------------------------------------

    private async Task DemonstrateErrorToleranceAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 6: Error Tolerance  (ContinueOnError = true)");
        Console.WriteLine("----------------------------------------------------");

        // Attempt to Insert the same seed products that already exist — this will produce
        // HTTP 409 Conflict for every item.  With ContinueOnError the pipeline keeps going.
        var products = CreateSeedProducts();

        var configuration = new CosmosConfiguration
        {
            HttpClientFactory = () => new HttpClient(new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator,
            }),
            UseGatewayMode = true,
            ContinueOnError = true,
        };

        var sinkNode = new CosmosSinkNode<Product>(
            _connectionString,
            DatabaseId,
            CatalogContainerId,
            CosmosWriteStrategy.Insert, // Insert fails with 409 for duplicates
            configuration: configuration);

        Console.WriteLine($"  Inserting {products.Count} products that already exist (expect 409 Conflict per item)...");

        Exception? caughtException = null;

        try
        {
            var dataPipe = new InMemoryDataStream<Product>(products);
            await sinkNode.ConsumeAsync(dataPipe, null!, cancellationToken);
        }
        catch (Exception ex)
        {
            caughtException = ex;
        }

        if (caughtException is null)
        {
            Console.WriteLine("  ✓ Pipeline continued despite individual item conflicts — no exception was thrown");
            Console.WriteLine("  - ContinueOnError absorbed all 409 Conflict errors silently");
        }
        else
        {
            // This branch should not be reached when ContinueOnError = true
            Console.WriteLine($"  ✗ Unexpected exception: {caughtException.Message}");
        }

        Console.WriteLine();
    }

    // -----------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------

    private static List<Product> CreateSeedProducts()
    {
        return
        [
            new Product
            {
                id = "electronics-001", Category = "Electronics", Name = "Wireless Headphones", Price = 79.99m, Stock = 150, LastUpdated = DateTime.UtcNow,
            },
            new Product
            {
                id = "electronics-002", Category = "Electronics", Name = "USB-C Hub 7-Port", Price = 39.99m, Stock = 300, LastUpdated = DateTime.UtcNow,
            },
            new Product
            {
                id = "electronics-003", Category = "Electronics", Name = "Mechanical Keyboard", Price = 129.99m, Stock = 80, LastUpdated = DateTime.UtcNow,
            },
            new Product { id = "books-001", Category = "Books", Name = "Clean Code", Price = 34.99m, Stock = 500, LastUpdated = DateTime.UtcNow },
            new Product { id = "books-002", Category = "Books", Name = "The Pragmatic Programmer", Price = 39.99m, Stock = 420, LastUpdated = DateTime.UtcNow },
            new Product
            {
                id = "clothing-001", Category = "Clothing", Name = "Merino Wool Sweater", Price = 89.99m, Stock = 200, LastUpdated = DateTime.UtcNow,
            },
            new Product { id = "clothing-002", Category = "Clothing", Name = "Waterproof Jacket", Price = 149.99m, Stock = 75, LastUpdated = DateTime.UtcNow },
            new Product { id = "home-001", Category = "Home", Name = "Pour-Over Coffee Set", Price = 54.99m, Stock = 120, LastUpdated = DateTime.UtcNow },
            new Product { id = "home-002", Category = "Home", Name = "Bamboo Cutting Board", Price = 24.99m, Stock = 350, LastUpdated = DateTime.UtcNow },
            new Product { id = "home-003", Category = "Home", Name = "Cast Iron Skillet", Price = 44.99m, Stock = 180, LastUpdated = DateTime.UtcNow },
        ];
    }

    private static List<Product> CreateBulkProducts(int count)
    {
        var categories = new[] { "Electronics", "Books", "Clothing", "Home", "Sports", "Toys" };
        var rng = new Random(42);
        var products = new List<Product>(count);

        for (var i = 1; i <= count; i++)
        {
            var category = categories[rng.Next(categories.Length)];

            products.Add(new Product
            {
                id = $"bulk-{i:D4}",
                Category = category,
                Name = $"Bulk Product {i:D4}",
                Price = Math.Round((decimal)(rng.NextDouble() * 200 + 5), 2),
                Stock = rng.Next(10, 1000),
                LastUpdated = DateTime.UtcNow,
            });
        }

        return products;
    }
}

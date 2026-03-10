using System.Diagnostics;
using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Nodes;
using NPipeline.DataFlow.DataPipes;

namespace Sample_MongoDbConnector;

/// <summary>
///     Demonstrates the NPipeline MongoDB connector for reading and writing data.
/// </summary>
/// <remarks>
///     The pipeline demonstrates an ETL (Extract, Transform, Load) pattern:
///     <list type="number">
///         <item>Read pending orders from the orders collection using <see cref="MongoSourceNode{T}" />.</item>
///         <item>Transform orders into processed orders with calculated tax and total.</item>
///         <item>Write processed orders to the processed_orders collection using <see cref="MongoSinkNode{T}" />.</item>
///         <item>Demonstrate BulkWrite strategy for high-throughput scenarios.</item>
///         <item>Demonstrate Upsert strategy for idempotent writes.</item>
///     </list>
/// </remarks>
public sealed class MongoDbConnectorPipeline
{
    private const string DatabaseName = "shop";
    private const string OrdersCollection = "orders";
    private const string ProcessedOrdersCollection = "processed_orders";

    private readonly string _connectionString;

    public MongoDbConnectorPipeline(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    // -----------------------------------------------------------------------------------------
    // Description
    // -----------------------------------------------------------------------------------------

    public static string GetDescription()
    {
        return """
               NPipeline MongoDB Connector Sample
               ====================================

               This sample demonstrates core MongoDB connector features:

                 Step 1 – Source Read (MongoSourceNode<Order>)
                          Reads pending orders from the 'orders' collection using a filter
                          and streams them as strongly-typed Order objects.

                 Step 2 – Transform & Write (MongoSinkNode<ProcessedOrder>)
                          Transforms orders into processed orders with calculated tax and total,
                          then writes them using the InsertMany strategy.

                 Step 3 – BulkWrite Strategy
                          Demonstrates high-throughput bulk writing for large datasets
                          with ordered writes disabled for maximum performance.

                 Step 4 – Upsert Strategy
                          Demonstrates idempotent writes that update existing documents
                          or insert new ones based on a key field.

               Connection string:
                 Uses MongoDB running locally via Docker Compose. Start with:

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
        await DemonstrateSourceReadAsync(cancellationToken);
        await DemonstrateTransformAndWriteAsync(cancellationToken);
        await DemonstrateBulkWriteAsync(cancellationToken);
        await DemonstrateUpsertAsync(cancellationToken);
    }

    // -----------------------------------------------------------------------------------------
    // Step 1 – Source Read
    // -----------------------------------------------------------------------------------------

    private async Task DemonstrateSourceReadAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 1: Source Read (MongoSourceNode<Order>)");
        Console.WriteLine("----------------------------------------------");

        var configuration = new MongoConfiguration
        {
            DatabaseName = DatabaseName,
            CollectionName = OrdersCollection,
            StreamResults = true,
            BatchSize = 100,
        };

        // Filter for pending orders only
        var filter = Builders<BsonDocument>.Filter.Eq("status", "pending");

        var sourceNode = new MongoSourceNode<Order>(
            _connectionString,
            configuration,
            filter);

        Console.WriteLine("  Reading pending orders from 'orders' collection...");
        Console.WriteLine();

        var pipe = sourceNode.OpenStream(null!, cancellationToken);

        var count = 0;

        await foreach (var order in pipe.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"  [{order.Status,-12}] {order.Customer,-20} ${order.Amount,8:F2}  ID: {order.Id}");
            count++;
        }

        Console.WriteLine();
        Console.WriteLine($"  ✓ Read {count} pending order(s)");
        Console.WriteLine();
    }

    // -----------------------------------------------------------------------------------------
    // Step 2 – Transform & Write
    // -----------------------------------------------------------------------------------------

    private async Task DemonstrateTransformAndWriteAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 2: Transform & Write (InsertMany Strategy)");
        Console.WriteLine("-------------------------------------------------");

        var sourceConfig = new MongoConfiguration
        {
            DatabaseName = DatabaseName,
            CollectionName = OrdersCollection,
            StreamResults = true,
            BatchSize = 100,
        };

        // Read all orders
        var sourceNode = new MongoSourceNode<Order>(
            _connectionString,
            sourceConfig);

        var pipe = sourceNode.OpenStream(null!, cancellationToken);

        // Transform orders to processed orders
        var processedOrders = new List<ProcessedOrder>();

        await foreach (var order in pipe.WithCancellation(cancellationToken))
        {
            var tax = Math.Round(order.Amount * 0.10m, 2);
            var total = order.Amount + tax;

            processedOrders.Add(new ProcessedOrder
            {
                Id = order.Id,
                Customer = order.Customer,
                Amount = order.Amount,
                Tax = tax,
                Total = total,
                ProcessedAt = DateTime.UtcNow.ToString("O"),
            });
        }

        Console.WriteLine($"  Transformed {processedOrders.Count} order(s) with tax and total calculations.");

        // Write to processed_orders collection
        var sinkConfig = new MongoConfiguration
        {
            DatabaseName = DatabaseName,
            CollectionName = ProcessedOrdersCollection,
            WriteStrategy = MongoWriteStrategy.InsertMany,
            WriteBatchSize = 100,
            OrderedWrites = true,
        };

        var sinkNode = new MongoSinkNode<ProcessedOrder>(
            _connectionString,
            sinkConfig);

        var dataPipe = new InMemoryDataStream<ProcessedOrder>(processedOrders);

        var sw = Stopwatch.StartNew();
        await sinkNode.ConsumeAsync(dataPipe, null!, cancellationToken);
        sw.Stop();

        Console.WriteLine($"  ✓ Wrote {processedOrders.Count} processed order(s) in {sw.ElapsedMilliseconds} ms");
        Console.WriteLine();

        // Display sample output
        Console.WriteLine("  Sample processed orders:");

        foreach (var order in processedOrders.Take(3))
        {
            Console.WriteLine($"    {order.Customer,-20} Amount: ${order.Amount,7:F2} Tax: ${order.Tax,5:F2} Total: ${order.Total,7:F2}");
        }

        Console.WriteLine();
    }

    // -----------------------------------------------------------------------------------------
    // Step 3 – BulkWrite Strategy
    // -----------------------------------------------------------------------------------------

    private async Task DemonstrateBulkWriteAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 3: BulkWrite Strategy (High Throughput)");
        Console.WriteLine("----------------------------------------------");

        // Generate a large batch of orders for demonstration
        var bulkOrders = GenerateBulkOrders(200);

        var sinkConfig = new MongoConfiguration
        {
            DatabaseName = DatabaseName,
            CollectionName = "bulk_orders",
            WriteStrategy = MongoWriteStrategy.BulkWrite,
            WriteBatchSize = 50,
            OrderedWrites = false, // Disable ordered writes for maximum throughput
        };

        var sinkNode = new MongoSinkNode<ProcessedOrder>(
            _connectionString,
            sinkConfig);

        Console.WriteLine($"  Writing {bulkOrders.Count} orders using BulkWrite strategy (OrderedWrites = false)...");

        var sw = Stopwatch.StartNew();
        var dataPipe = new InMemoryDataStream<ProcessedOrder>(bulkOrders);
        await sinkNode.ConsumeAsync(dataPipe, null!, cancellationToken);
        sw.Stop();

        var throughput = bulkOrders.Count / Math.Max(sw.Elapsed.TotalSeconds, 0.001);

        Console.WriteLine($"  ✓ BulkWrite completed in {sw.ElapsedMilliseconds} ms");
        Console.WriteLine($"  - Items written : {bulkOrders.Count}");
        Console.WriteLine($"  - Throughput    : {throughput:F0} items/sec");
        Console.WriteLine();
    }

    // -----------------------------------------------------------------------------------------
    // Step 4 – Upsert Strategy
    // -----------------------------------------------------------------------------------------

    private async Task DemonstrateUpsertAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Step 4: Upsert Strategy (Idempotent Writes)");
        Console.WriteLine("---------------------------------------------");

        // Create some orders that might already exist
        var upsertOrders = new List<ProcessedOrder>
        {
            new()
            {
                Id = "order-001", // This one already exists
                Customer = "Alice Johnson (Updated)",
                Amount = 175.00m,
                Tax = 17.50m,
                Total = 192.50m,
                ProcessedAt = DateTime.UtcNow.ToString("O"),
            },
            new()
            {
                Id = "order-new-001", // This is a new order
                Customer = "New Customer",
                Amount = 99.99m,
                Tax = 10.00m,
                Total = 109.99m,
                ProcessedAt = DateTime.UtcNow.ToString("O"),
            },
        };

        var sinkConfig = new MongoConfiguration
        {
            DatabaseName = DatabaseName,
            CollectionName = ProcessedOrdersCollection,
            WriteStrategy = MongoWriteStrategy.Upsert,
            WriteBatchSize = 100,
            UpsertKeyFields = new[] { "_id" }, // Use the _id field as the upsert key
        };

        var sinkNode = new MongoSinkNode<ProcessedOrder>(
            _connectionString,
            sinkConfig);

        Console.WriteLine($"  Upserting {upsertOrders.Count} order(s) (will update existing or insert new)...");

        var sw = Stopwatch.StartNew();
        var dataPipe = new InMemoryDataStream<ProcessedOrder>(upsertOrders);
        await sinkNode.ConsumeAsync(dataPipe, null!, cancellationToken);
        sw.Stop();

        Console.WriteLine($"  ✓ Upsert completed in {sw.ElapsedMilliseconds} ms");
        Console.WriteLine("  - 'order-001' updated with new values");
        Console.WriteLine("  - 'order-new-001' inserted as new document");
        Console.WriteLine();
    }

    // -----------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------

    private static List<ProcessedOrder> GenerateBulkOrders(int count)
    {
        var customers = new[] { "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry" };
        var rng = new Random(42);
        var orders = new List<ProcessedOrder>(count);

        for (var i = 1; i <= count; i++)
        {
            var amount = Math.Round((decimal)(rng.NextDouble() * 500 + 10), 2);
            var tax = Math.Round(amount * 0.10m, 2);

            orders.Add(new ProcessedOrder
            {
                Id = $"bulk-{i:D4}",
                Customer = customers[rng.Next(customers.Length)],
                Amount = amount,
                Tax = tax,
                Total = amount + tax,
                ProcessedAt = DateTime.UtcNow.ToString("O"),
            });
        }

        return orders;
    }
}

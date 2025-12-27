using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Pipeline;

namespace Sample_IntentDrivenGrouping;

/// <summary>
///     Demonstrates the intent-driven grouping API for clear distinction between
///     batching (operational efficiency) and aggregation (temporal correctness).
/// </summary>
public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== Intent-Driven Grouping API Demo ===\n");

        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((_, services) => { services.AddNPipeline(Assembly.GetExecutingAssembly()); })
            .Build();

        // Scenario 1: Batching for operational efficiency
        Console.WriteLine("Scenario 1: Batching for Operational Efficiency");
        Console.WriteLine("Goal: Reduce database load by batching inserts\n");
        await host.Services.RunPipelineAsync<OperationalEfficiencyPipeline>();

        Console.WriteLine("\n" + new string('-', 60) + "\n");

        // Scenario 2: Aggregation for temporal correctness
        Console.WriteLine("Scenario 2: Aggregation for Temporal Correctness");
        Console.WriteLine("Goal: Calculate sales totals, handling late data\n");
        await host.Services.RunPipelineAsync<TemporalCorrectnessPipeline>();

        Console.WriteLine("\n=== Demo Complete ===");
    }
}

/// <summary>
///     Scenario 1: Use batching for operational efficiency.
///     Perfect for bulk database operations where timing doesn't affect correctness.
/// </summary>
public sealed class OperationalEfficiencyPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Generate 250 orders
        var source = builder.AddSource(ct => GenerateOrders(250), "order-source");

        // Use intent-driven API: ForOperationalEfficiency
        // This clearly signals we're batching for performance, not correctness
        var batcher = builder.GroupItems<Order>()
            .ForOperationalEfficiency(
                100,
                TimeSpan.FromSeconds(2),
                "order-batcher");

        // Simulate bulk database insert
        var sink = builder.AddSink<IReadOnlyCollection<Order>>(
            batch => Console.WriteLine($"  💾 Bulk inserting {batch.Count} orders to database"),
            "bulk-insert");

        builder.Connect(source, batcher);
        builder.Connect<IReadOnlyCollection<Order>>(batcher, sink);
    }

    private static async IAsyncEnumerable<Order> GenerateOrders(int count)
    {
        for (var i = 1; i <= count; i++)
        {
            yield return new Order(
                $"ORD-{i:D5}",
                $"CUST-{i % 50:D3}",
                Random.Shared.Next(10, 1000),
                DateTimeOffset.UtcNow);

            // Simulate natural arrival rate
            await Task.Delay(5);
        }
    }
}

/// <summary>
///     Scenario 2: Use aggregation for temporal correctness.
///     Perfect for time-windowed analytics where late data must be handled correctly.
/// </summary>
public sealed class TemporalCorrectnessPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Generate sales events
        var source = builder.AddSource(ct => GenerateSales(50), "sales-source");

        // Use intent-driven API: ForTemporalCorrectness
        // This clearly signals we need time-based windowing
        var aggregator = builder.GroupItems<Sale>()
            .ForTemporalCorrectness(
                TimeSpan.FromMinutes(5),
                sale => sale.Category,
                () => new CategoryStats(0, 0m),
                (stats, sale) => new CategoryStats(
                    stats.Count + 1,
                    stats.TotalAmount + sale.Amount),
                sale => sale.Timestamp,
                "sales-aggregator");

        // Display aggregated results
        var sink = builder.AddSink<CategoryStats>(
            stats => Console.WriteLine($"  📊 {stats.Count} sales, Total: ${stats.TotalAmount:N2}"),
            "stats-display");

        builder.Connect(source, aggregator);
        builder.Connect<CategoryStats>(aggregator, sink);
    }

    private static async IAsyncEnumerable<Sale> GenerateSales(int count)
    {
        var baseTime = DateTimeOffset.UtcNow;
        var categories = new[] { "Electronics", "Clothing", "Food", "Books" };

        for (var i = 0; i < count; i++)
        {
            var sale = new Sale(
                $"SALE-{i:D5}",
                categories[Random.Shared.Next(categories.Length)],
                Random.Shared.Next(10, 500),
                baseTime);

            yield return sale;

            await Task.Delay(10);
        }
    }
}

/// <summary>
///     Order record for batching scenario.
/// </summary>
public sealed record Order(
    string OrderId,
    string CustomerId,
    decimal Amount,
    DateTimeOffset CreatedAt);

/// <summary>
///     Sale record for temporal correctness scenario.
/// </summary>
public sealed record Sale(
    string SaleId,
    string Category,
    decimal Amount,
    DateTimeOffset Timestamp);

/// <summary>
///     Aggregated statistics per category.
/// </summary>
public sealed record CategoryStats(
    int Count,
    decimal TotalAmount);

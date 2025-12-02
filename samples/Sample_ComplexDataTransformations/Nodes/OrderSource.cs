using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_ComplexDataTransformations.Nodes;

/// <summary>
///     Source node that generates order data for the complex data transformations pipeline.
///     This node creates realistic e-commerce order data with various statuses and amounts.
/// </summary>
public class OrderSource : SourceNode<Order>
{
    /// <summary>
    ///     Generates a collection of orders with realistic e-commerce data.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the generated orders.</returns>
    public override IDataPipe<Order> Execute(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Generating order data...");

        var random = new Random(42); // Fixed seed for reproducible results
        var orders = new List<Order>();
        var baseDate = DateTime.UtcNow.AddDays(-30);

        // Generate 50 orders with realistic data
        for (var i = 1; i <= 50; i++)
        {
            var customerId = random.Next(1, 11); // 10 customers
            var orderDate = baseDate.AddDays(random.Next(0, 30));
            var totalAmount = Math.Round(random.NextDecimal(10, 500), 2);

            var status = random.Next(0, 100) switch
            {
                < 60 => "Completed",
                < 80 => "Processing",
                < 95 => "Shipped",
                _ => "Pending",
            };

            orders.Add(new Order(i, customerId, orderDate, totalAmount, status));
        }

        Console.WriteLine($"Generated {orders.Count} orders");
        return new InMemoryDataPipe<Order>(orders, "OrderSource");
    }
}

/// <summary>
///     Extension methods for Random to generate decimal values
/// </summary>
internal static class RandomExtensions
{
    public static decimal NextDecimal(this Random random, decimal min, decimal max)
    {
        var range = max - min;
        var sample = random.NextDouble();
        var scaled = (decimal)(sample * (double)range);
        return min + scaled;
    }
}

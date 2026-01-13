using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_LineageExtension.Nodes;

/// <summary>
///     Source node that generates order events for the lineage tracking pipeline.
///     This node creates realistic e-commerce order data with various statuses and amounts.
/// </summary>
public class OrderSource : SourceNode<OrderEvent>
{
    private readonly int _orderCount;
    private readonly int? _seed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="OrderSource" /> class.
    /// </summary>
    /// <param name="orderCount">The number of orders to generate. Defaults to 20.</param>
    /// <param name="seed">Optional seed for reproducible random data.</param>
    public OrderSource(int orderCount = 20, int? seed = null)
    {
        _orderCount = orderCount;
        _seed = seed;
    }

    /// <summary>
    ///     Generates a collection of order events with realistic e-commerce data.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the generated order events.</returns>
    public override IDataPipe<OrderEvent> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"[OrderSource] Generating {_orderCount} order events...");

        var random = _seed.HasValue
            ? new Random(_seed.Value)
            : new Random();

        var orders = new List<OrderEvent>();
        var baseDate = DateTime.UtcNow.AddDays(-30);

        // Generate orders with realistic data
        for (var i = 1; i <= _orderCount; i++)
        {
            var customerId = random.Next(1, 11); // 10 customers
            var productId = random.Next(1, 51); // 50 products
            var quantity = random.Next(1, 6); // 1-5 items
            var unitPrice = Math.Round(random.NextDecimal(10, 500), 2);
            var orderDate = baseDate.AddDays(random.Next(0, 30));

            var status = random.Next(0, 100) switch
            {
                < 50 => OrderStatus.Pending,
                < 75 => OrderStatus.Processing,
                < 90 => OrderStatus.Validated,
                < 95 => OrderStatus.Completed,
                _ => OrderStatus.Cancelled,
            };

            var paymentMethod = random.Next(0, 100) switch
            {
                < 60 => PaymentMethod.CreditCard,
                < 80 => PaymentMethod.DebitCard,
                < 90 => PaymentMethod.PayPal,
                < 97 => PaymentMethod.BankTransfer,
                _ => PaymentMethod.CashOnDelivery,
            };

            var isFlaggedForFraud = random.Next(0, 100) < 5; // 5% fraud rate

            var order = new OrderEvent(
                i,
                customerId,
                productId,
                quantity,
                unitPrice,
                orderDate,
                status,
                $"123 Main St, City {random.Next(1, 10)}",
                paymentMethod,
                isFlaggedForFraud);

            orders.Add(order);
        }

        Console.WriteLine($"[OrderSource] Generated {orders.Count} order events");
        return new InMemoryDataPipe<OrderEvent>(orders, "OrderSource");
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

using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_BranchNode.Models;

namespace Sample_BranchNode.Nodes;

/// <summary>
///     Source node that generates simulated e-commerce order events.
///     This node simulates various types of customer orders with realistic timing and distribution.
/// </summary>
public class OrderSource : SourceNode<OrderEvent>
{
    private readonly string[] _customerIds =
    {
        "CUST_PREMIUM_001", "CUST_REGULAR_002", "CUST_NEW_003", "CUST_VIP_004", "CUST_RETURNING_005",
        "CUST_GUEST_006", "CUST_MEMBER_007", "CUST_CORPORATE_008", "CUST_INTERNATIONAL_009", "CUST_LOYAL_010",
    };

    private readonly string[] _paymentMethods =
    {
        "CreditCard", "PayPal", "ApplePay", "GooglePay", "BankTransfer", "Crypto",
    };

    private readonly string[] _productIds =
    {
        "PROD_LAPTOP_001", "PROD_PHONE_002", "PROD_TABLET_003", "PROD_HEADPHONES_004", "PROD_MOUSE_005",
        "PROD_KEYBOARD_006", "PROD_MONITOR_007", "PROD_CAMERA_008", "PROD_SPEAKER_009", "PROD_CHARGER_010",
        "PROD_CASE_011", "PROD_CABLE_012", "PROD_DESK_013", "PROD_CHAIR_014", "PROD_LAMP_015",
    };

    private readonly Random _random = new();

    private readonly string[] _shippingAddresses =
    {
        "123 Main St, New York, NY 10001",
        "456 Oak Ave, Los Angeles, CA 90001",
        "789 Pine Rd, Chicago, IL 60601",
        "321 Elm St, Houston, TX 77001",
        "654 Maple Dr, Phoenix, AZ 85001",
        "987 Cedar Ln, Philadelphia, PA 19101",
        "147 Birch Way, San Antonio, TX 78201",
        "258 Willow Blvd, San Diego, CA 92101",
        "369 Spruce Ct, Dallas, TX 75201",
        "741 Aspen Pl, San Jose, CA 95101",
    };

    private readonly string[] _statuses =
    {
        "Pending", "Confirmed", "Processing", "Shipped", "Delivered", "Cancelled",
    };

    /// <summary>
    ///     Generates a stream of simulated order events with realistic timing and distribution.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token to stop order generation.</param>
    /// <returns>A data pipe containing order events.</returns>
    public override IDataPipe<OrderEvent> Initialize(
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine("OrderSource: Starting to generate e-commerce order events...");
        Console.WriteLine("OrderSource: Simulating real-time customer orders...");

        var orderCount = 0;
        var startTime = DateTime.UtcNow;

        async IAsyncEnumerable<OrderEvent> GenerateOrders([EnumeratorCancellation] CancellationToken ct = default)
        {
            // Generate 10-15 sample orders
            var totalOrders = _random.Next(10, 16);

            while (orderCount < totalOrders && !ct.IsCancellationRequested)
            {
                var orderEvent = GenerateRandomOrder(orderCount++);
                yield return orderEvent;

                // Simulate variable order frequency (1-3 seconds between orders)
                var delayMs = _random.Next(1000, 3000);
                await Task.Delay(delayMs, ct);

                // Log progress
                if (orderCount % 5 == 0)
                {
                    var elapsed = DateTime.UtcNow - startTime;
                    var rate = orderCount / elapsed.TotalSeconds;
                    Console.WriteLine($"OrderSource: Generated {orderCount} orders ({rate:F1} orders/sec)");
                }
            }

            Console.WriteLine($"OrderSource: Completed generating {orderCount} orders");
        }

        return new StreamingDataPipe<OrderEvent>(GenerateOrders(cancellationToken), "OrderSource");
    }

    /// <summary>
    ///     Generates a single random order event with realistic data.
    /// </summary>
    /// <param name="sequenceNumber">The sequence number for the order.</param>
    /// <returns>A randomly generated order event.</returns>
    private OrderEvent GenerateRandomOrder(int sequenceNumber)
    {
        var productId = _productIds[_random.Next(_productIds.Length)];
        var customerId = _customerIds[_random.Next(_customerIds.Length)];
        var status = _statuses[_random.Next(_statuses.Length)];
        var paymentMethod = _paymentMethods[_random.Next(_paymentMethods.Length)];
        var shippingAddress = _shippingAddresses[_random.Next(_shippingAddresses.Length)];

        // Generate realistic quantities (1-5 items per order)
        var quantity = _random.Next(1, 6);

        // Generate realistic prices based on product type
        var price = GeneratePriceForProduct(productId);

        // Generate timestamp from last hour to simulate recent orders
        var timestamp = DateTime.UtcNow.AddMinutes(-_random.Next(0, 60));

        return new OrderEvent
        {
            OrderId = $"ORD_{sequenceNumber:D6}_{DateTime.UtcNow:yyyyMMdd}",
            CustomerId = customerId,
            ProductId = productId,
            Quantity = quantity,
            Price = price,
            Timestamp = timestamp,
            Status = status,
            ShippingAddress = shippingAddress,
            PaymentMethod = paymentMethod,
        };
    }

    /// <summary>
    ///     Generates a realistic price based on the product type.
    /// </summary>
    /// <param name="productId">The product identifier.</param>
    /// <returns>A price appropriate for the product type.</returns>
    private decimal GeneratePriceForProduct(string productId)
    {
        return productId switch
        {
            var id when id.Contains("LAPTOP") => (decimal)(_random.NextDouble() * 1500 + 800), // $800-2300
            var id when id.Contains("PHONE") => (decimal)(_random.NextDouble() * 800 + 400), // $400-1200
            var id when id.Contains("TABLET") => (decimal)(_random.NextDouble() * 600 + 300), // $300-900
            var id when id.Contains("MONITOR") => (decimal)(_random.NextDouble() * 400 + 200), // $200-600
            var id when id.Contains("CAMERA") => (decimal)(_random.NextDouble() * 800 + 200), // $200-1000
            var id when id.Contains("DESK") => (decimal)(_random.NextDouble() * 300 + 150), // $150-450
            var id when id.Contains("CHAIR") => (decimal)(_random.NextDouble() * 400 + 100), // $100-500
            var id when id.Contains("SPEAKER") => (decimal)(_random.NextDouble() * 200 + 50), // $50-250
            var id when id.Contains("HEADPHONES") => (decimal)(_random.NextDouble() * 300 + 50), // $50-350
            var id when id.Contains("KEYBOARD") => (decimal)(_random.NextDouble() * 150 + 30), // $30-180
            var id when id.Contains("MOUSE") => (decimal)(_random.NextDouble() * 80 + 20), // $20-100
            var id when id.Contains("CHARGER") => (decimal)(_random.NextDouble() * 50 + 15), // $15-65
            var id when id.Contains("CASE") => (decimal)(_random.NextDouble() * 40 + 10), // $10-50
            var id when id.Contains("CABLE") => (decimal)(_random.NextDouble() * 30 + 5), // $5-35
            var id when id.Contains("LAMP") => (decimal)(_random.NextDouble() * 100 + 25), // $25-125
            _ => (decimal)(_random.NextDouble() * 100 + 10), // Default: $10-110
        };
    }
}

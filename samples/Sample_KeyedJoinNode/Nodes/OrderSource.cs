using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_KeyedJoinNode.Nodes;

/// <summary>
///     Source node that generates sample order data for demonstrating keyed join functionality.
///     This node creates a stream of orders with varying customer IDs and products.
/// </summary>
public class OrderSource : SourceNode<Order>
{
    private readonly TimeSpan _delayBetweenOrders;
    private readonly ILogger<OrderSource>? _logger;
    private readonly int _maxOrders;

    /// <summary>
    ///     Initializes a new instance of the <see cref="OrderSource" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    /// <param name="delayBetweenOrders">Delay between generating orders to simulate real-time data.</param>
    /// <param name="maxOrders">Maximum number of orders to generate.</param>
    public OrderSource(ILogger<OrderSource>? logger = null, TimeSpan? delayBetweenOrders = null, int maxOrders = 20)
    {
        _logger = logger;
        _delayBetweenOrders = delayBetweenOrders ?? TimeSpan.FromMilliseconds(500);
        _maxOrders = maxOrders;
    }

    /// <inheritdoc />
    public override IDataPipe<Order> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger?.LogInformation("OrderSource: Starting to generate {MaxOrders} orders", _maxOrders);

        var random = new Random(42); // Fixed seed for reproducible results
        var customerIds = new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        var productCodes = new[] { "ELEC-001", "ELEC-002", "CLOTH-001", "CLOTH-002", "BOOK-001", "BOOK-002", "HOME-001", "HOME-002" };

        var productPrices = new Dictionary<string, decimal>
        {
            ["ELEC-001"] = 299.99m,
            ["ELEC-002"] = 599.99m,
            ["CLOTH-001"] = 49.99m,
            ["CLOTH-002"] = 79.99m,
            ["BOOK-001"] = 19.99m,
            ["BOOK-002"] = 29.99m,
            ["HOME-001"] = 149.99m,
            ["HOME-002"] = 199.99m,
        };

        var orders = new List<Order>();

        for (var i = 1; i <= _maxOrders; i++)
        {
            var customerId = customerIds[random.Next(customerIds.Length)];
            var productCode = productCodes[random.Next(productCodes.Length)];
            var quantity = random.Next(1, 5);
            var unitPrice = productPrices[productCode];

            var order = new Order(
                i,
                customerId,
                productCode,
                quantity,
                unitPrice,
                DateTime.UtcNow.AddDays(-random.Next(30))
            );

            _logger?.LogDebug("OrderSource: Generated Order {OrderId} for Customer {CustomerId}, Product {ProductCode}, Qty {Quantity}, Price {TotalPrice:C}",
                order.OrderId, order.CustomerId, order.ProductCode, order.Quantity, order.TotalPrice);

            orders.Add(order);
        }

        _logger?.LogInformation("OrderSource: Finished generating {MaxOrders} orders", _maxOrders);

        return new InMemoryDataPipe<Order>(orders, "OrderSource");
    }
}

using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_LineageExtension.Nodes;

/// <summary>
///     Join node that combines order events with customer data.
///     Demonstrates 1:1 cardinality where each order is enriched with exactly one customer record.
/// </summary>
public class OrderCustomerJoinNode : SourceNode<EnrichedOrder>
{
    private readonly CustomerData[] _customers;
    private readonly OrderEvent[] _orders;

    /// <summary>
    ///     Initializes a new instance of the <see cref="OrderCustomerJoinNode" /> class.
    /// </summary>
    /// <param name="orders">Array of order events to join.</param>
    /// <param name="customers">Array of customer data to join with.</param>
    public OrderCustomerJoinNode(OrderEvent[] orders, CustomerData[] customers)
    {
        _orders = orders ?? throw new ArgumentNullException(nameof(orders));
        _customers = customers ?? throw new ArgumentNullException(nameof(customers));
    }

    /// <summary>
    ///     Joins orders with customer data and returns enriched orders.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing enriched orders.</returns>
    public override IDataPipe<EnrichedOrder> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"[OrderCustomerJoinNode] Joining {_orders.Length} orders with {_customers.Length} customers...");

        var enrichedOrders = new List<EnrichedOrder>();
        var customerLookup = _customers.ToDictionary(c => c.CustomerId);

        foreach (var order in _orders)
        {
            if (customerLookup.TryGetValue(order.CustomerId, out var customer))
            {
                // Calculate discount based on loyalty tier
                var discount = CalculateDiscount(customer.LoyaltyTier, order.TotalAmount);

                // Determine processing priority
                var priority = DeterminePriority(customer, order);

                var enrichedOrder = new EnrichedOrder(
                    order,
                    customer,
                    discount,
                    priority);

                enrichedOrders.Add(enrichedOrder);
            }
            else
                Console.WriteLine($"[OrderCustomerJoinNode] Warning: Customer {order.CustomerId} not found for order {order.OrderId}");
        }

        Console.WriteLine($"[OrderCustomerJoinNode] Created {enrichedOrders.Count} enriched orders");
        return new InMemoryDataPipe<EnrichedOrder>(enrichedOrders, "OrderCustomerJoinNode");
    }

    /// <summary>
    ///     Calculates discount based on customer loyalty tier and order amount.
    /// </summary>
    private static decimal CalculateDiscount(LoyaltyTier tier, decimal orderAmount)
    {
        var discountRate = tier switch
        {
            LoyaltyTier.Bronze => 0m,
            LoyaltyTier.Silver => 0.05m, // 5%
            LoyaltyTier.Gold => 0.10m, // 10%
            LoyaltyTier.Platinum => 0.15m, // 15%
            _ => 0m,
        };

        // Additional discount for large orders
        if (orderAmount > 500m)
            discountRate += 0.02m; // Extra 2% for orders over $500

        return Math.Round(orderAmount * discountRate, 2);
    }

    /// <summary>
    ///     Determines processing priority based on customer tier and order value.
    /// </summary>
    private static ProcessingPriority DeterminePriority(CustomerData customer, OrderEvent order)
    {
        // VIP customers get higher priority
        if (customer.IsVip)
            return ProcessingPriority.High;

        // Large orders get higher priority
        if (order.TotalAmount > 1000m)
            return ProcessingPriority.High;

        // Fraud-flagged orders get lower priority
        if (order.IsFlaggedForFraud)
            return ProcessingPriority.Low;

        // Default priority
        return ProcessingPriority.Normal;
    }
}

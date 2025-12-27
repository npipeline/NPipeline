using System;
using Microsoft.Extensions.Logging;
using NPipeline.Attributes.Nodes;
using NPipeline.Nodes;

namespace Sample_KeyedJoinNode.Nodes;

/// <summary>
///     Join node that combines order data with customer information using CustomerId as the join key.
///     This node demonstrates multi-stream joins in NPipeline using the KeyedJoinNode base class.
///     It supports different join types (Inner, LeftOuter, RightOuter, FullOuter) to handle
///     various scenarios including unmatched orders or customers.
/// </summary>
[KeySelector(typeof(Order), "CustomerId")]
[KeySelector(typeof(Customer), "CustomerId")]
public class OrderCustomerJoinNode : KeyedJoinNode<int, Order, Customer, OrderCustomerJoin>
{
    private readonly ILogger<OrderCustomerJoinNode>? _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="OrderCustomerJoinNode" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    /// <param name="joinType">The type of join to perform (defaults to Inner).</param>
    public OrderCustomerJoinNode(ILogger<OrderCustomerJoinNode>? logger = null, JoinType joinType = JoinType.Inner)
    {
        _logger = logger;
        JoinType = joinType;
    }

    /// <summary>
    ///     Creates the output item by combining order and customer data.
    /// </summary>
    /// <param name="order">The order from the first input stream.</param>
    /// <param name="customer">The customer from the second input stream.</param>
    /// <returns>A combined OrderCustomerJoin record.</returns>
    public override OrderCustomerJoin CreateOutput(Order order, Customer customer)
    {
        _logger?.LogDebug("OrderCustomerJoinNode: Joined Order {OrderId} with Customer {Name} (ID: {CustomerId})",
            order.OrderId, customer.Name, customer.CustomerId);

        Console.WriteLine(
            $"✓ Joined Order {order.OrderId} with Customer {customer.Name} (ID: {customer.CustomerId}, Tier: {customer.CustomerTier}) - Total: {order.TotalPrice:C}");

        return new OrderCustomerJoin(order, customer);
    }

    /// <summary>
    ///     Creates output for unmatched orders (left outer join).
    /// </summary>
    /// <param name="order">The unmatched order.</param>
    /// <returns>An OrderCustomerJoin with null customer.</returns>
    public override OrderCustomerJoin CreateOutputFromLeft(Order order)
    {
        _logger?.LogWarning("OrderCustomerJoinNode: Unmatched Order {OrderId} - no customer found for CustomerId {CustomerId}",
            order.OrderId, order.CustomerId);

        Console.WriteLine($"⚠ Unmatched Order {order.OrderId} - no customer found for CustomerId {order.CustomerId} - Total: {order.TotalPrice:C}");

        // Create a placeholder customer for unmatched orders
        var placeholderCustomer = new Customer(
            order.CustomerId,
            "Unknown Customer",
            "unknown@example.com",
            DateTime.UtcNow,
            "Unknown"
        );

        return new OrderCustomerJoin(order, placeholderCustomer);
    }

    /// <summary>
    ///     Creates output for unmatched customers (right outer join).
    /// </summary>
    /// <param name="customer">The unmatched customer.</param>
    /// <returns>An OrderCustomerJoin with null order.</returns>
    public override OrderCustomerJoin CreateOutputFromRight(Customer customer)
    {
        _logger?.LogInformation("OrderCustomerJoinNode: Unmatched Customer {Name} (ID: {CustomerId}) - no orders found",
            customer.Name, customer.CustomerId);

        Console.WriteLine($"ℹ Unmatched Customer {customer.Name} (ID: {customer.CustomerId}, Tier: {customer.CustomerTier}) - no orders found");

        // Create a placeholder order for unmatched customers
        var placeholderOrder = new Order(
            0, // No OrderId
            customer.CustomerId,
            "UNKNOWN-PRODUCT",
            0,
            0m,
            DateTime.UtcNow
        );

        return new OrderCustomerJoin(placeholderOrder, customer);
    }
}

using NPipeline.Attributes.Nodes;
using NPipeline.Nodes;

namespace Sample_ComplexDataTransformations.Nodes;

/// <summary>
///     Join node that combines order data with customer information using CustomerId as the join key.
///     This node demonstrates multi-stream joins in NPipeline using the KeyedJoinNode base class.
/// </summary>
[KeySelector(typeof(Order), "CustomerId")]
[KeySelector(typeof(Customer), "CustomerId")]
public class OrderCustomerJoinNode : KeyedJoinNode<int, Order, Customer, OrderCustomerJoin>
{
    /// <summary>
    ///     Creates the output item by combining order and customer data.
    /// </summary>
    /// <param name="order">The order from the first input stream.</param>
    /// <param name="customer">The customer from the second input stream.</param>
    /// <returns>A combined OrderCustomerJoin record.</returns>
    public override OrderCustomerJoin CreateOutput(Order order, Customer customer)
    {
        Console.WriteLine($"Joined Order {order.OrderId} with Customer {customer.Name} (ID: {customer.CustomerId})");
        return new OrderCustomerJoin(order, customer);
    }

    /// <summary>
    ///     Creates output for unmatched orders (left outer join).
    /// </summary>
    /// <param name="order">The unmatched order.</param>
    /// <returns>An OrderCustomerJoin with null customer.</returns>
    public override OrderCustomerJoin CreateOutputFromLeft(Order order)
    {
        Console.WriteLine($"Unmatched Order {order.OrderId} - no customer found for CustomerId {order.CustomerId}");
        return new OrderCustomerJoin(order, null!);
    }

    /// <summary>
    ///     Creates output for unmatched customers (right outer join).
    /// </summary>
    /// <param name="customer">The unmatched customer.</param>
    /// <returns>An OrderCustomerJoin with null order.</returns>
    public override OrderCustomerJoin CreateOutputFromRight(Customer customer)
    {
        Console.WriteLine($"Unmatched Customer {customer.Name} (ID: {customer.CustomerId}) - no orders found");
        return new OrderCustomerJoin(null!, customer);
    }
}

using NPipeline.Configuration;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;

namespace Sample_ComplexDataTransformations.Nodes;

/// <summary>
///     Aggregation node that analyzes customer purchase behavior patterns.
///     This node demonstrates complex aggregations for customer analytics using AggregateNode
///     with time-windowed processing for real-time customer insights.
/// </summary>
public class CustomerBehaviorAggregator : AggregateNode<OrderCustomerJoin, int, CustomerPurchaseBehavior>
{
    /// <summary>
    ///     Initializes a new instance of the CustomerBehaviorAggregator.
    /// </summary>
    public CustomerBehaviorAggregator()
        : base(new AggregateNodeConfiguration<OrderCustomerJoin>(
            WindowAssigner.Tumbling(TimeSpan.FromMinutes(10))))
    {
        Console.WriteLine("CustomerBehaviorAggregator initialized with 10-minute tumbling windows");
    }

    /// <summary>
    ///     Extracts the customer key from an order-customer join for grouping.
    /// </summary>
    /// <param name="item">The OrderCustomerJoin item.</param>
    /// <returns>The CustomerId for grouping.</returns>
    public override int GetKey(OrderCustomerJoin item)
    {
        var customerId = item.Customer?.CustomerId ?? 0;
        Console.WriteLine($"Grouping by CustomerId: {customerId} ({item.Customer?.Name})");
        return customerId;
    }

    /// <summary>
    ///     Creates of initial accumulator value for a new customer group.
    /// </summary>
    /// <returns>The initial accumulator with default values.</returns>
    public override CustomerPurchaseBehavior CreateAccumulator()
    {
        return new CustomerPurchaseBehavior(
            0,
            string.Empty,
            string.Empty,
            0,
            0m,
            0m,
            DateTime.MaxValue,
            DateTime.MinValue,
            new List<string>()
        );
    }

    /// <summary>
    ///     Accumulates an order-customer join into customer behavior aggregator.
    /// </summary>
    /// <param name="accumulator">The current customer behavior aggregation.</param>
    /// <param name="item">The OrderCustomerJoin to accumulate.</param>
    /// <returns>The updated customer behavior aggregation.</returns>
    public override CustomerPurchaseBehavior Accumulate(CustomerPurchaseBehavior accumulator, OrderCustomerJoin item)
    {
        var order = item.Order;
        var customer = item.Customer;

        if (customer == null || order == null)
        {
            Console.WriteLine("Skipping accumulation due to missing customer or order data");
            return accumulator;
        }

        var totalOrders = accumulator.TotalOrders + 1;
        var totalSpent = accumulator.TotalSpent + order.TotalAmount;

        var averageOrderValue = totalOrders > 0
            ? totalSpent / totalOrders
            : 0m;

        var firstOrderDate = order.OrderDate < accumulator.FirstOrderDate
            ? order.OrderDate
            : accumulator.FirstOrderDate;

        var lastOrderDate = order.OrderDate > accumulator.LastOrderDate
            ? order.OrderDate
            : accumulator.LastOrderDate;

        // Simulate category preferences based on order amount ranges
        var preferredCategories = new List<string>(accumulator.PreferredCategories);

        var category = order.TotalAmount switch
        {
            < 50 => "Budget",
            < 200 => "Mid-Range",
            _ => "Premium",
        };

        if (!preferredCategories.Contains(category))
            preferredCategories.Add(category);

        Console.WriteLine(
            $"Accumulating customer behavior for {customer.Name}: Orders={totalOrders}, Spent=${totalSpent:F2}, AvgOrder=${averageOrderValue:F2}");

        return new CustomerPurchaseBehavior(
            customer.CustomerId,
            customer.Name,
            customer.Country,
            totalOrders,
            totalSpent,
            averageOrderValue,
            firstOrderDate,
            lastOrderDate,
            preferredCategories
        );
    }
}

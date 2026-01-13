using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_LineageExtension.Nodes;

/// <summary>
///     Transform node that enriches orders with customer data.
///     Calculates discounts based on loyalty tier and determines processing priority.
/// </summary>
public class EnrichmentTransform : TransformNode<OrderEvent, EnrichedOrder>
{
    /// <summary>
    ///     Enriches an order with customer information.
    /// </summary>
    /// <param name="order">The order event to enrich.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An enriched order with customer data and calculated fields.</returns>
    public override Task<EnrichedOrder> ExecuteAsync(OrderEvent order, PipelineContext context, CancellationToken cancellationToken)
    {
        // Generate customer data based on customer ID
        var customer = GenerateCustomerData(order.CustomerId);

        // Calculate discount based on loyalty tier
        var discount = CalculateDiscount(customer.LoyaltyTier, order.TotalAmount);

        // Determine processing priority
        var priority = DeterminePriority(customer, order);

        var enrichedOrder = new EnrichedOrder(
            order,
            customer,
            discount,
            priority);

        return Task.FromResult(enrichedOrder);
    }

    /// <summary>
    ///     Generates customer data based on customer ID.
    ///     In a real scenario, this would look up customer data from a database or cache.
    /// </summary>
    private static CustomerData GenerateCustomerData(int customerId)
    {
        // Use customer ID to generate deterministic customer data
        var random = new Random(customerId);

        var firstNames = new[] { "John", "Jane", "Mike", "Sarah", "David", "Emily", "Robert", "Lisa", "James", "Mary" };
        var lastNames = new[] { "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez" };

        var firstName = firstNames[customerId % firstNames.Length];
        var lastName = lastNames[customerId % lastNames.Length];
        var fullName = $"{firstName} {lastName}";
        var email = $"{firstName.ToLowerInvariant()}.{lastName.ToLowerInvariant()}{customerId}@example.com";
        var phone = $"+1-{random.Next(100, 999)}-{random.Next(100, 999)}-{random.Next(1000, 9999)}";

        // Distribute loyalty tiers based on customer ID
        var tierValue = customerId % 100;

        var loyaltyTier = tierValue switch
        {
            < 40 => LoyaltyTier.Bronze,
            < 70 => LoyaltyTier.Silver,
            < 90 => LoyaltyTier.Gold,
            _ => LoyaltyTier.Platinum,
        };

        // Lifetime value based on tier
        var lifetimeValue = loyaltyTier switch
        {
            LoyaltyTier.Bronze => random.NextDecimal(100, 1000),
            LoyaltyTier.Silver => random.NextDecimal(1000, 5000),
            LoyaltyTier.Gold => random.NextDecimal(5000, 15000),
            LoyaltyTier.Platinum => random.NextDecimal(15000, 50000),
            _ => random.NextDecimal(100, 1000),
        };

        // Order count based on tier and lifetime value
        var orderCount = (int)(lifetimeValue / random.NextDecimal(50, 200));
        orderCount = Math.Max(1, orderCount);

        var registrationDate = DateTime.UtcNow.AddDays(-random.Next(30, 365));

        return new CustomerData(
            customerId,
            fullName,
            email,
            phone,
            loyaltyTier,
            Math.Round(lifetimeValue, 2),
            orderCount,
            registrationDate);
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

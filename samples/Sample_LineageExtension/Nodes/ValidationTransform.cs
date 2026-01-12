using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_LineageExtension.Nodes;

/// <summary>
///     Transform node that validates enriched orders.
///     Performs business rule validation and returns validation results.
/// </summary>
public class ValidationTransform : TransformNode<EnrichedOrder, ValidatedOrder>
{
    private readonly bool _simulateErrors;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ValidationTransform" /> class.
    /// </summary>
    /// <param name="simulateErrors">Whether to simulate validation errors for demonstration purposes.</param>
    public ValidationTransform(bool simulateErrors = false)
    {
        _simulateErrors = simulateErrors;
    }

    /// <summary>
    ///     Validates an enriched order against business rules.
    /// </summary>
    /// <param name="enrichedOrder">The enriched order to validate.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A validated order with validation results.</returns>
    public override Task<ValidatedOrder> ExecuteAsync(EnrichedOrder enrichedOrder, PipelineContext context, CancellationToken cancellationToken)
    {
        var errors = new List<string>();

        // Validate order amount
        if (enrichedOrder.Order.TotalAmount <= 0)
        {
            errors.Add("Order total amount must be greater than zero");
        }

        // Validate quantity
        if (enrichedOrder.Order.Quantity <= 0)
        {
            errors.Add("Order quantity must be greater than zero");
        }

        // Validate unit price
        if (enrichedOrder.Order.UnitPrice <= 0)
        {
            errors.Add("Unit price must be greater than zero");
        }

        // Validate customer email
        if (string.IsNullOrWhiteSpace(enrichedOrder.Customer.Email) || !enrichedOrder.Customer.Email.Contains('@'))
        {
            errors.Add("Customer email is invalid");
        }

        // Validate shipping address
        if (string.IsNullOrWhiteSpace(enrichedOrder.Order.ShippingAddress))
        {
            errors.Add("Shipping address is required");
        }

        // Validate order status
        if (enrichedOrder.Order.Status == OrderStatus.Cancelled)
        {
            errors.Add("Cannot process cancelled orders");
        }

        // Check for fraud flag
        if (enrichedOrder.Order.IsFlaggedForFraud)
        {
            errors.Add("Order is flagged for fraud review");
        }

        // Simulate random errors for demonstration
        if (_simulateErrors && enrichedOrder.Order.OrderId % 5 == 0)
        {
            errors.Add("Simulated validation error for demonstration");
        }

        // Validate payment method restrictions
        if (enrichedOrder.Order.TotalAmount > 10000m && enrichedOrder.Order.PaymentMethod == PaymentMethod.CashOnDelivery)
        {
            errors.Add("Cash on delivery not available for orders over $10,000");
        }

        var isValid = errors.Count == 0;

        var validatedOrder = new ValidatedOrder(
            enrichedOrder: enrichedOrder,
            isValid: isValid,
            validationErrors: errors);

        return Task.FromResult(validatedOrder);
    }
}
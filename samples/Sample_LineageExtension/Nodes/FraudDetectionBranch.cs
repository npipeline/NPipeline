using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_LineageExtension.Nodes;

/// <summary>
///     Branch node that performs fraud detection and routes orders to different processing paths.
///     Demonstrates how lineage is maintained across branches.
/// </summary>
public class FraudDetectionBranch : TransformNode<EnrichedOrder, EnrichedOrder>
{
    private readonly Action<EnrichedOrder>? _onFraudDetected;
    private readonly Action<EnrichedOrder>? _onNormalOrder;

    /// <summary>
    ///     Initializes a new instance of the <see cref="FraudDetectionBranch" /> class.
    /// </summary>
    /// <param name="onFraudDetected">Action to call when fraud is detected.</param>
    /// <param name="onNormalOrder">Action to call for normal orders.</param>
    public FraudDetectionBranch(
        Action<EnrichedOrder>? onFraudDetected = null,
        Action<EnrichedOrder>? onNormalOrder = null)
    {
        _onFraudDetected = onFraudDetected;
        _onNormalOrder = onNormalOrder;
    }

    /// <summary>
    ///     Performs fraud detection and routes orders appropriately.
    /// </summary>
    /// <param name="enrichedOrder">The enriched order to analyze.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The original enriched order (passes through to main pipeline).</returns>
    public override Task<EnrichedOrder> ExecuteAsync(EnrichedOrder enrichedOrder, PipelineContext context, CancellationToken cancellationToken)
    {
        var isFraud = DetectFraud(enrichedOrder);

        if (isFraud)
        {
            Console.WriteLine($"[FraudDetectionBranch] Fraud detected for Order #{enrichedOrder.Order.OrderId}");
            _onFraudDetected?.Invoke(enrichedOrder);
        }
        else
        {
            _onNormalOrder?.Invoke(enrichedOrder);
        }

        // Return the original order unchanged to continue in the main pipeline
        return Task.FromResult(enrichedOrder);
    }

    /// <summary>
    ///     Detects fraud based on various criteria.
    /// </summary>
    private static bool DetectFraud(EnrichedOrder enrichedOrder)
    {
        var order = enrichedOrder.Order;
        var customer = enrichedOrder.Customer;

        // Check if already flagged
        if (order.IsFlaggedForFraud)
        {
            return true;
        }

        // Suspicious: New customer with large order
        if (customer.OrderCount < 2 && order.TotalAmount > 1000m)
        {
            return true;
        }

        // Suspicious: Multiple orders from same customer in short time
        if (customer.OrderCount > 50 && order.TotalAmount < 50m)
        {
            return true;
        }

        // Suspicious: Unusual payment method for high value
        if (order.TotalAmount > 5000m && order.PaymentMethod == PaymentMethod.CashOnDelivery)
        {
            return true;
        }

        // Suspicious: Missing shipping address
        if (string.IsNullOrWhiteSpace(order.ShippingAddress))
        {
            return true;
        }

        return false;
    }
}
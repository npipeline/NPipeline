namespace Sample_BranchNode.Models;

/// <summary>
///     Represents an e-commerce order event that will be processed through the pipeline
/// </summary>
public class OrderEvent
{
    /// <summary>
    ///     Unique identifier for the order
    /// </summary>
    public required string OrderId { get; init; }

    /// <summary>
    ///     Unique identifier for the customer
    /// </summary>
    public required string CustomerId { get; init; }

    /// <summary>
    ///     Unique identifier for the product
    /// </summary>
    public required string ProductId { get; init; }

    /// <summary>
    ///     Quantity of the product ordered
    /// </summary>
    public int Quantity { get; init; }

    /// <summary>
    ///     Unit price of the product
    /// </summary>
    public decimal Price { get; init; }

    /// <summary>
    ///     Total amount for the order line (Quantity * Price)
    /// </summary>
    public decimal TotalAmount => Quantity * Price;

    /// <summary>
    ///     Timestamp when the order was placed
    /// </summary>
    public DateTime Timestamp { get; init; }

    /// <summary>
    ///     Status of the order (Pending, Confirmed, Shipped, Delivered, Cancelled)
    /// </summary>
    public string Status { get; init; } = "Pending";

    /// <summary>
    ///     Shipping address for the order
    /// </summary>
    public string? ShippingAddress { get; init; }

    /// <summary>
    ///     Payment method used for the order
    /// </summary>
    public string? PaymentMethod { get; init; }
}

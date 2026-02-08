namespace Sample_SqsConnector;

/// <summary>
///     Represents an order message received from the SQS queue.
/// </summary>
public class Order
{
    /// <summary>
    ///     Gets or sets the unique order identifier.
    /// </summary>
    public int OrderId { get; set; }

    /// <summary>
    ///     Gets or sets the customer identifier who placed the order.
    /// </summary>
    public int CustomerId { get; set; }

    /// <summary>
    ///     Gets or sets the total amount of the order.
    /// </summary>
    public decimal TotalAmount { get; set; }

    /// <summary>
    ///     Gets or sets the order status.
    /// </summary>
    public string Status { get; set; } = "Pending";

    /// <summary>
    ///     Gets or sets the timestamp when the order was created.
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
///     Represents a processed order ready to be sent to the output queue.
/// </summary>
public class ProcessedOrder
{
    /// <summary>
    ///     Gets or sets the unique order identifier.
    /// </summary>
    public int OrderId { get; set; }

    /// <summary>
    ///     Gets or sets the customer identifier.
    /// </summary>
    public int CustomerId { get; set; }

    /// <summary>
    ///     Gets or sets the total amount of the order.
    /// </summary>
    public decimal TotalAmount { get; set; }

    /// <summary>
    ///     Gets or sets the processing status.
    /// </summary>
    public string Status { get; set; } = "Processed";

    /// <summary>
    ///     Gets or sets the timestamp when the order was processed.
    /// </summary>
    public DateTime ProcessedAt { get; set; } = DateTime.UtcNow;

    /// <summary>
    ///     Gets or sets additional processing metadata.
    /// </summary>
    public string? ProcessingNotes { get; set; }
}

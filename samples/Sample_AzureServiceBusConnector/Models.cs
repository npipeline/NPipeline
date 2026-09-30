namespace Sample_AzureServiceBusConnector;

/// <summary>An order received from the input queue as JSON.</summary>
public sealed record Order(int OrderId, int CustomerId, decimal TotalAmount, string Status, DateTime CreatedAt);

/// <summary>The result of processing an order, sent to the output queue.</summary>
public sealed record ProcessedOrder(int OrderId, int CustomerId, decimal TotalAmount, string Status, DateTime ProcessedAt, string? ProcessingNotes);

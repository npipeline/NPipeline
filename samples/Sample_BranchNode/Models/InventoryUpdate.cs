namespace Sample_BranchNode.Models;

/// <summary>
///     Represents an inventory update event generated from order processing
/// </summary>
public class InventoryUpdate
{
    /// <summary>
    ///     Unique identifier for the product
    /// </summary>
    public required string ProductId { get; init; }

    /// <summary>
    ///     Change in quantity (negative for orders, positive for returns/restocks)
    /// </summary>
    public int QuantityChange { get; init; }

    /// <summary>
    ///     Current inventory level after the update
    /// </summary>
    public int CurrentInventory { get; init; }

    /// <summary>
    ///     Warehouse or store location identifier
    /// </summary>
    public string? LocationId { get; init; }

    /// <summary>
    ///     Reason for the inventory change
    /// </summary>
    public string? Reason { get; init; }

    /// <summary>
    ///     Timestamp when the inventory update was generated
    /// </summary>
    public DateTime Timestamp { get; init; }

    /// <summary>
    ///     Reference to the original order that triggered this update
    /// </summary>
    public string? OrderId { get; init; }

    /// <summary>
    ///     Priority level for processing this update
    /// </summary>
    public int Priority { get; init; } = 1;
}

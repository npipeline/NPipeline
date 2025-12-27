using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_BranchNode.Models;

namespace Sample_BranchNode.Nodes;

/// <summary>
///     Transform node that processes order events and generates inventory updates.
///     This node demonstrates inventory management logic for e-commerce order processing.
/// </summary>
public class InventoryProcessor : TransformNode<OrderEvent, InventoryUpdate>
{
    private readonly Dictionary<string, int> _currentInventory = new();
    private readonly Dictionary<string, string> _productLocations = new();
    private int _totalUpdatesGenerated;

    /// <summary>
    ///     Initializes a new instance of the InventoryProcessor.
    /// </summary>
    public InventoryProcessor()
    {
        // Initialize inventory levels for demo products
        InitializeInventory();

        Console.WriteLine("InventoryProcessor: Initialized with inventory tracking");
        Console.WriteLine("InventoryProcessor: Tracking inventory levels and location assignments");
    }

    /// <summary>
    ///     Processes a single order event and generates an inventory update.
    /// </summary>
    /// <param name="orderEvent">The order event to process.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An inventory update event.</returns>
    public override async Task<InventoryUpdate> ExecuteAsync(
        OrderEvent orderEvent,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        await Task.CompletedTask; // Simulate async processing

        _totalUpdatesGenerated++;

        // Get current inventory for the product
        var productId = orderEvent.ProductId;
        var currentInventory = GetCurrentInventory(productId);

        // Calculate new inventory level after order
        var newInventory = currentInventory - orderEvent.Quantity;

        // Update inventory tracking
        _currentInventory[productId] = newInventory;

        // Determine priority based on inventory level
        var priority = CalculatePriority(newInventory);

        // Get location for the product
        var locationId = GetProductLocation(productId);

        // Determine reason for inventory change
        var reason = DetermineInventoryReason(orderEvent, newInventory);

        var inventoryUpdate = new InventoryUpdate
        {
            ProductId = productId,
            QuantityChange = -orderEvent.Quantity, // Negative for orders
            CurrentInventory = newInventory,
            LocationId = locationId,
            Reason = reason,
            Timestamp = DateTime.UtcNow,
            OrderId = orderEvent.OrderId,
            Priority = priority,
        };

        // Log the inventory update
        await LogInventoryUpdate(orderEvent, inventoryUpdate);

        return inventoryUpdate;
    }

    /// <summary>
    ///     Initializes the inventory system with sample data.
    /// </summary>
    private void InitializeInventory()
    {
        var sampleProducts = new[]
        {
            "PROD_LAPTOP_001", "PROD_PHONE_002", "PROD_TABLET_003", "PROD_HEADPHONES_004", "PROD_MOUSE_005",
            "PROD_KEYBOARD_006", "PROD_MONITOR_007", "PROD_CAMERA_008", "PROD_SPEAKER_009", "PROD_CHARGER_010",
            "PROD_CASE_011", "PROD_CABLE_012", "PROD_DESK_013", "PROD_CHAIR_014", "PROD_LAMP_015",
        };

        var random = new Random();

        foreach (var productId in sampleProducts)
        {
            // Initialize with random inventory levels (50-200 units)
            _currentInventory[productId] = random.Next(50, 201);

            // Assign warehouse locations based on product type
            _productLocations[productId] = AssignWarehouseLocation(productId);
        }
    }

    /// <summary>
    ///     Gets the current inventory level for a product.
    /// </summary>
    /// <param name="productId">The product identifier.</param>
    /// <returns>The current inventory level.</returns>
    private int GetCurrentInventory(string productId)
    {
        return _currentInventory.TryGetValue(productId, out var inventory)
            ? inventory
            : 100;
    }

    /// <summary>
    ///     Calculates processing priority based on inventory level.
    /// </summary>
    /// <param name="inventoryLevel">The current inventory level.</param>
    /// <returns>Priority level (1=low, 2=medium, 3=high, 4=critical).</returns>
    private int CalculatePriority(int inventoryLevel)
    {
        return inventoryLevel switch
        {
            < 10 => 4, // Critical - very low stock
            < 25 => 3, // High - low stock
            < 50 => 2, // Medium - moderate stock
            _ => 1, // Low - good stock
        };
    }

    /// <summary>
    ///     Gets the warehouse location for a product.
    /// </summary>
    /// <param name="productId">The product identifier.</param>
    /// <returns>The warehouse location identifier.</returns>
    private string GetProductLocation(string productId)
    {
        return _productLocations.TryGetValue(productId, out var location)
            ? location
            : "WAREHOUSE_MAIN";
    }

    /// <summary>
    ///     Assigns warehouse location based on product type.
    /// </summary>
    /// <param name="productId">The product identifier.</param>
    /// <returns>The warehouse location identifier.</returns>
    private string AssignWarehouseLocation(string productId)
    {
        return productId switch
        {
            var id when id.Contains("LAPTOP") || id.Contains("MONITOR") || id.Contains("DESK") || id.Contains("CHAIR") => "WAREHOUSE_LARGE_ITEMS",
            var id when id.Contains("PHONE") || id.Contains("TABLET") || id.Contains("CAMERA") => "WAREHOUSE_ELECTRONICS",
            var id when id.Contains("HEADPHONES") || id.Contains("SPEAKER") || id.Contains("MOUSE") || id.Contains("KEYBOARD") => "WAREHOUSE_ACCESSORIES",
            var id when id.Contains("CHARGER") || id.Contains("CABLE") || id.Contains("CASE") => "WAREHOUSE_SMALL_ITEMS",
            var id when id.Contains("LAMP") => "WAREHOUSE_HOME_GOODS",
            _ => "WAREHOUSE_MAIN",
        };
    }

    /// <summary>
    ///     Determines the reason for the inventory update.
    /// </summary>
    /// <param name="orderEvent">The order event that triggered the update.</param>
    /// <param name="newInventory">The new inventory level.</param>
    /// <returns>A descriptive reason for the inventory change.</returns>
    private string DetermineInventoryReason(OrderEvent orderEvent, int newInventory)
    {
        var baseReason = $"Order {orderEvent.OrderId} - {orderEvent.Quantity} units";

        if (newInventory < 10)
            return $"{baseReason} - CRITICAL STOCK LEVEL";

        if (newInventory < 25)
            return $"{baseReason} - LOW STOCK ALERT";

        if (newInventory < 50)
            return $"{baseReason} - REORDER SOON";

        return baseReason;
    }

    /// <summary>
    ///     Logs the inventory update for monitoring purposes.
    /// </summary>
    /// <param name="orderEvent">The original order event.</param>
    /// <param name="inventoryUpdate">The generated inventory update.</param>
    /// <returns>A task representing the logging operation.</returns>
    private async Task LogInventoryUpdate(OrderEvent orderEvent, InventoryUpdate inventoryUpdate)
    {
        await Task.CompletedTask; // Simulate async logging

        var priorityText = inventoryUpdate.Priority switch
        {
            4 => "CRITICAL",
            3 => "HIGH",
            2 => "MEDIUM",
            _ => "LOW",
        };

        Console.WriteLine(
            $"InventoryProcessor: Order {orderEvent.OrderId} - " +
            $"Product {orderEvent.ProductId} - " +
            $"Qty: {orderEvent.Quantity} - " +
            $"Inventory: {inventoryUpdate.CurrentInventory} - " +
            $"Priority: {priorityText} - " +
            $"Location: {inventoryUpdate.LocationId}");
    }

    /// <summary>
    ///     Gets the current inventory statistics.
    /// </summary>
    /// <returns>A tuple containing total updates and current inventory levels.</returns>
    public (int TotalUpdates, Dictionary<string, int> CurrentInventory) GetStatistics()
    {
        Console.WriteLine($"InventoryProcessor: Total updates generated: {_totalUpdatesGenerated}");
        return (_totalUpdatesGenerated, new Dictionary<string, int>(_currentInventory));
    }
}

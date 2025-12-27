using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_BranchNode.Models;

namespace Sample_BranchNode.Nodes;

/// <summary>
///     Transform node that processes order events and generates analytics events.
///     This node demonstrates business intelligence and analytics data processing for e-commerce.
/// </summary>
public class AnalyticsProcessor : TransformNode<OrderEvent, AnalyticsEvent>
{
    private readonly Dictionary<string, string> _customerRegions = new();
    private readonly Dictionary<string, string> _customerSegments = new();
    private readonly Dictionary<string, string> _productCategories = new();
    private int _totalAnalyticsEventsGenerated;

    /// <summary>
    ///     Initializes a new instance of the AnalyticsProcessor.
    /// </summary>
    public AnalyticsProcessor()
    {
        // Initialize product categories
        InitializeProductCategories();

        // Initialize customer segments and regions
        InitializeCustomerData();

        Console.WriteLine("AnalyticsProcessor: Initialized with analytics tracking");
        Console.WriteLine("AnalyticsProcessor: Categorizing products and segmenting customers for BI");
    }

    /// <summary>
    ///     Processes a single order event and generates an analytics event.
    /// </summary>
    /// <param name="orderEvent">The order event to process.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An analytics event.</returns>
    public override async Task<AnalyticsEvent> ExecuteAsync(
        OrderEvent orderEvent,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        await Task.CompletedTask; // Simulate async processing

        _totalAnalyticsEventsGenerated++;

        // Get product category
        var productCategory = GetProductCategory(orderEvent.ProductId);

        // Get customer segment
        var customerSegment = GetCustomerSegment(orderEvent.CustomerId);

        // Get customer region
        var region = GetCustomerRegion(orderEvent.CustomerId);

        // Determine event type based on order status
        var eventType = DetermineEventType(orderEvent.Status);

        // Generate session ID for tracking
        var sessionId = GenerateSessionId(orderEvent.CustomerId);

        // Create metadata for analytics
        var metadata = GenerateAnalyticsMetadata(orderEvent);

        var analyticsEvent = new AnalyticsEvent
        {
            OrderId = orderEvent.OrderId,
            CustomerId = orderEvent.CustomerId,
            ProductId = orderEvent.ProductId,
            Price = orderEvent.Price,
            EventType = eventType,
            ProductCategory = productCategory,
            CustomerSegment = customerSegment,
            Region = region,
            Timestamp = DateTime.UtcNow,
            Metadata = metadata,
            SessionId = sessionId,
        };

        // Log the analytics event
        await LogAnalyticsEvent(orderEvent, analyticsEvent);

        return analyticsEvent;
    }

    /// <summary>
    ///     Initializes product categories for analytics.
    /// </summary>
    private void InitializeProductCategories()
    {
        _productCategories["PROD_LAPTOP_001"] = "Computers";
        _productCategories["PROD_PHONE_002"] = "Mobile Devices";
        _productCategories["PROD_TABLET_003"] = "Mobile Devices";
        _productCategories["PROD_HEADPHONES_004"] = "Audio";
        _productCategories["PROD_MOUSE_005"] = "Accessories";
        _productCategories["PROD_KEYBOARD_006"] = "Accessories";
        _productCategories["PROD_MONITOR_007"] = "Displays";
        _productCategories["PROD_CAMERA_008"] = "Photography";
        _productCategories["PROD_SPEAKER_009"] = "Audio";
        _productCategories["PROD_CHARGER_010"] = "Accessories";
        _productCategories["PROD_CASE_011"] = "Accessories";
        _productCategories["PROD_CABLE_012"] = "Accessories";
        _productCategories["PROD_DESK_013"] = "Furniture";
        _productCategories["PROD_CHAIR_014"] = "Furniture";
        _productCategories["PROD_LAMP_015"] = "Home & Office";
    }

    /// <summary>
    ///     Initializes customer segments and regions for analytics.
    /// </summary>
    private void InitializeCustomerData()
    {
        // Customer segments
        _customerSegments["CUST_PREMIUM_001"] = "Premium";
        _customerSegments["CUST_REGULAR_002"] = "Regular";
        _customerSegments["CUST_NEW_003"] = "New";
        _customerSegments["CUST_VIP_004"] = "VIP";
        _customerSegments["CUST_RETURNING_005"] = "Returning";
        _customerSegments["CUST_GUEST_006"] = "Guest";
        _customerSegments["CUST_MEMBER_007"] = "Member";
        _customerSegments["CUST_CORPORATE_008"] = "Corporate";
        _customerSegments["CUST_INTERNATIONAL_009"] = "International";
        _customerSegments["CUST_LOYAL_010"] = "Loyal";

        // Customer regions (based on address patterns)
        _customerRegions["CUST_PREMIUM_001"] = "Northeast";
        _customerRegions["CUST_REGULAR_002"] = "West";
        _customerRegions["CUST_NEW_003"] = "Midwest";
        _customerRegions["CUST_VIP_004"] = "Northeast";
        _customerRegions["CUST_RETURNING_005"] = "South";
        _customerRegions["CUST_GUEST_006"] = "West";
        _customerRegions["CUST_MEMBER_007"] = "Midwest";
        _customerRegions["CUST_CORPORATE_008"] = "Northeast";
        _customerRegions["CUST_INTERNATIONAL_009"] = "International";
        _customerRegions["CUST_LOYAL_010"] = "South";
    }

    /// <summary>
    ///     Gets the product category for analytics.
    /// </summary>
    /// <param name="productId">The product identifier.</param>
    /// <returns>The product category.</returns>
    private string GetProductCategory(string productId)
    {
        return _productCategories.TryGetValue(productId, out var category)
            ? category
            : "Other";
    }

    /// <summary>
    ///     Gets the customer segment for analytics.
    /// </summary>
    /// <param name="customerId">The customer identifier.</param>
    /// <returns>The customer segment.</returns>
    private string GetCustomerSegment(string customerId)
    {
        return _customerSegments.TryGetValue(customerId, out var segment)
            ? segment
            : "Unknown";
    }

    /// <summary>
    ///     Gets the customer region for analytics.
    /// </summary>
    /// <param name="customerId">The customer identifier.</param>
    /// <returns>The customer region.</returns>
    private string GetCustomerRegion(string customerId)
    {
        return _customerRegions.TryGetValue(customerId, out var region)
            ? region
            : "Unknown";
    }

    /// <summary>
    ///     Determines the analytics event type based on order status.
    /// </summary>
    /// <param name="orderStatus">The order status.</param>
    /// <returns>The analytics event type.</returns>
    private string DetermineEventType(string orderStatus)
    {
        return orderStatus switch
        {
            "Pending" => "Purchase_Initiated",
            "Confirmed" => "Purchase_Confirmed",
            "Processing" => "Order_Processing",
            "Shipped" => "Order_Shipped",
            "Delivered" => "Purchase_Completed",
            "Cancelled" => "Purchase_Cancelled",
            _ => "Order_Update",
        };
    }

    /// <summary>
    ///     Generates a session ID for analytics tracking.
    /// </summary>
    /// <param name="customerId">The customer identifier.</param>
    /// <returns>A session ID.</returns>
    private string GenerateSessionId(string customerId)
    {
        var date = DateTime.UtcNow.ToString("yyyyMMdd");
        var hash = customerId.GetHashCode();
        return $"session_{date}_{Math.Abs(hash):x8}";
    }

    /// <summary>
    ///     Generates metadata for analytics processing.
    /// </summary>
    /// <param name="orderEvent">The order event.</param>
    /// <returns>A dictionary of metadata.</returns>
    private Dictionary<string, object> GenerateAnalyticsMetadata(OrderEvent orderEvent)
    {
        var metadata = new Dictionary<string, object>
        {
            ["order_total"] = orderEvent.TotalAmount,
            ["quantity"] = orderEvent.Quantity,
            ["payment_method"] = orderEvent.PaymentMethod ?? "Unknown",
            ["shipping_address"] = orderEvent.ShippingAddress ?? "Unknown",
            ["order_timestamp"] = orderEvent.Timestamp,
            ["processing_timestamp"] = DateTime.UtcNow,
            ["day_of_week"] = orderEvent.Timestamp.DayOfWeek.ToString(),
            ["hour_of_day"] = orderEvent.Timestamp.Hour,
            ["month"] = orderEvent.Timestamp.ToString("MMMM"),
        };

        // Add price range category
        var priceRange = orderEvent.Price switch
        {
            < 50 => "Budget",
            < 200 => "Mid-Range",
            < 500 => "Premium",
            < 1000 => "High-End",
            _ => "Luxury",
        };

        metadata["price_range"] = priceRange;

        // Add order size category
        var orderSize = orderEvent.Quantity switch
        {
            1 => "Single",
            2 => "Double",
            3 => "Triple",
            _ => "Bulk",
        };

        metadata["order_size"] = orderSize;

        return metadata;
    }

    /// <summary>
    ///     Logs the analytics event for monitoring purposes.
    /// </summary>
    /// <param name="orderEvent">The original order event.</param>
    /// <param name="analyticsEvent">The generated analytics event.</param>
    /// <returns>A task representing the logging operation.</returns>
    private async Task LogAnalyticsEvent(OrderEvent orderEvent, AnalyticsEvent analyticsEvent)
    {
        await Task.CompletedTask; // Simulate async logging

        Console.WriteLine(
            $"AnalyticsProcessor: Order {orderEvent.OrderId} - " +
            $"Event: {analyticsEvent.EventType} - " +
            $"Product: {analyticsEvent.ProductCategory} - " +
            $"Customer: {analyticsEvent.CustomerSegment} - " +
            $"Region: {analyticsEvent.Region} - " +
            $"Price: ${orderEvent.Price:F2}");
    }

    /// <summary>
    ///     Gets the current analytics statistics.
    /// </summary>
    /// <returns>The total number of analytics events generated.</returns>
    public int GetStatistics()
    {
        Console.WriteLine($"AnalyticsProcessor: Total analytics events generated: {_totalAnalyticsEventsGenerated}");
        return _totalAnalyticsEventsGenerated;
    }
}

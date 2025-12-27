using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_BranchNode.Models;

namespace Sample_BranchNode.Nodes;

/// <summary>
///     Transform node that processes order events and generates notification events.
///     This node demonstrates customer communication patterns for e-commerce order processing.
/// </summary>
public class NotificationProcessor : TransformNode<OrderEvent, NotificationEvent>
{
    private readonly Dictionary<string, string> _customerDevices = new();
    private readonly Dictionary<string, string> _customerEmails = new();
    private readonly Dictionary<string, string> _customerPhones = new();
    private int _totalNotificationsGenerated;

    /// <summary>
    ///     Initializes a new instance of NotificationProcessor.
    /// </summary>
    public NotificationProcessor()
    {
        // Initialize customer contact information
        InitializeCustomerContacts();

        Console.WriteLine("NotificationProcessor: Initialized with notification tracking");
        Console.WriteLine("NotificationProcessor: Managing customer communications across multiple channels");
    }

    /// <summary>
    ///     Processes a single order event and generates a notification event.
    /// </summary>
    /// <param name="orderEvent">The order event to process.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A notification event.</returns>
    public override async Task<NotificationEvent> ExecuteAsync(
        OrderEvent orderEvent,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        await Task.CompletedTask; // Simulate async processing

        _totalNotificationsGenerated++;

        // Determine notification type based on order status and customer preference
        var notificationType = DetermineNotificationType(orderEvent);

        // Get recipient address based on notification type
        var recipientAddress = GetRecipientAddress(orderEvent.CustomerId, notificationType);

        // Generate appropriate message and subject
        var (message, subject) = GenerateNotificationContent(orderEvent, notificationType);

        // Determine priority based on order status
        var priority = DetermineNotificationPriority(orderEvent.Status);

        // Determine if notification requires confirmation
        var requiresConfirmation = DetermineConfirmationRequirement(orderEvent.Status, notificationType);

        // Get template name for this notification
        var templateName = GetTemplateName(orderEvent.Status, notificationType);

        // Generate template data
        var templateData = GenerateTemplateData(orderEvent);

        // Schedule notification if needed
        var scheduledTime = DetermineScheduledTime(orderEvent.Status, orderEvent.Timestamp);

        var notificationEvent = new NotificationEvent
        {
            OrderId = orderEvent.OrderId,
            CustomerId = orderEvent.CustomerId,
            Message = message,
            NotificationType = notificationType,
            Subject = subject,
            RecipientAddress = recipientAddress,
            Priority = priority,
            Timestamp = DateTime.UtcNow,
            ScheduledTime = scheduledTime,
            TemplateName = templateName,
            TemplateData = templateData,
            RequiresConfirmation = requiresConfirmation,
        };

        // Log the notification event
        await LogNotificationEvent(orderEvent, notificationEvent);

        return notificationEvent;
    }

    /// <summary>
    ///     Initializes customer contact information for different notification channels.
    /// </summary>
    private void InitializeCustomerContacts()
    {
        // Email addresses
        _customerEmails["CUST_PREMIUM_001"] = "premium@example.com";
        _customerEmails["CUST_REGULAR_002"] = "regular@example.com";
        _customerEmails["CUST_NEW_003"] = "new@example.com";
        _customerEmails["CUST_VIP_004"] = "vip@example.com";
        _customerEmails["CUST_RETURNING_005"] = "returning@example.com";
        _customerEmails["CUST_GUEST_006"] = "guest@example.com";
        _customerEmails["CUST_MEMBER_007"] = "member@example.com";
        _customerEmails["CUST_CORPORATE_008"] = "corporate@example.com";
        _customerEmails["CUST_INTERNATIONAL_009"] = "international@example.com";
        _customerEmails["CUST_LOYAL_010"] = "loyal@example.com";

        // Phone numbers
        _customerPhones["CUST_PREMIUM_001"] = "+1-555-0101";
        _customerPhones["CUST_REGULAR_002"] = "+1-555-0202";
        _customerPhones["CUST_NEW_003"] = "+1-555-0303";
        _customerPhones["CUST_VIP_004"] = "+1-555-0404";
        _customerPhones["CUST_RETURNING_005"] = "+1-555-0505";
        _customerPhones["CUST_GUEST_006"] = "+1-555-0606";
        _customerPhones["CUST_MEMBER_007"] = "+1-555-0707";
        _customerPhones["CUST_CORPORATE_008"] = "+1-555-0808";
        _customerPhones["CUST_INTERNATIONAL_009"] = "+44-20-1234-5678";
        _customerPhones["CUST_LOYAL_010"] = "+1-555-1010";

        // Device tokens for push notifications
        _customerDevices["CUST_PREMIUM_001"] = "device_token_premium_001";
        _customerDevices["CUST_VIP_004"] = "device_token_vip_004";
        _customerDevices["CUST_MEMBER_007"] = "device_token_member_007";
        _customerDevices["CUST_LOYAL_010"] = "device_token_loyal_010";
    }

    /// <summary>
    ///     Determines the appropriate notification type based on order status and customer segment.
    /// </summary>
    /// <param name="orderEvent">The order event.</param>
    /// <returns>The notification type.</returns>
    private string DetermineNotificationType(OrderEvent orderEvent)
    {
        // High-value customers get email + push notifications
        if (orderEvent.CustomerId.Contains("PREMIUM") || orderEvent.CustomerId.Contains("VIP"))
        {
            return orderEvent.Status switch
            {
                "Pending" => "Email",
                "Confirmed" => "Push",
                "Shipped" => "Email",
                "Delivered" => "Push",
                "Cancelled" => "Email",
                _ => "Email",
            };
        }

        // Regular customers get email notifications
        if (orderEvent.CustomerId.Contains("REGULAR") || orderEvent.CustomerId.Contains("RETURNING") || orderEvent.CustomerId.Contains("MEMBER"))
            return "Email";

        // New customers get email notifications
        if (orderEvent.CustomerId.Contains("NEW") || orderEvent.CustomerId.Contains("GUEST"))
            return "Email";

        // Corporate customers get email notifications
        if (orderEvent.CustomerId.Contains("CORPORATE"))
            return "Email";

        // International customers get email notifications
        if (orderEvent.CustomerId.Contains("INTERNATIONAL"))
            return "Email";

        // Default to email
        return "Email";
    }

    /// <summary>
    ///     Gets the recipient address based on notification type.
    /// </summary>
    /// <param name="customerId">The customer identifier.</param>
    /// <param name="notificationType">The notification type.</param>
    /// <returns>The recipient address.</returns>
    private string GetRecipientAddress(string customerId, string notificationType)
    {
        return notificationType switch
        {
            "Email" => _customerEmails.TryGetValue(customerId, out var email)
                ? email
                : "default@example.com",
            "SMS" => _customerPhones.TryGetValue(customerId, out var phone)
                ? phone
                : "+1-555-0000",
            "Push" => _customerDevices.TryGetValue(customerId, out var device)
                ? device
                : "default_device_token",
            "InApp" => customerId, // In-app notifications use customer ID
            _ => _customerEmails.TryGetValue(customerId, out var defaultEmail)
                ? defaultEmail
                : "default@example.com",
        };
    }

    /// <summary>
    ///     Generates appropriate message and subject for the notification.
    /// </summary>
    /// <param name="orderEvent">The order event.</param>
    /// <param name="notificationType">The notification type.</param>
    /// <returns>A tuple containing message and subject.</returns>
    private (string message, string subject) GenerateNotificationContent(OrderEvent orderEvent, string notificationType)
    {
        var productName = GetProductName(orderEvent.ProductId);

        return orderEvent.Status switch
        {
            "Pending" => GeneratePendingNotification(orderEvent, productName, notificationType),
            "Confirmed" => GenerateConfirmedNotification(orderEvent, productName, notificationType),
            "Processing" => GenerateProcessingNotification(orderEvent, productName, notificationType),
            "Shipped" => GenerateShippedNotification(orderEvent, productName, notificationType),
            "Delivered" => GenerateDeliveredNotification(orderEvent, productName, notificationType),
            "Cancelled" => GenerateCancelledNotification(orderEvent, productName, notificationType),
            _ => GenerateDefaultNotification(orderEvent, productName, notificationType),
        };
    }

    /// <summary>
    ///     Generates notification content for pending orders.
    /// </summary>
    private (string message, string subject) GeneratePendingNotification(OrderEvent orderEvent, string productName, string notificationType)
    {
        if (notificationType == "Push")
            return ($"Your order {orderEvent.OrderId} for {productName} is pending confirmation.", "Order Pending");

        return (
            $"Thank you for your order! Your order {orderEvent.OrderId} for {orderEvent.Quantity} x {productName} (${orderEvent.TotalAmount:F2}) is pending confirmation. We'll notify you once it's confirmed.",
            $"Order {orderEvent.OrderId} - Pending Confirmation");
    }

    /// <summary>
    ///     Generates notification content for confirmed orders.
    /// </summary>
    private (string message, string subject) GenerateConfirmedNotification(OrderEvent orderEvent, string productName, string notificationType)
    {
        if (notificationType == "Push")
            return ($"Great news! Your order {orderEvent.OrderId} has been confirmed!", "Order Confirmed");

        return (
            $"Great news! Your order {orderEvent.OrderId} for {orderEvent.Quantity} x {productName} has been confirmed and is now being prepared for shipping. Expected delivery: {DateTime.Now.AddDays(3):d}.",
            $"Order {orderEvent.OrderId} - Confirmed");
    }

    /// <summary>
    ///     Generates notification content for processing orders.
    /// </summary>
    private (string message, string subject) GenerateProcessingNotification(OrderEvent orderEvent, string productName, string notificationType)
    {
        if (notificationType == "Push")
            return ($"Your order {orderEvent.OrderId} is being processed!", "Order Processing");

        return ($"Your order {orderEvent.OrderId} for {productName} is currently being processed. We're preparing your items for shipment.",
            $"Order {orderEvent.OrderId} - Processing");
    }

    /// <summary>
    ///     Generates notification content for shipped orders.
    /// </summary>
    private (string message, string subject) GenerateShippedNotification(OrderEvent orderEvent, string productName, string notificationType)
    {
        if (notificationType == "Push")
            return ($"Your order {orderEvent.OrderId} has been shipped!", "Order Shipped");

        return (
            $"Good news! Your order {orderEvent.OrderId} for {orderEvent.Quantity} x {productName} has been shipped and is on its way to you. Track your package using the order number.",
            $"Order {orderEvent.OrderId} - Shipped");
    }

    /// <summary>
    ///     Generates notification content for delivered orders.
    /// </summary>
    private (string message, string subject) GenerateDeliveredNotification(OrderEvent orderEvent, string productName, string notificationType)
    {
        if (notificationType == "Push")
            return ($"Your order {orderEvent.OrderId} has been delivered!", "Order Delivered");

        return (
            $"Your order {orderEvent.OrderId} for {productName} has been delivered! We hope you enjoy your purchase. Please leave a review to help other customers.",
            $"Order {orderEvent.OrderId} - Delivered");
    }

    /// <summary>
    ///     Generates notification content for cancelled orders.
    /// </summary>
    private (string message, string subject) GenerateCancelledNotification(OrderEvent orderEvent, string productName, string notificationType)
    {
        if (notificationType == "Push")
            return ($"Your order {orderEvent.OrderId} has been cancelled.", "Order Cancelled");

        return (
            $"We're sorry to inform you that your order {orderEvent.OrderId} for {productName} has been cancelled. If you didn't request this cancellation, please contact our customer support.",
            $"Order {orderEvent.OrderId} - Cancelled");
    }

    /// <summary>
    ///     Generates default notification content.
    /// </summary>
    private (string message, string subject) GenerateDefaultNotification(OrderEvent orderEvent, string productName, string notificationType)
    {
        if (notificationType == "Push")
            return ($"Update on your order {orderEvent.OrderId}", "Order Update");

        return ($"There's an update on your order {orderEvent.OrderId} for {productName}. Status: {orderEvent.Status}", $"Order {orderEvent.OrderId} - Update");
    }

    /// <summary>
    ///     Gets a user-friendly product name from product ID.
    /// </summary>
    /// <param name="productId">The product identifier.</param>
    /// <returns>A user-friendly product name.</returns>
    private string GetProductName(string productId)
    {
        return productId switch
        {
            "PROD_LAPTOP_001" => "Laptop Computer",
            "PROD_PHONE_002" => "Smartphone",
            "PROD_TABLET_003" => "Tablet",
            "PROD_HEADPHONES_004" => "Headphones",
            "PROD_MOUSE_005" => "Computer Mouse",
            "PROD_KEYBOARD_006" => "Keyboard",
            "PROD_MONITOR_007" => "Computer Monitor",
            "PROD_CAMERA_008" => "Digital Camera",
            "PROD_SPEAKER_009" => "Speaker",
            "PROD_CHARGER_010" => "Charger",
            "PROD_CASE_011" => "Protective Case",
            "PROD_CABLE_012" => "Cable",
            "PROD_DESK_013" => "Desk",
            "PROD_CHAIR_014" => "Chair",
            "PROD_LAMP_015" => "Lamp",
            _ => productId,
        };
    }

    /// <summary>
    ///     Determines notification priority based on order status.
    /// </summary>
    /// <param name="orderStatus">The order status.</param>
    /// <returns>The notification priority.</returns>
    private int DetermineNotificationPriority(string orderStatus)
    {
        return orderStatus switch
        {
            "Pending" => 2, // Medium priority
            "Confirmed" => 2, // Medium priority
            "Processing" => 2, // Medium priority
            "Shipped" => 3, // High priority
            "Delivered" => 1, // Low priority
            "Cancelled" => 4, // Critical priority
            _ => 2, // Default medium priority
        };
    }

    /// <summary>
    ///     Determines if notification requires confirmation.
    /// </summary>
    /// <param name="orderStatus">The order status.</param>
    /// <param name="notificationType">The notification type.</param>
    /// <returns>True if confirmation is required.</returns>
    private bool DetermineConfirmationRequirement(string orderStatus, string notificationType)
    {
        // Critical notifications require confirmation
        if (orderStatus == "Cancelled" || orderStatus == "Shipped")
            return true;

        // Push notifications often require confirmation
        if (notificationType == "Push")
            return true;

        return false;
    }

    /// <summary>
    ///     Gets the template name for the notification.
    /// </summary>
    /// <param name="orderStatus">The order status.</param>
    /// <param name="notificationType">The notification type.</param>
    /// <returns>The template name.</returns>
    private string GetTemplateName(string orderStatus, string notificationType)
    {
        return $"{notificationType.ToLowerInvariant()}_order_{orderStatus.ToLowerInvariant()}";
    }

    /// <summary>
    ///     Generates template data for the notification.
    /// </summary>
    /// <param name="orderEvent">The order event.</param>
    /// <returns>A dictionary of template data.</returns>
    private Dictionary<string, object> GenerateTemplateData(OrderEvent orderEvent)
    {
        return new Dictionary<string, object>
        {
            ["order_id"] = orderEvent.OrderId,
            ["customer_id"] = orderEvent.CustomerId,
            ["product_id"] = orderEvent.ProductId,
            ["product_name"] = GetProductName(orderEvent.ProductId),
            ["quantity"] = orderEvent.Quantity,
            ["price"] = orderEvent.Price,
            ["total_amount"] = orderEvent.TotalAmount,
            ["order_status"] = orderEvent.Status,
            ["payment_method"] = orderEvent.PaymentMethod ?? "Unknown",
            ["shipping_address"] = orderEvent.ShippingAddress ?? "Unknown",
            ["order_timestamp"] = orderEvent.Timestamp,
            ["notification_timestamp"] = DateTime.UtcNow,
        };
    }

    /// <summary>
    ///     Determines when the notification should be sent.
    /// </summary>
    /// <param name="orderStatus">The order status.</param>
    /// <param name="orderTimestamp">The order timestamp.</param>
    /// <returns>The scheduled time (null for immediate).</returns>
    private DateTime? DetermineScheduledTime(string orderStatus, DateTime orderTimestamp)
    {
        return orderStatus switch
        {
            "Delivered" => orderTimestamp.AddHours(2), // Delay delivery confirmation slightly
            "Processing" => orderTimestamp.AddMinutes(30), // Give some time before processing notification
            _ => null, // Immediate for other statuses
        };
    }

    /// <summary>
    ///     Logs the notification event for monitoring purposes.
    /// </summary>
    /// <param name="orderEvent">The original order event.</param>
    /// <param name="notificationEvent">The generated notification event.</param>
    /// <returns>A task representing the logging operation.</returns>
    private async Task LogNotificationEvent(OrderEvent orderEvent, NotificationEvent notificationEvent)
    {
        await Task.CompletedTask; // Simulate async logging

        var priorityText = notificationEvent.Priority switch
        {
            4 => "CRITICAL",
            3 => "HIGH",
            2 => "MEDIUM",
            _ => "LOW",
        };

        Console.WriteLine(
            $"NotificationProcessor: Order {orderEvent.OrderId} - " +
            $"Type: {notificationEvent.NotificationType} - " +
            $"Status: {orderEvent.Status} - " +
            $"Priority: {priorityText} - " +
            $"Recipient: {notificationEvent.RecipientAddress}");
    }

    /// <summary>
    ///     Gets the current notification statistics.
    /// </summary>
    /// <returns>The total number of notifications generated.</returns>
    public int GetStatistics()
    {
        Console.WriteLine($"NotificationProcessor: Total notifications generated: {_totalNotificationsGenerated}");
        return _totalNotificationsGenerated;
    }
}

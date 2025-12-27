using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_BranchNode.Models;

namespace Sample_BranchNode.Nodes;

/// <summary>
///     Sink node that outputs pipeline results to the console with formatted display.
///     This sink handles different event types with appropriate formatting and colors.
/// </summary>
public class ConsoleSink : SinkNode<object>
{
    private readonly string _sinkName;
    private int _totalItemsProcessed;

    /// <summary>
    ///     Initializes a new instance of the ConsoleSink.
    /// </summary>
    /// <param name="sinkName">Optional name for identifying this sink instance.</param>
    public ConsoleSink(string sinkName = "ConsoleSink")
    {
        _sinkName = sinkName;
        Console.WriteLine($"{_sinkName}: Initialized for console output");
    }

    /// <summary>
    ///     Processes and displays items as they arrive from the pipeline.
    ///     This method handles different event types with appropriate formatting.
    /// </summary>
    /// <param name="input">The data pipe containing items to process.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the display operation.</returns>
    public override async Task ExecuteAsync(
        IDataPipe<object> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine($"{_sinkName}: Starting to process pipeline output...");
        Console.WriteLine();

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            _totalItemsProcessed++;

            // Use pattern matching to handle different event types
            switch (item)
            {
                case InventoryUpdate inventoryUpdate:
                    await DisplayInventoryUpdate(inventoryUpdate);
                    break;

                case AnalyticsEvent analyticsEvent:
                    await DisplayAnalyticsEvent(analyticsEvent);
                    break;

                case NotificationEvent notificationEvent:
                    await DisplayNotificationEvent(notificationEvent);
                    break;

                case OrderEvent orderEvent:
                    await DisplayOrderEvent(orderEvent);
                    break;

                default:
                    await DisplayUnknownItem(item);
                    break;
            }
        }

        Console.WriteLine();
        Console.WriteLine($"{_sinkName}: Completed processing {_totalItemsProcessed} items");
    }

    /// <summary>
    ///     Displays an inventory update event with appropriate formatting.
    /// </summary>
    /// <param name="inventoryUpdate">The inventory update to display.</param>
    /// <returns>A task representing the display operation.</returns>
    private async Task DisplayInventoryUpdate(InventoryUpdate inventoryUpdate)
    {
        await Task.CompletedTask;

        var priorityColor = inventoryUpdate.Priority switch
        {
            > 4 => "\u001b[91m", // Red for critical
            > 3 => "\u001b[93m", // Yellow for high
            > 2 => "\u001b[96m", // Cyan for medium
            _ => "\u001b[92m", // Green for low
        };

        var resetColor = "\u001b[0m";

        Console.WriteLine(
            $"{priorityColor}[INVENTORY]{resetColor} " +
            $"Product: {inventoryUpdate.ProductId} | " +
            $"Change: {inventoryUpdate.QuantityChange:+#;-#;0} | " +
            $"Current: {inventoryUpdate.CurrentInventory} | " +
            $"Location: {inventoryUpdate.LocationId} | " +
            $"Priority: {inventoryUpdate.Priority} | " +
            $"Reason: {inventoryUpdate.Reason} | " +
            $"Time: {inventoryUpdate.Timestamp:HH:mm:ss}");
    }

    /// <summary>
    ///     Displays an analytics event with appropriate formatting.
    /// </summary>
    /// <param name="analyticsEvent">The analytics event to display.</param>
    /// <returns>A task representing the display operation.</returns>
    private async Task DisplayAnalyticsEvent(AnalyticsEvent analyticsEvent)
    {
        await Task.CompletedTask;

        Console.WriteLine(
            $"\u001b[94m[ANALYTICS]\u001b[0m " +
            $"Event: {analyticsEvent.EventType} | " +
            $"Product: {analyticsEvent.ProductCategory} ({analyticsEvent.ProductId}) | " +
            $"Customer: {analyticsEvent.CustomerSegment} | " +
            $"Region: {analyticsEvent.Region} | " +
            $"Price: ${analyticsEvent.Price:F2} | " +
            $"Session: {analyticsEvent.SessionId} | " +
            $"Time: {analyticsEvent.Timestamp:HH:mm:ss}");

        // Display key metadata if available
        if (analyticsEvent.Metadata.Count == 0)
        {
            // No metadata to display
        }
        else
        {
            var keyMetadata = analyticsEvent.Metadata
                .Where(kvp => kvp.Key is "order_total" or "price_range" or "order_size")
                .Select(kvp => $"{kvp.Key}: {kvp.Value}")
                .Take(3);

            if (keyMetadata.Any())
                Console.WriteLine($"    Metadata: {string.Join(", ", keyMetadata)}");
        }
    }

    /// <summary>
    ///     Displays a notification event with appropriate formatting.
    /// </summary>
    /// <param name="notificationEvent">The notification event to display.</param>
    /// <returns>A task representing the display operation.</returns>
    private async Task DisplayNotificationEvent(NotificationEvent notificationEvent)
    {
        await Task.CompletedTask;

        var priorityColor = notificationEvent.Priority switch
        {
            4 => "\u001b[91m", // Red for critical
            3 => "\u001b[93m", // Yellow for high
            2 => "\u001b[96m", // Cyan for medium
            _ => "\u001b[92m", // Green for low
        };

        var typeColor = notificationEvent.NotificationType switch
        {
            "Email" => "\u001b[95m", // Magenta
            "SMS" => "\u001b[93m", // Yellow
            "Push" => "\u001b[94m", // Blue
            "InApp" => "\u001b[96m", // Cyan
            _ => "\u001b[97m", // White
        };

        var resetColor = "\u001b[0m";

        Console.WriteLine(
            $"{priorityColor}[NOTIFICATION]{resetColor} " +
            $"{typeColor}{notificationEvent.NotificationType}{resetColor} | " +
            $"Order: {notificationEvent.OrderId} | " +
            $"Customer: {notificationEvent.CustomerId} | " +
            $"Priority: {notificationEvent.Priority} | " +
            $"Recipient: {notificationEvent.RecipientAddress} | " +
            $"Time: {notificationEvent.Timestamp:HH:mm:ss}");

        // Display subject if available
        if (!string.IsNullOrEmpty(notificationEvent.Subject))
            Console.WriteLine($"    Subject: {notificationEvent.Subject}");

        // Display message preview (first 60 characters)
        if (!string.IsNullOrEmpty(notificationEvent.Message))
        {
            var preview = notificationEvent.Message.Length > 60
                ? string.Concat(notificationEvent.Message.AsSpan(0, 57), "...")
                : notificationEvent.Message;

            Console.WriteLine($"    Message: {preview}");
        }

        // Display scheduling info if applicable
        if (notificationEvent.ScheduledTime.HasValue)
            Console.WriteLine($"    Scheduled: {notificationEvent.ScheduledTime.Value:yyyy-MM-dd HH:mm:ss}");
    }

    /// <summary>
    ///     Displays an order event with appropriate formatting.
    /// </summary>
    /// <param name="orderEvent">The order event to display.</param>
    /// <returns>A task representing the display operation.</returns>
    private async Task DisplayOrderEvent(OrderEvent orderEvent)
    {
        await Task.CompletedTask;

        var statusColor = orderEvent.Status switch
        {
            "Pending" => "\u001b[93m", // Yellow
            "Confirmed" => "\u001b[92m", // Green
            "Processing" => "\u001b[96m", // Cyan
            "Shipped" => "\u001b[94m", // Blue
            "Delivered" => "\u001b[95m", // Magenta
            "Cancelled" => "\u001b[91m", // Red
            _ => "\u001b[97m", // White
        };

        var resetColor = "\u001b[0m";

        Console.WriteLine(
            $"{statusColor}[ORDER]{resetColor} " +
            $"ID: {orderEvent.OrderId} | " +
            $"Customer: {orderEvent.CustomerId} | " +
            $"Product: {orderEvent.ProductId} | " +
            $"Qty: {orderEvent.Quantity} | " +
            $"Price: ${orderEvent.Price:F2} | " +
            $"Total: ${orderEvent.TotalAmount:F2} | " +
            $"Status: {statusColor}{orderEvent.Status}{resetColor} | " +
            $"Time: {orderEvent.Timestamp:HH:mm:ss}");

        // Display additional info
        var details = new List<string>();

        if (!string.IsNullOrEmpty(orderEvent.PaymentMethod))
            details.Add($"Payment: {orderEvent.PaymentMethod}");

        if (!string.IsNullOrEmpty(orderEvent.ShippingAddress))
            details.Add($"Ship to: {orderEvent.ShippingAddress.Split(',')[0]}"); // Just city, state

        if (details.Count > 0)
            Console.WriteLine($"    Details: {string.Join(" | ", details)}");
    }

    /// <summary>
    ///     Displays an unknown item type with basic formatting.
    /// </summary>
    /// <param name="item">The unknown item to display.</param>
    /// <returns>A task representing the display operation.</returns>
    private async Task DisplayUnknownItem(object item)
    {
        await Task.CompletedTask;

        Console.WriteLine(
            $"\u001b[91m[UNKNOWN]\u001b[0m " +
            $"Type: {item.GetType().Name} | " +
            $"Value: {item} | " +
            $"Time: {DateTime.UtcNow:HH:mm:ss}");
    }

    /// <summary>
    ///     Gets the current sink statistics.
    /// </summary>
    /// <returns>The total number of items processed.</returns>
    public int GetStatistics()
    {
        Console.WriteLine($"{_sinkName}: Total items processed: {_totalItemsProcessed}");
        return _totalItemsProcessed;
    }

    /// <summary>
    ///     Resets sink statistics.
    /// </summary>
    public void ResetStatistics()
    {
        _totalItemsProcessed = 0;
        Console.WriteLine($"{_sinkName}: Statistics reset");
    }

    /// <summary>
    ///     Displays a separator line for better readability.
    /// </summary>
    /// <param name="title">Optional title to display in the separator.</param>
    private void DisplaySeparator(string? title = null)
    {
        if (string.IsNullOrEmpty(title))
            Console.WriteLine("─────────────────────────────────────────────────────────────────────────────");
        else
        {
            var padding = (69 - title.Length) / 2;
            var line = new string('─', padding) + $" {title} " + new string('─', padding);
            Console.WriteLine(line);
        }
    }
}

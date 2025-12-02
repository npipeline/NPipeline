using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_AggregateNode.Models;

namespace Sample_AggregateNode.Nodes;

/// <summary>
///     Source node that generates simulated real-time analytics events.
///     This node simulates various types of user interactions like page views, clicks, purchases, etc.
/// </summary>
public class EventSource : SourceNode<AnalyticsEvent>
{
    private readonly string[] _categories =
    {
        AnalyticsConstants.UserEngagement,
        AnalyticsConstants.ECommerce,
        AnalyticsConstants.SearchActivity,
        AnalyticsConstants.Authentication,
        AnalyticsConstants.Navigation,
    };

    private readonly string[] _eventTypes =
    {
        AnalyticsConstants.PageView,
        AnalyticsConstants.Click,
        AnalyticsConstants.Purchase,
        AnalyticsConstants.AddToCart,
        AnalyticsConstants.Search,
        AnalyticsConstants.Login,
        AnalyticsConstants.Logout,
    };

    private readonly string[] _pages =
    {
        "/home", "/products", "/cart", "/checkout", "/profile",
        "/search", "/category/electronics", "/category/clothing", "/about", "/contact",
    };

    private readonly Random _random = new();

    private readonly string[] _userIds =
    {
        "user_001", "user_002", "user_003", "user_004", "user_005",
        "user_006", "user_007", "user_008", "user_009", "user_010",
    };

    /// <summary>
    ///     Generates a stream of simulated analytics events with realistic timing and distribution.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token to stop event generation.</param>
    /// <returns>A data pipe containing analytics events.</returns>
    public override IDataPipe<AnalyticsEvent> Execute(
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine("EventSource: Starting to generate analytics events...");
        Console.WriteLine("EventSource: Simulating real-time user interactions...");

        var eventCount = 0;
        var startTime = DateTime.UtcNow;

        async IAsyncEnumerable<AnalyticsEvent> GenerateEvents([EnumeratorCancellation] CancellationToken ct = default)
        {
            while (!ct.IsCancellationRequested)
            {
                var analyticsEvent = GenerateRandomEvent(eventCount++);
                yield return analyticsEvent;

                // Simulate variable event frequency (1-5 events per second on average)
                var delayMs = _random.Next(200, 1000);
                await Task.Delay(delayMs, ct);

                // Log progress every 50 events
                if (eventCount % 50 == 0)
                {
                    var elapsed = DateTime.UtcNow - startTime;
                    var rate = eventCount / elapsed.TotalSeconds;
                    Console.WriteLine($"EventSource: Generated {eventCount} events ({rate:F1} events/sec)");
                }
            }

            Console.WriteLine($"EventSource: Stopped after generating {eventCount} events");
        }

        return new StreamingDataPipe<AnalyticsEvent>(GenerateEvents(cancellationToken), "EventSource");
    }

    /// <summary>
    ///     Generates a single random analytics event with realistic data.
    /// </summary>
    /// <param name="sequenceNumber">The sequence number for the event.</param>
    /// <returns>A randomly generated analytics event.</returns>
    private AnalyticsEvent GenerateRandomEvent(int sequenceNumber)
    {
        var eventType = _eventTypes[_random.Next(_eventTypes.Length)];
        var category = GetCategoryForEventType(eventType);
        var userId = _userIds[_random.Next(_userIds.Length)];
        var timestamp = DateTime.UtcNow.AddSeconds(-_random.Next(0, 300)); // Events from last 5 minutes

        // Generate value based on event type
        var value = GenerateValueForEventType(eventType);

        // Generate properties based on event type
        var properties = GeneratePropertiesForEventType(eventType);

        return new AnalyticsEvent(
            $"event_{sequenceNumber:D6}",
            eventType,
            category,
            value,
            timestamp,
            userId,
            properties
        );
    }

    /// <summary>
    ///     Gets the appropriate category for a given event type.
    /// </summary>
    /// <param name="eventType">The event type.</param>
    /// <returns>The corresponding category.</returns>
    private string GetCategoryForEventType(string eventType)
    {
        return eventType switch
        {
            AnalyticsConstants.PageView or AnalyticsConstants.Click => AnalyticsConstants.UserEngagement,
            AnalyticsConstants.Purchase or AnalyticsConstants.AddToCart => AnalyticsConstants.ECommerce,
            AnalyticsConstants.Search => AnalyticsConstants.SearchActivity,
            AnalyticsConstants.Login or AnalyticsConstants.Logout => AnalyticsConstants.Authentication,
            _ => AnalyticsConstants.Navigation,
        };
    }

    /// <summary>
    ///     Generates a realistic value based on the event type.
    /// </summary>
    /// <param name="eventType">The event type.</param>
    /// <returns>A value appropriate for the event type.</returns>
    private decimal GenerateValueForEventType(string eventType)
    {
        return eventType switch
        {
            AnalyticsConstants.PageView => 1, // Page views have value of 1
            AnalyticsConstants.Click => (decimal)_random.NextDouble() * 5, // Clicks: 0-5
            AnalyticsConstants.Purchase => (decimal)(_random.NextDouble() * 500 + 10), // Purchases: $10-510
            AnalyticsConstants.AddToCart => (decimal)(_random.NextDouble() * 200 + 5), // Add to cart: $5-205
            AnalyticsConstants.Search => 1, // Searches have value of 1
            AnalyticsConstants.Login => 10, // Logins have higher value
            AnalyticsConstants.Logout => 5, // Logouts have medium value
            _ => (decimal)_random.NextDouble() * 10, // Default: 0-10
        };
    }

    /// <summary>
    ///     Generates event-specific properties for realistic simulation.
    /// </summary>
    /// <param name="eventType">The event type.</param>
    /// <returns>A dictionary of event properties.</returns>
    private Dictionary<string, object> GeneratePropertiesForEventType(string eventType)
    {
        var properties = new Dictionary<string, object>
        {
            ["session_id"] = $"session_{_random.Next(1000, 9999)}",
            ["device_type"] = _random.Next(0, 3) switch { 0 => "desktop", 1 => "mobile", _ => "tablet" },
            ["browser"] = _random.Next(0, 4) switch { 0 => "chrome", 1 => "firefox", 2 => "safari", _ => "edge" },
        };

        switch (eventType)
        {
            case AnalyticsConstants.PageView:
                properties["page"] = _pages[_random.Next(_pages.Length)];

                properties["referrer"] = _random.Next(0, 2) == 0
                    ? "direct"
                    : "search";

                break;

            case AnalyticsConstants.Click:
                properties["element"] = _random.Next(0, 3) switch { 0 => "button", 1 => "link", _ => "image" };
                properties["page"] = _pages[_random.Next(_pages.Length)];
                break;

            case AnalyticsConstants.Purchase:
                properties["product_id"] = $"prod_{_random.Next(100, 999)}";
                properties["quantity"] = _random.Next(1, 5);
                properties["payment_method"] = _random.Next(0, 3) switch { 0 => "credit_card", 1 => "paypal", _ => "apple_pay" };
                break;

            case AnalyticsConstants.AddToCart:
                properties["product_id"] = $"prod_{_random.Next(100, 999)}";
                properties["quantity"] = _random.Next(1, 3);
                break;

            case AnalyticsConstants.Search:
                properties["query"] = $"search_term_{_random.Next(10, 99)}";
                properties["results_count"] = _random.Next(0, 100);
                break;
        }

        return properties;
    }
}

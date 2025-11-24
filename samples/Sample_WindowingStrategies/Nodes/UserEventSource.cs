using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_WindowingStrategies.Models;

namespace Sample_WindowingStrategies.Nodes;

/// <summary>
///     Source node that generates user events for the windowing strategies sample.
///     This node simulates a user analytics platform receiving events from multiple users.
/// </summary>
public class UserEventSource : SourceNode<UserEvent>
{
    private readonly string[] _browsers;
    private readonly string[] _cities;
    private readonly string[] _countries;
    private readonly string[] _deviceTypes;
    private readonly int _eventCount;
    private readonly string[] _eventTypes;
    private readonly TimeSpan _interval;
    private readonly string[] _operatingSystems;
    private readonly string[] _pageUrls;
    private readonly int _sessionCount;
    private readonly int _userCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="UserEventSource" /> class.
    /// </summary>
    /// <param name="eventCount">The number of user events to generate.</param>
    /// <param name="interval">The interval between events.</param>
    /// <param name="userCount">The number of different users to simulate.</param>
    /// <param name="sessionCount">The number of different sessions to simulate.</param>
    public UserEventSource(
        int eventCount = 200,
        TimeSpan? interval = null,
        int userCount = 20,
        int sessionCount = 50)
    {
        _eventCount = eventCount;
        _interval = interval ?? TimeSpan.FromMilliseconds(75);
        _userCount = userCount;
        _sessionCount = sessionCount;

        _eventTypes = new[] { "PageView", "Click", "Search", "AddToCart", "Purchase", "Login", "Logout", "Signup", "Download", "Share" };

        _pageUrls = new[]
        {
            "/home", "/products", "/products/laptops", "/products/phones", "/cart",
            "/checkout", "/profile", "/settings", "/search", "/about", "/contact", "/blog",
        };

        _deviceTypes = new[] { "Desktop", "Mobile", "Tablet" };
        _browsers = new[] { "Chrome", "Firefox", "Safari", "Edge", "Opera" };
        _operatingSystems = new[] { "Windows", "macOS", "Linux", "iOS", "Android" };
        _countries = new[] { "United States", "United Kingdom", "Canada", "Australia", "Germany", "France", "Japan", "India" };
        _cities = new[] { "New York", "London", "Toronto", "Sydney", "Berlin", "Paris", "Tokyo", "Mumbai", "San Francisco", "Chicago" };
    }

    /// <summary>
    ///     Generates a stream of user events from multiple users and sessions.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the user events.</returns>
    public override IDataPipe<UserEvent> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Generating {_eventCount} user events from {_userCount} users with {_interval.TotalMilliseconds}ms intervals");

        var events = new List<UserEvent>();
        var random = new Random(42); // Fixed seed for reproducible results
        var baseTime = DateTime.UtcNow;

        // Generate user and session assignments
        var userSessions = new Dictionary<string, List<string>>();

        for (var i = 0; i < _userCount; i++)
        {
            var userId = $"User-{i:D3}";
            var sessionCount = random.Next(1, 4); // 1-3 sessions per user
            var sessions = new List<string>();

            for (var j = 0; j < sessionCount && sessions.Count < _sessionCount; j++)
            {
                sessions.Add($"Session-{userId}-{j:D2}");
            }

            userSessions[userId] = sessions;
        }

        var allSessions = userSessions.SelectMany(kvp => kvp.Value).ToList();
        var sessionStartTimes = new Dictionary<string, DateTime>();

        for (var i = 0; i < _eventCount; i++)
        {
            var userId = $"User-{random.Next(_userCount):D3}";
            var userSessionsList = userSessions[userId];
            var sessionId = userSessionsList[random.Next(userSessionsList.Count)];

            // Track session start time for realistic session durations
            if (!sessionStartTimes.TryGetValue(sessionId, out var sessionStartTime))
            {
                sessionStartTime = baseTime.AddMinutes(random.Next(-60, 0)); // Sessions started in the last hour
                sessionStartTimes[sessionId] = sessionStartTime;
            }

            var eventType = _eventTypes[random.Next(_eventTypes.Length)];
            var pageUrl = _pageUrls[random.Next(_pageUrls.Length)];

            var referrerUrl = random.NextDouble() > 0.3
                ? _pageUrls[random.Next(_pageUrls.Length)]
                : null;

            var deviceType = _deviceTypes[random.Next(_deviceTypes.Length)];
            var browser = _browsers[random.Next(_browsers.Length)];
            var operatingSystem = _operatingSystems[random.Next(_operatingSystems.Length)];
            var country = _countries[random.Next(_countries.Length)];
            var city = _cities[random.Next(_cities.Length)];

            var timestamp = sessionStartTimes[sessionId].AddSeconds(random.Next(0, 3600)); // Events within session duration
            var ipAddress = GenerateIpAddress(random, country);
            var userAgent = $"{browser}/{random.Next(80, 120)}.0 on {operatingSystem}";

            var metadata = new Dictionary<string, object>
            {
                ["Source"] = "Web",
                ["Campaign"] = $"Campaign-{random.Next(1, 10)}",
                ["Version"] = $"v{random.Next(1, 5)}.{random.Next(0, 10)}",
                ["Experiment"] = $"Exp-{random.Next(1, 5):D2}",
            };

            var propertyValue = eventType switch
            {
                "Search" => $"search-term-{random.Next(1, 100)}",
                "Purchase" => $"product-{random.Next(1, 50)}",
                "AddToCart" => $"product-{random.Next(1, 50)}",
                _ => null,
            };

            var userEvent = new UserEvent(
                $"Event-{i:D4}",
                userId,
                sessionId,
                timestamp,
                eventType,
                pageUrl,
                referrerUrl,
                userAgent,
                ipAddress,
                deviceType,
                browser,
                operatingSystem,
                country,
                city,
                propertyValue,
                metadata);

            events.Add(userEvent);

            // Simulate the interval between events
            if (i < _eventCount - 1) // Don't wait after the last event
                Task.Delay(_interval, cancellationToken).Wait(cancellationToken);
        }

        Console.WriteLine($"Successfully generated {events.Count} user events");

        // Group by user to show the distribution
        var userGroups = events.GroupBy(e => e.UserId).ToList();
        var sessionGroups = events.GroupBy(e => e.SessionId).ToList();

        Console.WriteLine($"  Users: {userGroups.Count}");
        Console.WriteLine($"  Sessions: {sessionGroups.Count}");
        Console.WriteLine($"  Average events per user: {(double)events.Count / userGroups.Count:F1}");
        Console.WriteLine($"  Average events per session: {(double)events.Count / sessionGroups.Count:F1}");

        return new ListDataPipe<UserEvent>(events, "UserEventSource");
    }

    private static string GenerateIpAddress(Random random, string country)
    {
        return country switch
        {
            "United States" => $"192.168.{random.Next(1, 255)}.{random.Next(1, 255)}",
            "United Kingdom" => $"10.0.{random.Next(1, 255)}.{random.Next(1, 255)}",
            "Canada" => $"172.16.{random.Next(1, 255)}.{random.Next(1, 255)}",
            "Australia" => $"203.0.{random.Next(1, 255)}.{random.Next(1, 255)}",
            "Germany" => $"217.0.{random.Next(1, 255)}.{random.Next(1, 255)}",
            "France" => $"90.0.{random.Next(1, 255)}.{random.Next(1, 255)}",
            "Japan" => $"202.0.{random.Next(1, 255)}.{random.Next(1, 255)}",
            "India" => $"117.0.{random.Next(1, 255)}.{random.Next(1, 255)}",
            _ => $"1.2.{random.Next(1, 255)}.{random.Next(1, 255)}",
        };
    }
}

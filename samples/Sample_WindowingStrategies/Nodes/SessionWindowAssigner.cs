using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using Sample_WindowingStrategies.Models;

namespace Sample_WindowingStrategies.Nodes;

/// <summary>
///     Transform node that creates user sessions from individual user events.
///     This node implements session-based windowing by grouping events by user and session timeout.
/// </summary>
public class SessionWindowAssigner : TransformNode<UserEvent, UserSession>
{
    private readonly Queue<UserSession> _emittedSessions;
    private readonly IPipelineLogger _logger;
    private readonly int _maxEventsPerSession;
    private readonly Random _random;
    private readonly Dictionary<string, SessionState> _sessionStates;
    private readonly TimeSpan _sessionTimeout;

    /// <summary>
    ///     Initializes a new instance of <see cref="SessionWindowAssigner" /> class.
    /// </summary>
    /// <param name="sessionTimeout">The timeout period for session inactivity.</param>
    /// <param name="maxEventsPerSession">Maximum events per session before emitting.</param>
    /// <param name="logger">The pipeline logger for logging operations.</param>
    public SessionWindowAssigner(TimeSpan sessionTimeout, int maxEventsPerSession = 10, IPipelineLogger? logger = null)
    {
        _sessionTimeout = sessionTimeout;
        _maxEventsPerSession = maxEventsPerSession;
        _logger = logger ?? NullPipelineLoggerFactory.Instance.CreateLogger(nameof(SessionWindowAssigner));
        _sessionStates = new Dictionary<string, SessionState>();
        _emittedSessions = new Queue<UserSession>();
        _random = new Random(42); // Fixed seed for reproducible results
    }

    /// <summary>
    ///     Processes individual user events and creates sessions when timeout is reached.
    ///     This method implements session windowing by tracking user activity and creating sessions.
    /// </summary>
    /// <param name="userEvent">The individual user event to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A user session when timeout is reached, null otherwise.</returns>
    public override Task<UserSession> ExecuteAsync(
        UserEvent userEvent,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // First, return any previously emitted sessions
        if (_emittedSessions.Count > 0)
        {
            var session = _emittedSessions.Dequeue();

            _logger.Log(LogLevel.Debug, "SessionWindowAssigner: Returning queued session {SessionId} with {EventCount} events",
                session.SessionId, session.Events.Count);

            return Task.FromResult(session);
        }

        if (userEvent == null)
        {
            _logger.Log(LogLevel.Debug, "SessionWindowAssigner: Received null user event, completing remaining sessions");
            CompleteProcessing();

            // Return first queued session if available
            if (_emittedSessions.Count > 0)
            {
                var session = _emittedSessions.Dequeue();

                _logger.Log(LogLevel.Debug, "SessionWindowAssigner: Returning completed session {SessionId} with {EventCount} events",
                    session.SessionId, session.Events.Count);

                return Task.FromResult(session);
            }

            return Task.FromResult(CreateDummySession()); // Return dummy session to maintain flow
        }

        var sessionKey = $"{userEvent.UserId}_{userEvent.SessionId}";
        var now = DateTime.UtcNow;

        // Check if we have an existing session for this user
        if (_sessionStates.TryGetValue(sessionKey, out var existingSession))
        {
            // Always emit session after 3-5 events to ensure flow
            if (existingSession.Events.Count >= _random.Next(3, 6))
            {
                // Session completed, create and return completed session
                var completedSession = CreateSession(existingSession);
                _sessionStates.Remove(sessionKey);

                // Start new session with current event
                var newSession = new SessionState
                {
                    UserId = userEvent.UserId,
                    SessionId = userEvent.SessionId,
                    StartTime = userEvent.Timestamp,
                    LastActivity = userEvent.Timestamp,
                    Events = new List<UserEvent> { userEvent },
                    DeviceType = userEvent.DeviceType,
                    Browser = userEvent.Browser,
                    OperatingSystem = userEvent.OperatingSystem,
                    Country = userEvent.Country,
                    City = userEvent.City,
                    EntryPage = userEvent.PageUrl,
                    ExitPage = userEvent.PageUrl,
                    Referrer = userEvent.ReferrerUrl,
                    ConversionValue = decimal.TryParse(userEvent.PropertyValue, out var value)
                        ? value
                        : 0m,
                };

                _sessionStates[sessionKey] = newSession;

                _logger.Log(LogLevel.Debug, "SessionWindowAssigner: Created session {SessionId} with {EventCount} events",
                    completedSession.SessionId, completedSession.Events.Count);

                return Task.FromResult(completedSession);
            }
            else
            {
                // Update existing session
                existingSession.LastActivity = userEvent.Timestamp;
                existingSession.Events.Add(userEvent);
                existingSession.ExitPage = userEvent.PageUrl;

                if (decimal.TryParse(userEvent.PropertyValue, out var value))
                    existingSession.ConversionValue += value;

                return Task.FromResult(CreateDummySession()); // Return dummy session to maintain flow
            }
        }

        {
            // Create new session
            var newSession = new SessionState
            {
                UserId = userEvent.UserId,
                SessionId = userEvent.SessionId,
                StartTime = userEvent.Timestamp,
                LastActivity = userEvent.Timestamp,
                Events = new List<UserEvent> { userEvent },
                DeviceType = userEvent.DeviceType,
                Browser = userEvent.Browser,
                OperatingSystem = userEvent.OperatingSystem,
                Country = userEvent.Country,
                City = userEvent.City,
                EntryPage = userEvent.PageUrl,
                ExitPage = userEvent.PageUrl,
                Referrer = userEvent.ReferrerUrl,
                ConversionValue = decimal.TryParse(userEvent.PropertyValue, out var value)
                    ? value
                    : 0m,
            };

            _sessionStates[sessionKey] = newSession;

            return Task.FromResult(CreateDummySession()); // Return dummy session to maintain flow
        }
    }

    /// <summary>
    ///     Called when pipeline processing is complete to emit any remaining sessions.
    /// </summary>
    public void CompleteProcessing()
    {
        foreach (var kvp in _sessionStates.ToList())
        {
            var session = CreateSession(kvp.Value);
            _emittedSessions.Enqueue(session);
            _sessionStates.Remove(kvp.Key);

            _logger.Log(LogLevel.Debug, "SessionWindowAssigner: Queued remaining session {SessionId} with {EventCount} events",
                session.SessionId, session.Events.Count);
        }

        _logger.Log(LogLevel.Information, "SessionWindowAssigner: Queued {Count} remaining sessions at pipeline end",
            _emittedSessions.Count);
    }

    private UserSession CreateDummySession()
    {
        return new UserSession(
            "dummy-session",
            "dummy-user",
            DateTime.UtcNow,
            DateTime.UtcNow,
            TimeSpan.Zero,
            0,
            new List<UserEvent>().AsReadOnly(),
            0,
            0,
            0.0,
            "Unknown",
            "Unknown",
            "Unknown",
            "Unknown",
            "Unknown",
            "Unknown",
            "Unknown",
            null,
            0m,
            false
        );
    }

    private UserSession CreateSession(SessionState sessionState)
    {
        var duration = sessionState.LastActivity - sessionState.StartTime;
        var uniquePages = sessionState.Events.Select(e => e.PageUrl).Distinct().Count();
        var hasConversion = sessionState.ConversionValue > 0;

        return new UserSession(
            sessionState.SessionId,
            sessionState.UserId,
            sessionState.StartTime,
            sessionState.LastActivity,
            duration,
            sessionState.Events.Count,
            sessionState.Events.AsReadOnly(),
            sessionState.Events.Count, // PageViews is same as EventCount for simplicity
            uniquePages,
            sessionState.Events.Count == 1
                ? 1.0
                : 0.0, // BounceRate: 1.0 if single event, 0.0 otherwise
            sessionState.DeviceType,
            sessionState.Browser,
            sessionState.OperatingSystem,
            sessionState.Country,
            sessionState.City,
            sessionState.EntryPage,
            sessionState.ExitPage,
            sessionState.Referrer,
            sessionState.ConversionValue,
            hasConversion
        );
    }
}

/// <summary>
///     Internal state for tracking user sessions during windowing.
/// </summary>
public class SessionState
{
    public string UserId { get; set; } = string.Empty;
    public string SessionId { get; set; } = string.Empty;
    public DateTime StartTime { get; set; }
    public DateTime LastActivity { get; set; }
    public List<UserEvent> Events { get; set; } = new();
    public string DeviceType { get; set; } = string.Empty;
    public string Browser { get; set; } = string.Empty;
    public string OperatingSystem { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string EntryPage { get; set; } = string.Empty;
    public string ExitPage { get; set; } = string.Empty;
    public string? Referrer { get; set; }
    public decimal ConversionValue { get; set; }
}

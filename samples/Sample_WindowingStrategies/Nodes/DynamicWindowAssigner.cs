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
///     Transform node that creates dynamic windows based on session characteristics.
///     This node implements adaptive windowing that adjusts window size based on activity patterns.
/// </summary>
public class DynamicWindowAssigner : TransformNode<UserSession, IReadOnlyList<UserSession>>
{
    private readonly double _activityThreshold;
    private readonly double _diversityThreshold;
    private readonly IPipelineLogger _logger;
    private readonly TimeSpan _maxWindowDuration;
    private readonly int _maxWindowSize;
    private readonly int _minWindowSize;
    private readonly List<UserSession> _sessionBuffer;
    private int _totalEventsInWindow;

    /// <summary>
    ///     Initializes a new instance of <see cref="DynamicWindowAssigner" /> class.
    /// </summary>
    /// <param name="minWindowSize">The minimum window size (number of sessions).</param>
    /// <param name="maxWindowSize">The maximum window size (number of sessions).</param>
    /// <param name="activityThreshold">The activity threshold for window expansion.</param>
    /// <param name="diversityThreshold">The diversity threshold for window expansion.</param>
    /// <param name="logger">The pipeline logger for logging operations.</param>
    public DynamicWindowAssigner(
        int minWindowSize = 5,
        int maxWindowSize = 25,
        double activityThreshold = 0.7,
        double diversityThreshold = 0.6,
        TimeSpan? maxWindowDuration = null,
        IPipelineLogger? logger = null)
    {
        _minWindowSize = minWindowSize;
        _maxWindowSize = maxWindowSize;
        _activityThreshold = activityThreshold;
        _diversityThreshold = diversityThreshold;
        _maxWindowDuration = maxWindowDuration ?? TimeSpan.FromMinutes(30);
        _logger = logger ?? NullPipelineLoggerFactory.Instance.CreateLogger(nameof(DynamicWindowAssigner));
        _sessionBuffer = [];
        _totalEventsInWindow = 0;
    }

    /// <summary>
    ///     Processes individual user sessions and creates dynamic windows based on activity and diversity.
    ///     This method implements adaptive windowing that adjusts size based on session characteristics.
    /// </summary>
    /// <param name="session">The individual user session to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A collection of sessions forming a dynamic window when conditions are met.</returns>
    public override Task<IReadOnlyList<UserSession>> ExecuteAsync(
        UserSession session,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (!IsRealSession(session))
            return Task.FromResult<IReadOnlyList<UserSession>>([]);

        _sessionBuffer.Add(session);
        _totalEventsInWindow += session.EventCount;

        // Check if we should emit a window
        if (ShouldEmitWindow())
        {
            var windowSessions = new List<UserSession>(_sessionBuffer);
            _sessionBuffer.Clear();
            _totalEventsInWindow = 0;

            _logger.Log(LogLevel.Debug, "DynamicWindowAssigner: Emitting window with {Count} sessions", windowSessions.Count);
            return Task.FromResult<IReadOnlyList<UserSession>>(windowSessions);
        }

        return Task.FromResult<IReadOnlyList<UserSession>>([]);
    }

    private bool ShouldEmitWindow()
    {
        if (_sessionBuffer.Count >= _maxWindowSize)
            return true;

        if (_sessionBuffer.Count < _minWindowSize)
            return false;

        var duration = CalculateWindowDuration();

        if (duration >= _maxWindowDuration)
            return true;

        // Calculate activity metrics
        var avgEventsPerSession = (double)_totalEventsInWindow / _sessionBuffer.Count;
        var activityScore = Math.Min(avgEventsPerSession / 10.0, 1.0); // Normalize to 0-1

        // Calculate diversity metrics
        var uniqueUsers = _sessionBuffer.Select(s => s.UserId).Distinct().Count();
        var uniqueCountries = _sessionBuffer.Select(s => s.Country).Distinct().Count();
        var uniqueDevices = _sessionBuffer.Select(s => s.DeviceType).Distinct().Count();
        var diversityScore = (uniqueUsers + uniqueCountries + uniqueDevices) / (3.0 * _sessionBuffer.Count);

        // Emit if activity or diversity thresholds are met
        return activityScore >= _activityThreshold || diversityScore >= _diversityThreshold;
    }

    private TimeSpan CalculateWindowDuration()
    {
        if (_sessionBuffer.Count == 0)
            return TimeSpan.Zero;

        var start = _sessionBuffer.Min(s => s.StartTime);
        var end = _sessionBuffer.Max(s => s.EndTime);
        return end - start;
    }

    private static bool IsRealSession(UserSession? session)
    {
        return session is not null
               && session.EventCount > 0
               && !string.IsNullOrEmpty(session.SessionId)
               && !session.SessionId.StartsWith("dummy", StringComparison.OrdinalIgnoreCase);
    }
}

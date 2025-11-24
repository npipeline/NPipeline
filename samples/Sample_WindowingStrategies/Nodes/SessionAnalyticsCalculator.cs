using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using Sample_WindowingStrategies.Models;

namespace Sample_WindowingStrategies.Nodes;

/// <summary>
///     Transform node that calculates comprehensive analytics for session windows.
///     This node processes collections of sessions and generates detailed metrics.
/// </summary>
public class SessionAnalyticsCalculator : TransformNode<IReadOnlyCollection<UserSession>, SessionMetrics>
{
    private readonly IPipelineLogger _logger;

    /// <summary>
    ///     Initializes a new instance of <see cref="SessionAnalyticsCalculator" /> class.
    /// </summary>
    /// <param name="logger">The pipeline logger for logging operations.</param>
    public SessionAnalyticsCalculator(IPipelineLogger? logger = null)
    {
        _logger = logger ?? NullPipelineLoggerFactory.Instance.CreateLogger(nameof(SessionAnalyticsCalculator));
    }

    /// <summary>
    ///     Processes collections of user sessions and calculates comprehensive analytics.
    ///     This method generates detailed metrics for session-based analytics.
    /// </summary>
    /// <param name="sessions">The collection of user sessions to analyze.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>Comprehensive session metrics for the analyzed sessions.</returns>
    public override Task<SessionMetrics> ExecuteAsync(
        IReadOnlyCollection<UserSession> sessions,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        if (sessions.Count == 0)
        {
            _logger.Log(LogLevel.Debug, "SessionAnalyticsCalculator: Received empty sessions collection");
            return Task.FromResult(CreateEmptyMetrics(stopwatch.Elapsed));
        }

        _logger.Log(LogLevel.Debug, "SessionAnalyticsCalculator: Processing {Count} sessions", sessions.Count);

        // Calculate basic metrics
        var metrics = CalculateBasicMetrics(sessions, stopwatch.Elapsed);

        stopwatch.Stop();

        _logger.Log(LogLevel.Information, "SessionAnalyticsCalculator: Processed {Count} sessions in {ElapsedMs}ms",
            sessions.Count, stopwatch.ElapsedMilliseconds);

        return Task.FromResult(metrics);
    }

    private SessionMetrics CalculateBasicMetrics(IReadOnlyCollection<UserSession> sessions, TimeSpan processingTime)
    {
        if (sessions.Count == 0)
            return CreateEmptyMetrics(processingTime);

        var windowId = $"window_{DateTime.UtcNow:yyyyMMddHHmmss}_{Guid.NewGuid():N}";
        var windowStartTime = sessions.Min(s => s.StartTime);
        var windowEndTime = sessions.Max(s => s.EndTime);
        var windowDuration = windowEndTime - windowStartTime;

        var sessionDurations = sessions.Select(s => s.Duration.TotalSeconds).ToList();
        var sessionValues = sessions.Select(s => s.ConversionValue).ToList();
        var eventCounts = sessions.Select(s => s.EventCount).ToList();

        var uniqueUsers = sessions.Select(s => s.UserId).Distinct().Count();
        var totalEvents = eventCounts.Sum();
        var averageSessionDuration = TimeSpan.FromSeconds(sessionDurations.Average());
        var averageEventsPerSession = eventCounts.Average();
        var bounceRate = sessions.Average(s => s.BounceRate);
        var conversionRate = (double)sessions.Count(s => s.HasConversion) / sessions.Count;
        var totalConversionValue = sessionValues.Sum();
        var averageConversionValue = sessionValues.Average();

        // Calculate top pages
        var pageViews = sessions.SelectMany(s => s.Events)
            .Where(e => e != null)
            .GroupBy(e => e.PageUrl)
            .Select(g => new PageMetric(g.Key, g.Count(), g.Count(), TimeSpan.Zero, 0.0, 0.0))
            .OrderByDescending(p => p.ViewCount)
            .Take(10)
            .ToList();

        // Calculate top event types
        var eventTypeCounts = sessions.SelectMany(s => s.Events)
            .Where(e => e != null)
            .GroupBy(e => e.EventType)
            .Select(g => new EventTypeMetric(g.Key, g.Count(), (double)g.Count() / totalEvents, 0.0))
            .OrderByDescending(e => e.Count)
            .Take(10)
            .ToList();

        // Calculate distributions
        var deviceDistribution = sessions.GroupBy(s => s.DeviceType)
            .ToDictionary(g => g.Key, g => g.Count());

        var browserDistribution = sessions.GroupBy(s => s.Browser)
            .ToDictionary(g => g.Key, g => g.Count());

        var geographicDistribution = sessions.GroupBy(s => s.Country)
            .ToDictionary(g => g.Key, g => g.Count());

        var peakActivityGroup = sessions.GroupBy(s => s.StartTime.Hour)
            .OrderByDescending(g => g.Count())
            .FirstOrDefault();

        var peakActivityTime = peakActivityGroup?.Key ?? 12; // Default to noon if no sessions

        var engagementScore = CalculateEngagementScore(sessions);
        var retentionRate = CalculateRetentionRate(sessions);
        var newUserRate = CalculateNewUserRate(sessions);
        var churnRisk = CalculateChurnRisk(sessions);

        return new SessionMetrics(
            windowId,
            windowStartTime,
            windowEndTime,
            windowDuration,
            sessions.Count,
            uniqueUsers,
            totalEvents,
            averageSessionDuration,
            averageEventsPerSession,
            bounceRate,
            conversionRate,
            totalConversionValue,
            averageConversionValue,
            pageViews,
            eventTypeCounts,
            deviceDistribution,
            browserDistribution,
            geographicDistribution,
            windowStartTime.AddHours(peakActivityTime),
            engagementScore,
            retentionRate,
            newUserRate,
            churnRisk,
            (long)processingTime.TotalMilliseconds
        );
    }

    private SessionMetrics CreateEmptyMetrics(TimeSpan processingTime)
    {
        var windowId = $"empty_window_{DateTime.UtcNow:yyyyMMddHHmmss}";
        var now = DateTime.UtcNow;

        return new SessionMetrics(
            windowId,
            now,
            now,
            TimeSpan.Zero,
            0,
            0,
            0,
            TimeSpan.Zero,
            0.0,
            0.0,
            0.0,
            0m,
            0m,
            new List<PageMetric>(),
            new List<EventTypeMetric>(),
            new Dictionary<string, int>(),
            new Dictionary<string, int>(),
            new Dictionary<string, int>(),
            now,
            0.0,
            0.0,
            0.0,
            0.0,
            (long)processingTime.TotalMilliseconds
        );
    }

    private double CalculateEngagementScore(IReadOnlyCollection<UserSession> sessions)
    {
        if (sessions.Count == 0)
            return 0.0;

        var avgDuration = sessions.Average(s => s.Duration.TotalMinutes);
        var avgEventsPerSession = sessions.Average(s => s.EventCount);
        var conversionRate = (double)sessions.Count(s => s.HasConversion) / sessions.Count;

        // Simple engagement score calculation
        return avgDuration / 30.0 * 0.4 + avgEventsPerSession / 10.0 * 0.3 + conversionRate * 0.3;
    }

    private double CalculateRetentionRate(IReadOnlyCollection<UserSession> sessions)
    {
        if (sessions.Count == 0)
            return 0.0;

        // For demo purposes, assume 30% of sessions are from returning users
        return 0.3;
    }

    private double CalculateNewUserRate(IReadOnlyCollection<UserSession> sessions)
    {
        if (sessions.Count == 0)
            return 0.0;

        // For demo purposes, assume 40% of sessions are from new users
        return 0.4;
    }

    private double CalculateChurnRisk(IReadOnlyCollection<UserSession> sessions)
    {
        if (sessions.Count == 0)
            return 0.0;

        var avgDuration = sessions.Average(s => s.Duration.TotalMinutes);
        var bounceRate = sessions.Average(s => s.BounceRate);

        // Simple churn risk calculation based on duration and bounce rate
        return Math.Max(0, 1.0 - avgDuration / 30.0 - bounceRate * 0.5);
    }

    private double CalculateStandardDeviation(IEnumerable<double> values)
    {
        if (!values.Any())
            return 0;

        var valuesList = values.ToList();
        var mean = valuesList.Average();
        var sumOfSquares = valuesList.Sum(x => Math.Pow(x - mean, 2));
        return Math.Sqrt(sumOfSquares / valuesList.Count);
    }

    private (double Mean, double StdDev) CalculateSessionDurationStats(IReadOnlyCollection<UserSession> sessions)
    {
        if (sessions == null || sessions.Count == 0)
            return (0, 0);

        var durations = sessions.Where(s => s != null)
            .Select(s => (s.EndTime - s.StartTime).TotalSeconds)
            .ToList();

        if (durations.Count == 0)
            return (0, 0);

        var mean = durations.Average();
        var variance = durations.Select(d => Math.Pow(d - mean, 2)).Average();
        var stdDev = Math.Sqrt(variance);

        return (mean, stdDev);
    }
}

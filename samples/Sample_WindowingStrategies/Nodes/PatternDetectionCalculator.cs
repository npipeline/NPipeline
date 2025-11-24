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
///     Transform node that detects patterns in user session collections.
///     This node implements advanced pattern detection algorithms for user behavior analysis.
/// </summary>
public class PatternDetectionCalculator : TransformNode<IReadOnlyCollection<UserSession>, PatternMatch>
{
    private readonly IPipelineLogger _logger;
    private readonly int _minPatternLength;
    private readonly int _minPatternSupport;
    private readonly double _patternConfidenceThreshold;

    /// <summary>
    ///     Initializes a new instance of <see cref="PatternDetectionCalculator" /> class.
    /// </summary>
    /// <param name="minPatternSupport">The minimum support for pattern detection.</param>
    /// <param name="minPatternLength">The minimum pattern length to detect.</param>
    /// <param name="patternConfidenceThreshold">The confidence threshold for pattern detection.</param>
    /// <param name="logger">The pipeline logger for logging operations.</param>
    public PatternDetectionCalculator(
        int minPatternSupport = 3,
        int minPatternLength = 2,
        double patternConfidenceThreshold = 0.7,
        IPipelineLogger? logger = null)
    {
        _minPatternSupport = minPatternSupport;
        _minPatternLength = minPatternLength;
        _patternConfidenceThreshold = patternConfidenceThreshold;
        _logger = logger ?? NullPipelineLoggerFactory.Instance.CreateLogger(nameof(PatternDetectionCalculator));
    }

    /// <summary>
    ///     Processes collections of user sessions and detects behavior patterns.
    ///     This method implements advanced pattern detection algorithms.
    /// </summary>
    /// <param name="sessions">The collection of user sessions to analyze.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>The most significant pattern detected in the sessions.</returns>
    public override Task<PatternMatch> ExecuteAsync(
        IReadOnlyCollection<UserSession> sessions,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        if (sessions.Count == 0)
        {
            _logger.Log(LogLevel.Debug, "PatternDetectionCalculator: Received empty sessions collection");
            return Task.FromResult(CreateEmptyPattern(stopwatch.Elapsed));
        }

        _logger.Log(LogLevel.Debug, "PatternDetectionCalculator: Processing {Count} sessions", sessions.Count);

        // Detect different types of patterns
        var patterns = new List<PatternMatch>
        {
            DetectHighValuePattern(sessions, stopwatch.Elapsed),
            DetectBouncePattern(sessions, stopwatch.Elapsed),
            DetectGeographicPattern(sessions, stopwatch.Elapsed),
            DetectDevicePattern(sessions, stopwatch.Elapsed),
            DetectTemporalPattern(sessions, stopwatch.Elapsed),
        };

        // Return pattern with highest confidence that meets threshold
        var bestPattern = patterns
            .Where(p => p.ConfidenceScore >= _patternConfidenceThreshold)
            .OrderByDescending(p => p.ConfidenceScore)
            .FirstOrDefault();

        if (bestPattern == null)
        {
            _logger.Log(LogLevel.Debug, "PatternDetectionCalculator: No patterns found with confidence >= {Threshold}", _patternConfidenceThreshold);
            return Task.FromResult(CreateEmptyPattern(stopwatch.Elapsed));
        }

        stopwatch.Stop();

        _logger.Log(LogLevel.Information, "PatternDetectionCalculator: Detected {PatternType} pattern with confidence {Confidence:F2} in {ElapsedMs}ms",
            bestPattern.PatternType, bestPattern.ConfidenceScore, stopwatch.ElapsedMilliseconds);

        return Task.FromResult(bestPattern);
    }

    private PatternMatch DetectHighValuePattern(IReadOnlyCollection<UserSession> sessions, TimeSpan processingTime)
    {
        if (sessions == null || sessions.Count == 0)
            return CreateEmptyPattern(processingTime);

        var highValueThreshold = 50m; // Lower threshold for demo data
        var highValueSessions = sessions.Where(s => s.ConversionValue >= highValueThreshold).ToList();

        if (highValueSessions.Count < _minPatternSupport)
            return CreateEmptyPattern(processingTime);

        var avgValue = highValueSessions.Average(s => s.ConversionValue);
        var confidence = (double)highValueSessions.Count / sessions.Count;

        var windowId = $"window_{DateTime.UtcNow:yyyyMMddHHmmss}";
        var detectionTime = DateTime.UtcNow;

        return new PatternMatch(
            Guid.NewGuid().ToString(),
            "HighValueConversion",
            "Behavioral",
            windowId,
            detectionTime,
            confidence,
            confidence >= 0.8
                ? "High"
                : "Medium",
            $"High-value conversion pattern detected: {highValueSessions.Count} sessions with avg value {avgValue:C}",
            highValueSessions.Select(s => s.UserId).ToList(),
            highValueSessions.Select(s => s.SessionId).ToList(),
            highValueSessions.SelectMany(s => s.Events).ToList(),
            CreatePatternSteps(highValueSessions),
            new TimeConstraint(null, null, null, null, null),
            (double)highValueSessions.Count / sessions.Count,
            confidence * 10, // Impact score
            new List<string> { "Monitor high-value users", "Provide premium support" },
            new Dictionary<string, object>
            {
                ["AverageValue"] = avgValue,
                ["Threshold"] = highValueThreshold,
                ["TopCountry"] = highValueSessions.GroupBy(s => s.Country).OrderByDescending(g => g.Count()).FirstOrDefault()?.Key ?? "Unknown",
                ["TopDevice"] = highValueSessions.GroupBy(s => s.DeviceType).OrderByDescending(g => g.Count()).FirstOrDefault()?.Key ?? "Unknown",
            },
            (long)processingTime.TotalMilliseconds
        );
    }

    private PatternMatch DetectBouncePattern(IReadOnlyCollection<UserSession> sessions, TimeSpan processingTime)
    {
        if (sessions == null || sessions.Count == 0)
            return CreateEmptyPattern(processingTime);

        var bounceSessions = sessions.Where(s => s.EventCount == 1).ToList();

        if (bounceSessions.Count < _minPatternSupport)
            return CreateEmptyPattern(processingTime);

        var bounceRate = (double)bounceSessions.Count / sessions.Count;

        var confidence = bounceRate >= 0.5
            ? bounceRate
            : 0.0;

        var windowId = $"window_{DateTime.UtcNow:yyyyMMddHHmmss}";
        var detectionTime = DateTime.UtcNow;

        return new PatternMatch(
            Guid.NewGuid().ToString(),
            "HighBounceRate",
            "Behavioral",
            windowId,
            detectionTime,
            confidence,
            confidence >= 0.7
                ? "High"
                : "Medium",
            $"High bounce rate pattern detected: {bounceRate:P2} of sessions bounced",
            bounceSessions.Select(s => s.UserId).ToList(),
            bounceSessions.Select(s => s.SessionId).ToList(),
            bounceSessions.SelectMany(s => s.Events).ToList(),
            CreatePatternSteps(bounceSessions),
            new TimeConstraint(null, null, null, null, null),
            bounceRate,
            bounceRate * 5, // Impact score
            new List<string> { "Improve landing pages", "Optimize page load times" },
            new Dictionary<string, object>
            {
                ["BounceRate"] = bounceRate,
                ["Threshold"] = 0.5,
                ["TopCountry"] = bounceSessions.GroupBy(s => s.Country).OrderByDescending(g => g.Count()).FirstOrDefault()?.Key ?? "Unknown",
                ["TopDevice"] = bounceSessions.GroupBy(s => s.DeviceType).OrderByDescending(g => g.Count()).FirstOrDefault()?.Key ?? "Unknown",
            },
            (long)processingTime.TotalMilliseconds
        );
    }

    private PatternMatch DetectGeographicPattern(IReadOnlyCollection<UserSession> sessions, TimeSpan processingTime)
    {
        if (sessions == null || sessions.Count == 0)
            return CreateEmptyPattern(processingTime);

        var countryGroups = sessions.GroupBy(s => s.Country).ToList();
        var dominantCountry = countryGroups.OrderByDescending(g => g.Count()).FirstOrDefault();

        if (dominantCountry == null || dominantCountry.Count() < _minPatternSupport)
            return CreateEmptyPattern(processingTime);

        var countryConcentration = (double)dominantCountry.Count() / sessions.Count;

        var confidence = countryConcentration >= 0.4
            ? countryConcentration
            : 0.0;

        var windowId = $"window_{DateTime.UtcNow:yyyyMMddHHmmss}";
        var detectionTime = DateTime.UtcNow;

        return new PatternMatch(
            Guid.NewGuid().ToString(),
            "GeographicConcentration",
            "Geographic",
            windowId,
            detectionTime,
            confidence,
            confidence >= 0.6
                ? "High"
                : "Medium",
            $"Geographic concentration pattern: {countryConcentration:P2} of sessions from {dominantCountry.Key}",
            dominantCountry.Select(s => s.UserId).ToList(),
            dominantCountry.Select(s => s.SessionId).ToList(),
            dominantCountry.SelectMany(s => s.Events).ToList(),
            CreatePatternSteps(dominantCountry.ToList()),
            new TimeConstraint(null, null, null, null, null),
            countryConcentration,
            countryConcentration * 3, // Impact score
            new List<string> { "Target regional campaigns", "Localize content" },
            new Dictionary<string, object>
            {
                ["Country"] = dominantCountry.Key,
                ["Concentration"] = countryConcentration,
                ["Threshold"] = 0.4,
                ["AvgSessionDuration"] = dominantCountry.Average(s => s.Duration.TotalMinutes),
                ["ConversionRate"] = (double)dominantCountry.Count(s => s.HasConversion) / dominantCountry.Count(),
            },
            (long)processingTime.TotalMilliseconds
        );
    }

    private PatternMatch DetectDevicePattern(IReadOnlyCollection<UserSession> sessions, TimeSpan processingTime)
    {
        if (sessions == null || sessions.Count == 0)
            return CreateEmptyPattern(processingTime);

        var deviceGroups = sessions.GroupBy(s => s.DeviceType).ToList();
        var dominantDevice = deviceGroups.OrderByDescending(g => g.Count()).FirstOrDefault();

        if (dominantDevice == null || dominantDevice.Count() < _minPatternSupport)
            return CreateEmptyPattern(processingTime);

        var deviceConcentration = (double)dominantDevice.Count() / sessions.Count;

        var confidence = deviceConcentration >= 0.3
            ? deviceConcentration
            : 0.0;

        var windowId = $"window_{DateTime.UtcNow:yyyyMMddHHmmss}";
        var detectionTime = DateTime.UtcNow;

        return new PatternMatch(
            Guid.NewGuid().ToString(),
            "DevicePreference",
            "Device",
            windowId,
            detectionTime,
            confidence,
            confidence >= 0.5
                ? "High"
                : "Medium",
            $"Device preference pattern: {deviceConcentration:P2} of sessions on {dominantDevice.Key}",
            dominantDevice.Select(s => s.UserId).ToList(),
            dominantDevice.Select(s => s.SessionId).ToList(),
            dominantDevice.SelectMany(s => s.Events).ToList(),
            CreatePatternSteps(dominantDevice.ToList()),
            new TimeConstraint(null, null, null, null, null),
            deviceConcentration,
            deviceConcentration * 2, // Impact score
            new List<string> { "Optimize for device", "Test device-specific features" },
            new Dictionary<string, object>
            {
                ["DeviceType"] = dominantDevice.Key,
                ["Concentration"] = deviceConcentration,
                ["Threshold"] = 0.3,
                ["AvgSessionDuration"] = dominantDevice.Average(s => s.Duration.TotalMinutes),
                ["ConversionRate"] = (double)dominantDevice.Count(s => s.HasConversion) / dominantDevice.Count(),
            },
            (long)processingTime.TotalMilliseconds
        );
    }

    private PatternMatch DetectTemporalPattern(IReadOnlyCollection<UserSession> sessions, TimeSpan processingTime)
    {
        if (sessions == null || sessions.Count == 0)
            return CreateEmptyPattern(processingTime);

        var hourGroups = sessions.GroupBy(s => s.StartTime.Hour).ToList();
        var peakHour = hourGroups.OrderByDescending(g => g.Count()).FirstOrDefault();

        if (peakHour == null || peakHour.Count() < _minPatternSupport)
            return CreateEmptyPattern(processingTime);

        var hourConcentration = (double)peakHour.Count() / sessions.Count;

        var confidence = hourConcentration >= 0.25
            ? hourConcentration
            : 0.0;

        var windowId = $"window_{DateTime.UtcNow:yyyyMMddHHmmss}";
        var detectionTime = DateTime.UtcNow;

        return new PatternMatch(
            Guid.NewGuid().ToString(),
            "TemporalActivity",
            "Temporal",
            windowId,
            detectionTime,
            confidence,
            confidence >= 0.4
                ? "High"
                : "Medium",
            $"Temporal activity pattern: {hourConcentration:P2} of sessions at hour {peakHour.Key}",
            peakHour.Select(s => s.UserId).ToList(),
            peakHour.Select(s => s.SessionId).ToList(),
            peakHour.SelectMany(s => s.Events).ToList(),
            CreatePatternSteps(peakHour.ToList()),
            new TimeConstraint(null, null, null, null, null),
            hourConcentration,
            hourConcentration * 1.5, // Impact score
            new List<string> { "Schedule campaigns for peak hours", "Adjust server capacity" },
            new Dictionary<string, object>
            {
                ["PeakHour"] = peakHour.Key,
                ["Concentration"] = hourConcentration,
                ["Threshold"] = 0.25,
                ["AvgSessionDuration"] = peakHour.Average(s => s.Duration.TotalMinutes),
                ["ConversionRate"] = (double)peakHour.Count(s => s.HasConversion) / peakHour.Count(),
            },
            (long)processingTime.TotalMilliseconds
        );
    }

    private IReadOnlyList<PatternStep> CreatePatternSteps(IReadOnlyCollection<UserSession> sessions)
    {
        var steps = new List<PatternStep>();
        var stepNumber = 0;

        // Create pattern steps based on common event sequences
        var commonEvents = sessions
            .SelectMany(s => s.Events)
            .GroupBy(e => e.EventType)
            .OrderByDescending(g => g.Count())
            .Take(5)
            .ToList();

        foreach (var eventGroup in commonEvents)
        {
            steps.Add(new PatternStep(
                stepNumber++,
                eventGroup.Key,
                null, // PageUrl
                null, // PropertyValue
                null, // TimeFromPreviousStep
                false, // IsOptional
                new Dictionary<string, object>() // Conditions
            ));
        }

        return steps;
    }

    private PatternMatch CreateEmptyPattern(TimeSpan processingTime)
    {
        var windowId = $"empty_window_{DateTime.UtcNow:yyyyMMddHHmmss}";
        var detectionTime = DateTime.UtcNow;

        return new PatternMatch(
            Guid.NewGuid().ToString(),
            "NoPattern",
            "None",
            windowId,
            detectionTime,
            0.0,
            "Low",
            "No significant pattern detected",
            new List<string>(),
            new List<string>(),
            new List<UserEvent>(),
            new List<PatternStep>(),
            new TimeConstraint(null, null, null, null, null),
            0.0,
            0.0,
            new List<string>(),
            new Dictionary<string, object>(),
            (long)processingTime.TotalMilliseconds
        );
    }
}

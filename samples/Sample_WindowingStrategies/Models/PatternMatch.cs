using System;
using System.Collections.Generic;

namespace Sample_WindowingStrategies.Models;

/// <summary>
///     Represents a detected user behavior pattern within a time window.
///     This model demonstrates pattern detection and matching in user analytics.
/// </summary>
/// <param name="PatternId">The unique identifier for this detected pattern.</param>
/// <param name="PatternName">The name of the detected pattern.</param>
/// <param name="PatternType">The type of pattern (Sequential, Temporal, Behavioral, etc.).</param>
/// <param name="WindowId">The analytics window where this pattern was detected.</param>
/// <param name="DetectionTime">When this pattern was detected.</param>
/// <param name="ConfidenceScore">The confidence score of this pattern detection (0-1).</param>
/// <param name="Severity">The severity or importance level of this pattern.</param>
/// <param name="Description">A description of what this pattern represents.</param>
/// <param name="AffectedUsers">The list of users exhibiting this pattern.</param>
/// <param name="AffectedSessions">The list of sessions where this pattern was detected.</param>
/// <param name="TriggerEvents">The events that triggered this pattern detection.</param>
/// <param name="PatternSequence">The sequence of events that constitute this pattern.</param>
/// <param name="TimeConstraints">The time constraints of this pattern.</param>
/// <param name="Frequency">How frequently this pattern occurs.</param>
/// <param name="ImpactScore">The business impact score of this pattern.</param>
/// <param name="RecommendedActions">Recommended actions based on this pattern.</param>
/// <param name="Metadata">Additional metadata about this pattern.</param>
/// <param name="ProcessingTimeMs">Time taken to detect this pattern.</param>
public record PatternMatch(
    string PatternId,
    string PatternName,
    string PatternType,
    string WindowId,
    DateTime DetectionTime,
    double ConfidenceScore,
    string Severity,
    string Description,
    IReadOnlyList<string> AffectedUsers,
    IReadOnlyList<string> AffectedSessions,
    IReadOnlyList<UserEvent> TriggerEvents,
    IReadOnlyList<PatternStep> PatternSequence,
    TimeConstraint TimeConstraints,
    double Frequency,
    double ImpactScore,
    IReadOnlyList<string> RecommendedActions,
    IReadOnlyDictionary<string, object> Metadata,
    long ProcessingTimeMs);

/// <summary>
///     Represents a single step in a detected pattern sequence.
/// </summary>
/// <param name="StepOrder">The order of this step in the pattern.</param>
/// <param name="EventType">The expected event type for this step.</param>
/// <param name="PageUrl">The expected page URL for this step (optional).</param>
/// <param name="PropertyValue">The expected property value for this step (optional).</param>
/// <param name="TimeFromPreviousStep">The expected time from the previous step.</param>
/// <param name="IsOptional">Whether this step is optional in the pattern.</param>
/// <param name="Conditions">Additional conditions for this step.</param>
public record PatternStep(
    int StepOrder,
    string EventType,
    string? PageUrl,
    string? PropertyValue,
    TimeSpan? TimeFromPreviousStep,
    bool IsOptional,
    IReadOnlyDictionary<string, object> Conditions);

/// <summary>
///     Represents time constraints for pattern detection.
/// </summary>
/// <param name="MinPatternDuration">The minimum duration for the pattern.</param>
/// <param name="MaxPatternDuration">The maximum duration for the pattern.</param>
/// <param name="MaxGapBetweenEvents">The maximum allowed gap between events in the pattern.</param>
/// <param name="TimeOfDayConstraints">Time of day constraints for the pattern.</param>
/// <param name="DayOfWeekConstraints">Day of week constraints for the pattern.</param>
public record TimeConstraint(
    TimeSpan? MinPatternDuration,
    TimeSpan? MaxPatternDuration,
    TimeSpan? MaxGapBetweenEvents,
    TimeRange? TimeOfDayConstraints,
    IReadOnlyList<DayOfWeek>? DayOfWeekConstraints);

/// <summary>
///     Represents a time range constraint.
/// </summary>
/// <param name="StartTime">The start time of the range.</param>
/// <param name="EndTime">The end time of the range.</param>
public record TimeRange(
    TimeSpan StartTime,
    TimeSpan EndTime);

/// <summary>
///     Represents pattern detection statistics for a window.
/// </summary>
/// <param name="WindowId">The analytics window ID.</param>
/// <param name="TotalPatternsDetected">The total number of patterns detected.</param>
/// <param name="PatternsByType">The count of patterns grouped by type.</param>
/// <param name="PatternsBySeverity">The count of patterns grouped by severity.</param>
/// <param name="AverageConfidenceScore">The average confidence score across all patterns.</param>
/// <param name="HighImpactPatterns">The number of high-impact patterns detected.</param>
/// <param name="ProcessingTimeMs">Total time spent on pattern detection.</param>
/// <param name="TopPatterns">The top patterns by impact score.</param>
public record PatternDetectionStats(
    string WindowId,
    int TotalPatternsDetected,
    IReadOnlyDictionary<string, int> PatternsByType,
    IReadOnlyDictionary<string, int> PatternsBySeverity,
    double AverageConfidenceScore,
    int HighImpactPatterns,
    long ProcessingTimeMs,
    IReadOnlyList<PatternMatch> TopPatterns);

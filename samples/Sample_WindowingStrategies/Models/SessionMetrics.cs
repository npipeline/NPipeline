using System;
using System.Collections.Generic;

namespace Sample_WindowingStrategies.Models;

/// <summary>
///     Represents analytics metrics calculated for user sessions within a time window.
///     This model demonstrates window-based analytics for user behavior patterns.
/// </summary>
/// <param name="WindowId">The unique identifier for this analytics window.</param>
/// <param name="WindowStartTime">When this analytics window started.</param>
/// <param name="WindowEndTime">When this analytics window ended.</param>
/// <param name="WindowDuration">The duration of this analytics window.</param>
/// <param name="SessionCount">The number of sessions in this window.</param>
/// <param name="UniqueUserCount">The number of unique users in this window.</param>
/// <param name="TotalEvents">The total number of events across all sessions.</param>
/// <param name="AverageSessionDuration">The average duration of sessions in this window.</param>
/// <param name="AverageEventsPerSession">The average number of events per session.</param>
/// <param name="BounceRate">The bounce rate for sessions in this window.</param>
/// <param name="ConversionRate">The conversion rate for sessions in this window.</param>
/// <param name="TotalConversionValue">The total conversion value across all sessions.</param>
/// <param name="AverageConversionValue">The average conversion value per converting session.</param>
/// <param name="TopPages">The most visited pages in this window.</param>
/// <param name="TopEventTypes">The most common event types in this window.</param>
/// <param name="DeviceDistribution">The distribution of device types.</param>
/// <param name="BrowserDistribution">The distribution of browsers.</param>
/// <param name="GeographicDistribution">The distribution of users by country.</param>
/// <param name="PeakActivityTime">The time of peak activity within this window.</param>
/// <param name="EngagementScore">The overall engagement score for this window.</param>
/// <param name="RetentionRate">The retention rate for returning users.</param>
/// <param name="NewUserRate">The rate of new users in this window.</param>
/// <param name="ChurnRisk">The churn risk assessment for this window.</param>
/// <param name="ProcessingTimeMs">Time taken to calculate these metrics.</param>
public record SessionMetrics(
    string WindowId,
    DateTime WindowStartTime,
    DateTime WindowEndTime,
    TimeSpan WindowDuration,
    int SessionCount,
    int UniqueUserCount,
    int TotalEvents,
    TimeSpan AverageSessionDuration,
    double AverageEventsPerSession,
    double BounceRate,
    double ConversionRate,
    decimal TotalConversionValue,
    decimal AverageConversionValue,
    IReadOnlyList<PageMetric> TopPages,
    IReadOnlyList<EventTypeMetric> TopEventTypes,
    IReadOnlyDictionary<string, int> DeviceDistribution,
    IReadOnlyDictionary<string, int> BrowserDistribution,
    IReadOnlyDictionary<string, int> GeographicDistribution,
    DateTime PeakActivityTime,
    double EngagementScore,
    double RetentionRate,
    double NewUserRate,
    double ChurnRisk,
    long ProcessingTimeMs);

/// <summary>
///     Represents page-specific metrics within a session window.
/// </summary>
/// <param name="PageUrl">The URL of the page.</param>
/// <param name="ViewCount">The number of views for this page.</param>
/// <param name="UniqueViews">The number of unique views for this page.</param>
/// <param name="AverageTimeOnPage">The average time spent on this page.</param>
/// <param name="BounceRate">The bounce rate for this page.</param>
/// <param name="ConversionRate">The conversion rate for this page.</param>
public record PageMetric(
    string PageUrl,
    int ViewCount,
    int UniqueViews,
    TimeSpan AverageTimeOnPage,
    double BounceRate,
    double ConversionRate);

/// <summary>
///     Represents event type metrics within a session window.
/// </summary>
/// <param name="EventType">The type of event.</param>
/// <param name="Count">The number of occurrences of this event type.</param>
/// <param name="Frequency">The frequency of this event type relative to all events.</param>
/// <param name="ConversionImpact">The impact of this event type on conversions.</param>
public record EventTypeMetric(
    string EventType,
    int Count,
    double Frequency,
    double ConversionImpact);

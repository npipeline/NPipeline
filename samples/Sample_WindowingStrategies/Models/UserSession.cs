using System;
using System.Collections.Generic;

namespace Sample_WindowingStrategies.Models;

/// <summary>
///     Represents a user session containing multiple events within a time window.
///     This model demonstrates session-based windowing for user behavior analysis.
/// </summary>
/// <param name="SessionId">The unique identifier for this session.</param>
/// <param name="UserId">The unique identifier of the user.</param>
/// <param name="StartTime">When the session started.</param>
/// <param name="EndTime">When the session ended.</param>
/// <param name="Duration">The total duration of the session.</param>
/// <param name="EventCount">The number of events in this session.</param>
/// <param name="Events">The collection of events in this session.</param>
/// <param name="PageViews">The number of page views in this session.</param>
/// <param name="UniquePages">The number of unique pages visited.</param>
/// <param name="BounceRate">The bounce rate (single-page sessions / total sessions).</param>
/// <param name="DeviceType">The primary device type used in this session.</param>
/// <param name="Browser">The primary browser used in this session.</param>
/// <param name="OperatingSystem">The primary operating system used in this session.</param>
/// <param name="Country">The country where the session originated.</param>
/// <param name="City">The city where the session originated.</param>
/// <param name="EntryPage">The first page visited in this session.</param>
/// <param name="ExitPage">The last page visited in this session.</param>
/// <param name="Referrer">The referrer that brought the user to this session.</param>
/// <param name="ConversionValue">The total conversion value for this session.</param>
/// <param name="HasConversion">Whether this session resulted in a conversion.</param>
public record UserSession(
    string SessionId,
    string UserId,
    DateTime StartTime,
    DateTime EndTime,
    TimeSpan Duration,
    int EventCount,
    IReadOnlyList<UserEvent> Events,
    int PageViews,
    int UniquePages,
    double BounceRate,
    string DeviceType,
    string Browser,
    string OperatingSystem,
    string Country,
    string City,
    string EntryPage,
    string ExitPage,
    string? Referrer,
    decimal ConversionValue,
    bool HasConversion);

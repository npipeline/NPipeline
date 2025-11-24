using System;
using System.Collections.Generic;

namespace Sample_WindowingStrategies.Models;

/// <summary>
///     Represents a user event in the analytics platform.
///     This model demonstrates individual user interactions that will be windowed for session analysis.
/// </summary>
/// <param name="EventId">The unique identifier for this user event.</param>
/// <param name="UserId">The unique identifier of the user who performed the action.</param>
/// <param name="SessionId">The session identifier this event belongs to.</param>
/// <param name="Timestamp">When the event occurred.</param>
/// <param name="EventType">The type of user action (Click, View, Purchase, Search, etc.).</param>
/// <param name="PageUrl">The URL or page where the event occurred.</param>
/// <param name="ReferrerUrl">The referrer URL that led to this event.</param>
/// <param name="UserAgent">The user agent string of the client.</param>
/// <param name="IpAddress">The IP address of the user.</param>
/// <param name="DeviceType">The type of device used (Desktop, Mobile, Tablet).</param>
/// <param name="Browser">The browser used (Chrome, Firefox, Safari, etc.).</param>
/// <param name="OperatingSystem">The operating system of the device.</param>
/// <param name="Country">The country where the event originated.</param>
/// <param name="City">The city where the event originated.</param>
/// <param name="PropertyValue">Optional property value associated with the event.</param>
/// <param name="Metadata">Additional metadata about the event.</param>
public record UserEvent(
    string EventId,
    string UserId,
    string SessionId,
    DateTime Timestamp,
    string EventType,
    string PageUrl,
    string? ReferrerUrl,
    string UserAgent,
    string IpAddress,
    string DeviceType,
    string Browser,
    string OperatingSystem,
    string Country,
    string City,
    string? PropertyValue,
    IReadOnlyDictionary<string, object> Metadata);

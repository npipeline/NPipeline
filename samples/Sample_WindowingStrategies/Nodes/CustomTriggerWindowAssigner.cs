using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using Sample_WindowingStrategies.Models;

namespace Sample_WindowingStrategies.Nodes;

/// <summary>
///     Transform node that creates windows based on custom trigger conditions.
///     This node implements event-based and condition-based windowing for complex scenarios.
/// </summary>
public class CustomTriggerWindowAssigner : TransformNode<UserSession, IReadOnlyList<UserSession>>
{
    private readonly int _conversionTrigger;
    private readonly decimal _highValueTrigger;
    private readonly ILogger _logger;
    private readonly List<UserSession> _sessionBuffer;
    private readonly TimeSpan _timeInterval;
    private DateTime _sessionStartTime;
    private int _totalConversions;
    private decimal _totalValueInWindow;

    /// <summary>
    ///     Initializes a new instance of <see cref="CustomTriggerWindowAssigner" /> class.
    /// </summary>
    /// <param name="conversionTrigger">The number of conversions to trigger window emission.</param>
    /// <param name="highValueTrigger">The total conversion value to trigger window emission.</param>
    /// <param name="timeInterval">The maximum time interval for window emission.</param>
    /// <param name="logger">The pipeline logger for logging operations.</param>
    public CustomTriggerWindowAssigner(
        int conversionTrigger = 3,
        decimal highValueTrigger = 500m,
        TimeSpan? timeInterval = null,
        ILogger? logger = null)
    {
        _conversionTrigger = conversionTrigger;
        _highValueTrigger = highValueTrigger;
        _timeInterval = timeInterval ?? TimeSpan.FromMinutes(15);
        _logger = logger ?? NullLoggerFactory.Instance.CreateLogger(nameof(CustomTriggerWindowAssigner));
        _sessionBuffer = [];
        _sessionStartTime = default;
        _totalConversions = 0;
        _totalValueInWindow = 0m;
    }

    /// <summary>
    ///     Processes individual user sessions and creates windows based on custom triggers.
    ///     This method implements complex trigger conditions for window emission.
    /// </summary>
    /// <param name="session">The individual user session to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A collection of sessions forming a custom-triggered window when conditions are met.</returns>
    public override Task<IReadOnlyList<UserSession>> ExecuteAsync(
        UserSession session,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (!IsRealSession(session))
            return Task.FromResult<IReadOnlyList<UserSession>>([]);

        _sessionBuffer.Add(session);

        if (_sessionBuffer.Count == 1)
            _sessionStartTime = session.StartTime;

        if (session.HasConversion)
            _totalConversions++;

        _totalValueInWindow += session.ConversionValue;

        // Check trigger conditions
        var triggerResult = EvaluateTriggerConditions();

        if (triggerResult.ShouldTrigger)
        {
            var windowSessions = new List<UserSession>(_sessionBuffer);
            ResetWindow();

            _logger.Log(LogLevel.Debug, "CustomTriggerWindowAssigner: Triggered window with {Count} sessions due to {TriggerType}",
                windowSessions.Count, triggerResult.TriggerType);

            return Task.FromResult<IReadOnlyList<UserSession>>(windowSessions);
        }

        return Task.FromResult<IReadOnlyList<UserSession>>([]);
    }

    private TriggerResult EvaluateTriggerConditions()
    {
        var now = DateTime.UtcNow;

        var sessionWindowStart = _sessionStartTime == default
            ? now
            : _sessionStartTime;

        var timeElapsed = now - sessionWindowStart;

        // Conversion trigger
        if (_totalConversions >= _conversionTrigger)
            return new TriggerResult(true, "ConversionThreshold");

        // High-value trigger
        if (_totalValueInWindow >= _highValueTrigger)
            return new TriggerResult(true, "HighValueThreshold");

        // Time interval trigger
        return timeElapsed >= _timeInterval
            ? new TriggerResult(true, "TimeInterval")
            : new TriggerResult(false, "None");
    }

    private void ResetWindow()
    {
        _sessionBuffer.Clear();
        _sessionStartTime = default;
        _totalConversions = 0;
        _totalValueInWindow = 0m;
    }

    private static bool IsRealSession(UserSession? session)
    {
        return session is not null
               && session.EventCount > 0
               && !string.IsNullOrEmpty(session.SessionId)
               && !session.SessionId.StartsWith("dummy", StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
///     Represents the result of trigger condition evaluation.
/// </summary>
public record TriggerResult(bool ShouldTrigger, string TriggerType);

/// <summary>
///     Represents the type of trigger for window emission.
/// </summary>
public enum TriggerType
{
    None,
    ConversionThreshold,
    HighValueThreshold,
    TimeInterval,
    CustomCondition,
}

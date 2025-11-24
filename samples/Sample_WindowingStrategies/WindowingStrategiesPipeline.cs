using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_WindowingStrategies.Models;
using Sample_WindowingStrategies.Nodes;

namespace Sample_WindowingStrategies;

/// <summary>
///     Simple transform node that converts individual UserSession to IReadOnlyCollection
///     <UserSession>
///         .
///         Filters out dummy sessions and wraps real sessions in collections.
/// </summary>
public class SessionToCollectionNode : TransformNode<UserSession, IReadOnlyCollection<UserSession>>
{
    public override Task<IReadOnlyCollection<UserSession>> ExecuteAsync(
        UserSession session,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (session is null || !IsRealSession(session))
            return Task.FromResult<IReadOnlyCollection<UserSession>>([]);

        return Task.FromResult<IReadOnlyCollection<UserSession>>([session]);
    }

    private static bool IsRealSession(UserSession session)
    {
        return !string.IsNullOrEmpty(session.SessionId)
               && !session.SessionId.StartsWith("dummy", StringComparison.OrdinalIgnoreCase)
               && session.EventCount > 0;
    }
}

/// <summary>
///     Transform node that converts IReadOnlyList
///     <UserSession>
///         to IReadOnlyCollection
///         <UserSession>
///             .
///             Used for type compatibility between window assigners and analytics calculators.
/// </summary>
public class DynamicListToCollectionNode : TransformNode<IReadOnlyList<UserSession>, IReadOnlyCollection<UserSession>>
{
    public override Task<IReadOnlyCollection<UserSession>> ExecuteAsync(
        IReadOnlyList<UserSession> sessions,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (sessions is null || sessions.Count == 0)
            return Task.FromResult<IReadOnlyCollection<UserSession>>([]);

        var filteredSessions = sessions
            .Where(s => s is not null && s.EventCount > 0 && !s.SessionId.StartsWith("dummy", StringComparison.OrdinalIgnoreCase))
            .ToList();

        return Task.FromResult<IReadOnlyCollection<UserSession>>(filteredSessions);
    }
}

/// <summary>
///     Transform node that converts IReadOnlyList
///     <UserSession>
///         to IReadOnlyCollection
///         <UserSession>
///             .
///             Used for type compatibility between custom trigger window assigners and analytics calculators.
/// </summary>
public class CustomTriggerListToCollectionNode : TransformNode<IReadOnlyList<UserSession>, IReadOnlyCollection<UserSession>>
{
    public override Task<IReadOnlyCollection<UserSession>> ExecuteAsync(
        IReadOnlyList<UserSession> sessions,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (sessions is null || sessions.Count == 0)
            return Task.FromResult<IReadOnlyCollection<UserSession>>([]);

        var filteredSessions = sessions
            .Where(s => s is not null && s.EventCount > 0 && !s.SessionId.StartsWith("dummy", StringComparison.OrdinalIgnoreCase))
            .ToList();

        return Task.FromResult<IReadOnlyCollection<UserSession>>(filteredSessions);
    }
}

/// <summary>
///     Advanced windowing strategies pipeline demonstrating multiple windowing approaches for user analytics.
///     This pipeline showcases sophisticated windowing techniques beyond basic tumbling and sliding windows.
/// </summary>
/// <remarks>
///     This implementation demonstrates three advanced windowing strategies:
///     1. Session-based windowing: Groups events by user sessions with custom timeouts
///     2. Dynamic windowing: Adapts window size based on data characteristics and patterns
///     3. Custom trigger windowing: Uses complex conditions to determine window boundaries
///     The pipeline flow:
///     UserEventSource -> SessionWindowAssigner -> [Parallel Processing Paths] -> Analytics & Pattern Detection -> UserBehaviorSink
///     Parallel processing paths:
///     - Path 1: SessionWindowAssigner -> SessionAnalyticsCalculator
///     - Path 2: SessionWindowAssigner -> DynamicWindowAssigner -> SessionAnalyticsCalculator
///     - Path 3: SessionWindowAssigner -> CustomTriggerWindowAssigner -> SessionAnalyticsCalculator
///     - All paths: -> PatternDetectionCalculator -> UserBehaviorSink
///     This demonstrates how different windowing strategies can provide different insights
///     from the same source data, enabling comprehensive user behavior analysis.
/// </remarks>
public class WindowingStrategiesPipeline : IPipelineDefinition
{
    private double _activityThreshold;
    private int _conversionTriggerThreshold;
    private double _diversityThreshold;
    private bool _enableDetailedOutput;
    private bool _enablePatternAnalysis;
    private bool _enablePerformanceMetrics;
    private TimeSpan _eventGenerationInterval;
    private double _highValueTriggerThreshold;
    private TimeSpan _maxWindowDuration;
    private int _maxWindowSize;
    private int _minWindowSize;
    private double _patternConfidenceThreshold;
    private TimeSpan _sessionTimeout;
    private TimeSpan _timeBasedTriggerInterval;
    private int _userEventCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="WindowingStrategiesPipeline" /> class.
    /// </summary>
    /// <param name="userEventCount">The number of user events to generate.</param>
    /// <param name="eventGenerationInterval">The interval between event generation.</param>
    /// <param name="sessionTimeout">The timeout for session windowing.</param>
    /// <param name="minWindowSize">The minimum window size for dynamic windowing.</param>
    /// <param name="maxWindowSize">The maximum window size for dynamic windowing.</param>
    /// <param name="maxWindowDuration">The maximum window duration.</param>
    /// <param name="activityThreshold">The activity threshold for dynamic windowing.</param>
    /// <param name="diversityThreshold">The diversity threshold for dynamic windowing.</param>
    /// <param name="conversionTriggerThreshold">The conversion trigger threshold for custom windowing.</param>
    /// <param name="highValueTriggerThreshold">The high-value trigger threshold for custom windowing.</param>
    /// <param name="timeBasedTriggerInterval">The time-based trigger interval for custom windowing.</param>
    /// <param name="patternConfidenceThreshold">The confidence threshold for pattern detection.</param>
    /// <param name="enableDetailedOutput">Whether to enable detailed output.</param>
    /// <param name="enablePatternAnalysis">Whether to enable pattern analysis.</param>
    /// <param name="enablePerformanceMetrics">Whether to enable performance metrics.</param>
    public WindowingStrategiesPipeline(
        int userEventCount = 200,
        TimeSpan? eventGenerationInterval = null,
        TimeSpan? sessionTimeout = null,
        int minWindowSize = 2,
        int maxWindowSize = 10,
        TimeSpan? maxWindowDuration = null,
        double activityThreshold = 0.3,
        double diversityThreshold = 0.2,
        int conversionTriggerThreshold = 1,
        double highValueTriggerThreshold = 50.0,
        TimeSpan? timeBasedTriggerInterval = null,
        double patternConfidenceThreshold = 0.6,
        bool enableDetailedOutput = true,
        bool enablePatternAnalysis = true,
        bool enablePerformanceMetrics = true)
    {
        _userEventCount = userEventCount;
        _eventGenerationInterval = eventGenerationInterval ?? TimeSpan.FromMilliseconds(75);
        _sessionTimeout = sessionTimeout ?? TimeSpan.FromMinutes(30);
        _minWindowSize = minWindowSize;
        _maxWindowSize = maxWindowSize;
        _maxWindowDuration = maxWindowDuration ?? TimeSpan.FromHours(2);
        _activityThreshold = activityThreshold;
        _diversityThreshold = diversityThreshold;
        _conversionTriggerThreshold = conversionTriggerThreshold;
        _highValueTriggerThreshold = highValueTriggerThreshold;
        _timeBasedTriggerInterval = timeBasedTriggerInterval ?? TimeSpan.FromMinutes(1);
        _patternConfidenceThreshold = patternConfidenceThreshold;
        _enableDetailedOutput = enableDetailedOutput;
        _enablePatternAnalysis = enablePatternAnalysis;
        _enablePerformanceMetrics = enablePerformanceMetrics;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="WindowingStrategiesPipeline" /> class with default parameters.
    /// </summary>
    public WindowingStrategiesPipeline()
    {
        _userEventCount = 200;
        _eventGenerationInterval = TimeSpan.FromMilliseconds(75);
        _sessionTimeout = TimeSpan.FromSeconds(2); // Much shorter for demo
        _minWindowSize = 2; // Reduced from 5
        _maxWindowSize = 10; // Reduced from 25
        _maxWindowDuration = TimeSpan.FromHours(2);
        _activityThreshold = 0.3; // Reduced from 0.7
        _diversityThreshold = 0.2; // Reduced from 0.6
        _conversionTriggerThreshold = 1; // Reduced from 3
        _highValueTriggerThreshold = 50.0; // Reduced from 500.0
        _timeBasedTriggerInterval = TimeSpan.FromMinutes(1); // Reduced from 15 minutes
        _patternConfidenceThreshold = 0.6;
        _enableDetailedOutput = true;
        _enablePatternAnalysis = true;
        _enablePerformanceMetrics = true;
    }

    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes with windowing strategies.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(context);

        ReadConfigurationFromContext(context);

        var userEventSource = builder.AddSource<UserEventSource, UserEvent>("user-event-source");
        _ = builder.AddPreconfiguredNodeInstance(userEventSource.Id, new UserEventSource(_userEventCount, _eventGenerationInterval));

        var sessionWindowAssigner = builder.AddTransform<SessionWindowAssigner, UserEvent, UserSession>("session-window-assigner");
        _ = builder.AddPreconfiguredNodeInstance(sessionWindowAssigner.Id, new SessionWindowAssigner(_sessionTimeout, 5));

        // Session-based analytics branch
        var sessionToCollection = builder.AddTransform<SessionToCollectionNode, UserSession, IReadOnlyCollection<UserSession>>("session-to-collection");
        _ = builder.AddPreconfiguredNodeInstance(sessionToCollection.Id, new SessionToCollectionNode());

        var sessionAnalytics = builder.AddTransform<SessionAnalyticsCalculator, IReadOnlyCollection<UserSession>, SessionMetrics>("session-analytics");
        _ = builder.AddPreconfiguredNodeInstance(sessionAnalytics.Id, new SessionAnalyticsCalculator());

        var sessionPatternDetection =
            builder.AddTransform<PatternDetectionCalculator, IReadOnlyCollection<UserSession>, PatternMatch>("session-pattern-detection");

        _ = builder.AddPreconfiguredNodeInstance(sessionPatternDetection.Id, new PatternDetectionCalculator(
            Math.Max(2, _minWindowSize),
            patternConfidenceThreshold: _patternConfidenceThreshold));

        // Dynamic window analytics branch
        var dynamicWindowAssigner = builder.AddTransform<DynamicWindowAssigner, UserSession, IReadOnlyList<UserSession>>("dynamic-window-assigner");

        _ = builder.AddPreconfiguredNodeInstance(dynamicWindowAssigner.Id, new DynamicWindowAssigner(
            _minWindowSize,
            _maxWindowSize,
            _activityThreshold,
            _diversityThreshold,
            _maxWindowDuration));

        var dynamicToCollection =
            builder.AddTransform<DynamicListToCollectionNode, IReadOnlyList<UserSession>, IReadOnlyCollection<UserSession>>("dynamic-to-collection");

        _ = builder.AddPreconfiguredNodeInstance(dynamicToCollection.Id, new DynamicListToCollectionNode());

        var dynamicAnalytics = builder.AddTransform<SessionAnalyticsCalculator, IReadOnlyCollection<UserSession>, SessionMetrics>("dynamic-session-analytics");
        _ = builder.AddPreconfiguredNodeInstance(dynamicAnalytics.Id, new SessionAnalyticsCalculator());

        var dynamicPatternDetection =
            builder.AddTransform<PatternDetectionCalculator, IReadOnlyCollection<UserSession>, PatternMatch>("dynamic-pattern-detection");

        _ = builder.AddPreconfiguredNodeInstance(dynamicPatternDetection.Id, new PatternDetectionCalculator(
            Math.Max(2, _minWindowSize),
            patternConfidenceThreshold: _patternConfidenceThreshold));

        // Custom trigger analytics branch
        var customWindowAssigner = builder.AddTransform<CustomTriggerWindowAssigner, UserSession, IReadOnlyList<UserSession>>("custom-trigger-window-assigner");

        _ = builder.AddPreconfiguredNodeInstance(customWindowAssigner.Id, new CustomTriggerWindowAssigner(
            _conversionTriggerThreshold,
            (decimal)_highValueTriggerThreshold,
            _timeBasedTriggerInterval));

        var customToCollection =
            builder.AddTransform<CustomTriggerListToCollectionNode, IReadOnlyList<UserSession>, IReadOnlyCollection<UserSession>>(
                "custom-trigger-to-collection");

        _ = builder.AddPreconfiguredNodeInstance(customToCollection.Id, new CustomTriggerListToCollectionNode());

        var customAnalytics = builder.AddTransform<SessionAnalyticsCalculator, IReadOnlyCollection<UserSession>, SessionMetrics>("custom-session-analytics");
        _ = builder.AddPreconfiguredNodeInstance(customAnalytics.Id, new SessionAnalyticsCalculator());

        var customPatternDetection =
            builder.AddTransform<PatternDetectionCalculator, IReadOnlyCollection<UserSession>, PatternMatch>("custom-pattern-detection");

        _ = builder.AddPreconfiguredNodeInstance(customPatternDetection.Id, new PatternDetectionCalculator(
            Math.Max(2, _minWindowSize),
            patternConfidenceThreshold: _patternConfidenceThreshold));

        var userBehaviorSink = builder.AddSink<UserBehaviorSink, object>("user-behavior-sink");

        _ = builder.AddPreconfiguredNodeInstance(userBehaviorSink.Id, new UserBehaviorSink(
            _enableDetailedOutput,
            _enablePatternAnalysis,
            _enablePerformanceMetrics));

        // Wiring — fan-out from session windows into three strategies
        _ = builder.Connect(userEventSource, sessionWindowAssigner);

        // Session-based direct analytics
        _ = builder.Connect(sessionWindowAssigner, sessionToCollection);
        _ = builder.Connect(sessionToCollection, sessionAnalytics);
        _ = builder.Connect(sessionAnalytics, userBehaviorSink);
        _ = builder.Connect(sessionToCollection, sessionPatternDetection);
        _ = builder.Connect(sessionPatternDetection, userBehaviorSink);

        // Dynamic window analytics
        _ = builder.Connect(sessionWindowAssigner, dynamicWindowAssigner);
        _ = builder.Connect(dynamicWindowAssigner, dynamicToCollection);
        _ = builder.Connect(dynamicToCollection, dynamicAnalytics);
        _ = builder.Connect(dynamicAnalytics, userBehaviorSink);
        _ = builder.Connect(dynamicToCollection, dynamicPatternDetection);
        _ = builder.Connect(dynamicPatternDetection, userBehaviorSink);

        // Custom trigger analytics
        _ = builder.Connect(sessionWindowAssigner, customWindowAssigner);
        _ = builder.Connect(customWindowAssigner, customToCollection);
        _ = builder.Connect(customToCollection, customAnalytics);
        _ = builder.Connect(customAnalytics, userBehaviorSink);
        _ = builder.Connect(customToCollection, customPatternDetection);
        _ = builder.Connect(customPatternDetection, userBehaviorSink);

        StoreConfigurationInContext(context);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"Advanced Windowing Strategies Sample:

This sample demonstrates sophisticated windowing techniques beyond basic tumbling and sliding windows:

Three Windowing Strategies:
1. Session-based Windowing: Groups user events into sessions based on activity timeouts
2. Dynamic Windowing: Adapts window size based on data characteristics and patterns
3. Custom Trigger Windowing: Uses complex business rules to determine window boundaries

Pipeline Architecture:
UserEventSource -> SessionWindowAssigner -> [Three Parallel Paths] -> Analytics & Pattern Detection -> UserBehaviorSink

Parallel Processing Paths:
- Path 1: Session-based direct analytics
- Path 2: Dynamic windowing with adaptive sizing
- Path 3: Custom trigger windowing with business rules

Key Concepts Demonstrated:
- Session timeout management for user activity tracking
- Dynamic window sizing based on activity and diversity metrics
- Complex trigger conditions (conversions, high-value events, time patterns)
- Multi-strategy comparison for comprehensive insights
- Advanced pattern detection across different windowing approaches
- Comprehensive analytics and behavioral analysis

Windowing Strategies in Detail:

Session-based Windowing:
- Groups events by user sessions with configurable timeouts
- Handles session splitting when gaps exceed timeout
- Provides traditional session-based analytics

Dynamic Windowing:
- Adapts window size based on activity levels
- Considers device diversity and geographic distribution
- Balances processing efficiency with analytical depth

Custom Trigger Windowing:
- Uses multiple trigger types: time-based, conversion-based, high-value events
- Detects activity spikes and geographic diversity
- Provides business-rule-driven window boundaries

This implementation follows the IPipelineDefinition pattern, providing:
- Reusable pipeline definitions with configurable parameters
- Proper node isolation between executions
- Type-safe node connections
- Clear separation of pipeline structure from execution logic

The advanced windowing strategies are essential when:
- Standard time-based windows don't capture user behavior patterns
- Business requirements demand complex windowing logic
- Different analytical insights are needed from the same data
- Adaptive processing is required for varying data patterns";
    }

    private void ReadConfigurationFromContext(PipelineContext context)
    {
        if (context.Parameters.TryGetValue("UserEventCount", out var eventCountObj) && eventCountObj is int eventCount)
            _userEventCount = eventCount;

        if (context.Parameters.TryGetValue("EventGenerationInterval", out var intervalObj) && intervalObj is TimeSpan interval)
            _eventGenerationInterval = interval;

        if (context.Parameters.TryGetValue("SessionTimeout", out var timeoutObj) && timeoutObj is TimeSpan timeout)
            _sessionTimeout = timeout;

        if (context.Parameters.TryGetValue("MinWindowSize", out var minSizeObj) && minSizeObj is int minSize)
            _minWindowSize = minSize;

        if (context.Parameters.TryGetValue("MaxWindowSize", out var maxSizeObj) && maxSizeObj is int maxSize)
            _maxWindowSize = maxSize;

        if (context.Parameters.TryGetValue("MaxWindowDuration", out var maxDurationObj) && maxDurationObj is TimeSpan maxDuration)
            _maxWindowDuration = maxDuration;

        if (context.Parameters.TryGetValue("ActivityThreshold", out var activityObj) && activityObj is double activity)
            _activityThreshold = activity;

        if (context.Parameters.TryGetValue("DiversityThreshold", out var diversityObj) && diversityObj is double diversity)
            _diversityThreshold = diversity;

        if (context.Parameters.TryGetValue("ConversionTriggerThreshold", out var convTriggerObj) && convTriggerObj is int convTrigger)
            _conversionTriggerThreshold = convTrigger;

        if (context.Parameters.TryGetValue("HighValueTriggerThreshold", out var highValueObj) && highValueObj is double highValue)
            _highValueTriggerThreshold = highValue;

        if (context.Parameters.TryGetValue("TimeBasedTriggerInterval", out var timeIntervalObj) && timeIntervalObj is TimeSpan triggerInterval)
            _timeBasedTriggerInterval = triggerInterval;

        if (context.Parameters.TryGetValue("PatternConfidenceThreshold", out var confidenceObj) && confidenceObj is double confidence)
            _patternConfidenceThreshold = confidence;

        if (context.Parameters.TryGetValue("EnableDetailedOutput", out var detailedObj) && detailedObj is bool detailed)
            _enableDetailedOutput = detailed;

        if (context.Parameters.TryGetValue("EnablePatternAnalysis", out var patternObj) && patternObj is bool pattern)
            _enablePatternAnalysis = pattern;

        if (context.Parameters.TryGetValue("EnablePerformanceMetrics", out var perfObj) && perfObj is bool perf)
            _enablePerformanceMetrics = perf;
    }

    private void StoreConfigurationInContext(PipelineContext context)
    {
        context.Parameters["UserEventCount"] = _userEventCount;
        context.Parameters["EventGenerationInterval"] = _eventGenerationInterval;
        context.Parameters["SessionTimeout"] = _sessionTimeout;
        context.Parameters["MinWindowSize"] = _minWindowSize;
        context.Parameters["MaxWindowSize"] = _maxWindowSize;
        context.Parameters["MaxWindowDuration"] = _maxWindowDuration;
        context.Parameters["ActivityThreshold"] = _activityThreshold;
        context.Parameters["DiversityThreshold"] = _diversityThreshold;
        context.Parameters["ConversionTriggerThreshold"] = _conversionTriggerThreshold;
        context.Parameters["HighValueTriggerThreshold"] = _highValueTriggerThreshold;
        context.Parameters["TimeBasedTriggerInterval"] = _timeBasedTriggerInterval;
        context.Parameters["PatternConfidenceThreshold"] = _patternConfidenceThreshold;
        context.Parameters["EnableDetailedOutput"] = _enableDetailedOutput;
        context.Parameters["EnablePatternAnalysis"] = _enablePatternAnalysis;
        context.Parameters["EnablePerformanceMetrics"] = _enablePerformanceMetrics;
    }
}

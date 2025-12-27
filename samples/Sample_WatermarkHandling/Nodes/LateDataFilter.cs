using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_WatermarkHandling.Models;

namespace Sample_WatermarkHandling.Nodes;

/// <summary>
///     Transform node that filters and handles late data with configurable tolerance policies.
///     This node demonstrates sophisticated late data handling strategies and policies.
/// </summary>
public class LateDataFilter : TransformNode<SensorReading, SensorReading>
{
    private readonly ConcurrentDictionary<string, LateDataPolicy> _devicePolicies = new();
    private readonly LateDataHandlingMode _handlingMode = LateDataHandlingMode.Adaptive;
    private readonly ConcurrentQueue<LateDataRecord> _lateDataRecords = new();
    private readonly ILogger<LateDataFilter> _logger;
    private readonly LateDataStatistics _statistics = new();
    private DateTimeOffset _currentWatermark = DateTimeOffset.MinValue;

    /// <summary>
    ///     Initializes a new instance of the LateDataFilter class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public LateDataFilter(ILogger<LateDataFilter> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        InitializeDefaultPolicies();
    }

    /// <summary>
    ///     Processes sensor reading and applies late data filtering policies.
    /// </summary>
    /// <param name="reading">The input sensor reading.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The processed sensor reading (or filtered out).</returns>
    public override async Task<SensorReading> ExecuteAsync(
        SensorReading reading,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug("Processing reading from {DeviceId} for late data filtering",
            reading.DeviceId);

        // Update current watermark from quality indicators
        UpdateCurrentWatermark(reading);

        // Get device policy
        var devicePrefix = ExtractDevicePrefix(reading.DeviceId);
        var policy = _devicePolicies.GetValueOrDefault(devicePrefix, GetDefaultPolicy());

        // Check for late data
        var lateness = CalculateLateness(reading.Timestamp, _currentWatermark);

        if (lateness <= TimeSpan.Zero)
        {
            // On-time or early data
            UpdateStatistics(false, false, false, false);
            _logger.LogDebug("Reading from {DeviceId} is on-time", reading.DeviceId);
            return reading;
        }

        // Handle late data based on policy
        var shouldProcess = await HandleLateData(reading, lateness, policy, cancellationToken);

        if (shouldProcess)
        {
            _logger.LogDebug("Late reading from {DeviceId} accepted for processing", reading.DeviceId);
            return reading;
        }

        _logger.LogDebug("Late reading from {DeviceId} filtered out", reading.DeviceId);

        // For TransformNode, we need to return something, but we can't return null
        // In a real implementation, this might be handled differently
        return reading;
    }

    /// <summary>
    ///     Initializes default late data policies for different device types.
    /// </summary>
    private void InitializeDefaultPolicies()
    {
        // Production Line A (WiFi, GPS-disciplined): Strict policy
        _devicePolicies["PLA"] = new LateDataPolicy
        {
            DeviceType = "Production Line A",
            ToleranceWindow = TimeSpan.FromMilliseconds(100),
            HandlingAction = LateDataHandlingAction.Accepted,
            MaxLatenessForAcceptance = TimeSpan.FromMilliseconds(200),
            AlertThreshold = TimeSpan.FromMilliseconds(150),
        };

        // Production Line B (LoRaWAN, NTP): Relaxed policy
        _devicePolicies["PLB"] = new LateDataPolicy
        {
            DeviceType = "Production Line B",
            ToleranceWindow = TimeSpan.FromMilliseconds(500),
            HandlingAction = LateDataHandlingAction.SideOutput,
            MaxLatenessForAcceptance = TimeSpan.FromMilliseconds(1000),
            AlertThreshold = TimeSpan.FromMilliseconds(750),
        };

        // Environmental (Ethernet, Internal clock): Moderate policy
        _devicePolicies["ENV"] = new LateDataPolicy
        {
            DeviceType = "Environmental",
            ToleranceWindow = TimeSpan.FromMilliseconds(250),
            HandlingAction = LateDataHandlingAction.Accepted,
            MaxLatenessForAcceptance = TimeSpan.FromMilliseconds(500),
            AlertThreshold = TimeSpan.FromMilliseconds(400),
        };
    }

    /// <summary>
    ///     Processes a sensor reading and applies late data filtering.
    /// </summary>
    /// <param name="reading">The input sensor reading.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if the reading should be processed, false if it should be filtered out.</returns>
    private async Task<bool> ProcessReadingWithLateDataFilter(
        SensorReading reading,
        CancellationToken cancellationToken)
    {
        // Update current watermark from quality indicators
        UpdateCurrentWatermark(reading);

        // Get device policy
        var devicePrefix = ExtractDevicePrefix(reading.DeviceId);
        var policy = _devicePolicies.GetValueOrDefault(devicePrefix, GetDefaultPolicy());

        // Check for late data
        var lateness = CalculateLateness(reading.Timestamp, _currentWatermark);

        if (lateness <= TimeSpan.Zero)
        {
            // On-time or early data
            UpdateStatistics(false, false, false, false);
            return true;
        }

        // Handle late data based on policy
        return await HandleLateData(reading, lateness, policy, cancellationToken);
    }

    /// <summary>
    ///     Updates the current watermark from quality indicators.
    /// </summary>
    /// <param name="reading">The sensor reading.</param>
    private void UpdateCurrentWatermark(SensorReading reading)
    {
        // Extract watermark information from quality indicators if available
        // For now, use the reading timestamp as a proxy
        if (reading.Timestamp > _currentWatermark)
            _currentWatermark = reading.Timestamp;
    }

    /// <summary>
    ///     Extracts device prefix from device ID.
    /// </summary>
    /// <param name="deviceId">The device identifier.</param>
    /// <returns>The device prefix.</returns>
    private static string ExtractDevicePrefix(string deviceId)
    {
        var parts = deviceId.Split('-');

        return parts.Length > 0
            ? parts[0]
            : "UNKNOWN";
    }

    /// <summary>
    ///     Calculates the lateness of a reading relative to the current watermark.
    /// </summary>
    /// <param name="eventTime">The event timestamp.</param>
    /// <param name="watermark">The current watermark.</param>
    /// <returns>The lateness duration.</returns>
    private static TimeSpan CalculateLateness(DateTimeOffset eventTime, DateTimeOffset watermark)
    {
        return eventTime - watermark;
    }

    /// <summary>
    ///     Handles late data based on the configured policy.
    /// </summary>
    /// <param name="reading">The late sensor reading.</param>
    /// <param name="lateness">The calculated lateness.</param>
    /// <param name="policy">The late data policy.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if the reading should be processed, false if it should be filtered out.</returns>
    private async Task<bool> HandleLateData(
        SensorReading reading,
        TimeSpan lateness,
        LateDataPolicy policy,
        CancellationToken cancellationToken)
    {
        var action = DetermineHandlingAction(lateness, policy);

        // Record the late data event
        var lateRecord = new LateDataRecord(
            reading.DeviceId,
            reading.Timestamp.UtcDateTime,
            DateTimeOffset.UtcNow.UtcDateTime,
            action,
            $"Lateness: {lateness.TotalMilliseconds:F0}ms, Policy: {policy.DeviceType}")
        {
            ReadingType = reading.ReadingType.ToString(),
            Value = reading.Value,
            Unit = reading.Unit,
            WatermarkAtArrival = _currentWatermark.UtcDateTime,
        };

        _lateDataRecords.Enqueue(lateRecord);

        // Update statistics
        var isAccepted = action == LateDataHandlingAction.Accepted;
        var isRejected = action == LateDataHandlingAction.Rejected;
        var isAlerted = action == LateDataHandlingAction.Alerted;
        UpdateStatistics(true, isAccepted, isRejected, isAlerted);

        // Log the late data handling
        await LogLateDataHandling(reading, lateness, action, policy, cancellationToken);

        // Return whether to process the reading
        return isAccepted || action == LateDataHandlingAction.SideOutput;
    }

    /// <summary>
    ///     Determines the handling action for late data.
    /// </summary>
    /// <param name="lateness">The calculated lateness.</param>
    /// <param name="policy">The late data policy.</param>
    /// <returns>The handling action to take.</returns>
    private LateDataHandlingAction DetermineHandlingAction(TimeSpan lateness, LateDataPolicy policy)
    {
        return _handlingMode switch
        {
            LateDataHandlingMode.Strict => lateness <= policy.ToleranceWindow
                ? LateDataHandlingAction.Accepted
                : LateDataHandlingAction.Rejected,

            LateDataHandlingMode.Lenient => lateness <= policy.MaxLatenessForAcceptance
                ? LateDataHandlingAction.Accepted
                : LateDataHandlingAction.SideOutput,

            LateDataHandlingMode.Adaptive => lateness <= policy.ToleranceWindow
                ? LateDataHandlingAction.Accepted
                : lateness <= policy.MaxLatenessForAcceptance
                    ? LateDataHandlingAction.SideOutput
                    : LateDataHandlingAction.Rejected,

            _ => LateDataHandlingAction.Accepted,
        };
    }

    /// <summary>
    ///     Gets the default late data policy.
    /// </summary>
    /// <returns>The default late data policy.</returns>
    private static LateDataPolicy GetDefaultPolicy()
    {
        return new LateDataPolicy
        {
            DeviceType = "Unknown",
            ToleranceWindow = TimeSpan.FromMilliseconds(100),
            HandlingAction = LateDataHandlingAction.Accepted,
            MaxLatenessForAcceptance = TimeSpan.FromMilliseconds(500),
            AlertThreshold = TimeSpan.FromMilliseconds(250),
        };
    }

    /// <summary>
    ///     Updates late data statistics.
    /// </summary>
    /// <param name="isLate">Whether the data is late.</param>
    /// <param name="isAccepted">Whether the data was accepted.</param>
    /// <param name="isRejected">Whether the data was rejected.</param>
    /// <param name="isAlerted">Whether an alert was generated.</param>
    private void UpdateStatistics(bool isLate, bool isAccepted, bool isRejected, bool isAlerted)
    {
        if (isLate)
            _statistics.TotalLateEvents++;

        if (isAccepted)
            _statistics.AcceptedEvents++;

        if (isRejected)
            _statistics.RejectedEvents++;

        if (isAlerted)
            _statistics.AlertedEvents++;

        // Update lateness statistics
        if (_lateDataRecords.TryPeek(out var latestRecord))
        {
            var totalLateness = _statistics.AverageLateness.Ticks * (_statistics.TotalLateEvents - 1) + latestRecord.LatenessDuration.Ticks;
            _statistics.AverageLateness = TimeSpan.FromTicks(totalLateness / _statistics.TotalLateEvents);

            if (latestRecord.LatenessDuration > _statistics.MaximumLateness)
                _statistics.MaximumLateness = latestRecord.LatenessDuration;

            if (latestRecord.LatenessDuration < _statistics.MinimumLateness)
                _statistics.MinimumLateness = latestRecord.LatenessDuration;
        }
    }

    /// <summary>
    ///     Logs late data handling events.
    /// </summary>
    /// <param name="reading">The late sensor reading.</param>
    /// <param name="lateness">The calculated lateness.</param>
    /// <param name="action">The handling action taken.</param>
    /// <param name="policy">The late data policy.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task LogLateDataHandling(
        SensorReading reading,
        TimeSpan lateness,
        LateDataHandlingAction action,
        LateDataPolicy policy,
        CancellationToken cancellationToken)
    {
        var severity = lateness > policy.AlertThreshold
            ? "WARNING"
            : "INFO";

        var actionText = action switch
        {
            LateDataHandlingAction.Accepted => "Accepted",
            LateDataHandlingAction.Rejected => "Rejected",
            LateDataHandlingAction.SideOutput => "Side Output",
            LateDataHandlingAction.Alerted => "Alerted",
            LateDataHandlingAction.LoggedOnly => "Logged Only",
            _ => "Unknown",
        };

        _logger.Log(
            lateness > policy.AlertThreshold
                ? LogLevel.Warning
                : LogLevel.Information,
            "Late Data [{Severity}] from {DeviceId}: {Timestamp} is {LatenessMs:F0}ms late - Action: {Action}",
            severity,
            reading.DeviceId,
            reading.Timestamp,
            lateness.TotalMilliseconds,
            actionText);

        await Task.CompletedTask;
    }
}

/// <summary>
///     Represents a late data policy for specific device types.
/// </summary>
public class LateDataPolicy
{
    /// <summary>
    ///     Gets or sets the device type description.
    /// </summary>
    public string DeviceType { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the tolerance window for considering data as late.
    /// </summary>
    public TimeSpan ToleranceWindow { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     Gets or sets the default handling action for late data.
    /// </summary>
    public LateDataHandlingAction HandlingAction { get; set; } = LateDataHandlingAction.Accepted;

    /// <summary>
    ///     Gets or sets the maximum lateness for acceptance.
    /// </summary>
    public TimeSpan MaxLatenessForAcceptance { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    ///     Gets or sets the alert threshold for late data.
    /// </summary>
    public TimeSpan AlertThreshold { get; set; } = TimeSpan.FromMilliseconds(250);
}

/// <summary>
///     Represents late data handling modes.
/// </summary>
public enum LateDataHandlingMode
{
    /// <summary>
    ///     Strict mode: reject data beyond tolerance window.
    /// </summary>
    Strict,

    /// <summary>
    ///     Lenient mode: accept data within maximum lateness.
    /// </summary>
    Lenient,

    /// <summary>
    ///     Adaptive mode: accept within tolerance, side-output within maximum, reject beyond maximum.
    /// </summary>
    Adaptive,
}

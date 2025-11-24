namespace Sample_WatermarkHandling.Models;

/// <summary>
///     Represents a record of late data events for analysis in the IoT manufacturing platform.
///     This class captures information about data that arrives after watermarks have advanced.
/// </summary>
public class LateDataRecord
{
    /// <summary>
    ///     Initializes a new instance of the LateDataRecord class.
    /// </summary>
    /// <param name="deviceId">Source device identifier.</param>
    /// <param name="originalTimestamp">Original event timestamp.</param>
    /// <param name="arrivalTimestamp">When the late data arrived.</param>
    /// <param name="handlingAction">What action was taken.</param>
    /// <param name="reason">Why the data was considered late.</param>
    public LateDataRecord(
        string deviceId,
        DateTime originalTimestamp,
        DateTime arrivalTimestamp,
        LateDataHandlingAction handlingAction,
        string reason)
    {
        DeviceId = deviceId;
        OriginalTimestamp = originalTimestamp;
        ArrivalTimestamp = arrivalTimestamp;
        HandlingAction = handlingAction;
        Reason = reason;
    }

    /// <summary>
    ///     Gets the source device identifier.
    /// </summary>
    public string DeviceId { get; }

    /// <summary>
    ///     Gets the original event timestamp.
    /// </summary>
    public DateTime OriginalTimestamp { get; }

    /// <summary>
    ///     Gets when the late data arrived.
    /// </summary>
    public DateTime ArrivalTimestamp { get; }

    /// <summary>
    ///     Gets how late the data was.
    /// </summary>
    public TimeSpan LatenessDuration => ArrivalTimestamp - OriginalTimestamp;

    /// <summary>
    ///     Gets what action was taken (accept, reject, side-output).
    /// </summary>
    public LateDataHandlingAction HandlingAction { get; }

    /// <summary>
    ///     Gets why the data was considered late.
    /// </summary>
    public string Reason { get; }

    /// <summary>
    ///     Gets or sets the reading type for the late data.
    /// </summary>
    public string? ReadingType { get; set; }

    /// <summary>
    ///     Gets or sets the value of the late reading.
    /// </summary>
    public double? Value { get; set; }

    /// <summary>
    ///     Gets or sets the unit of measurement for the late reading.
    /// </summary>
    public string? Unit { get; set; }

    /// <summary>
    ///     Gets or sets the watermark timestamp at the time of arrival.
    /// </summary>
    public DateTime? WatermarkAtArrival { get; set; }

    /// <summary>
    ///     Gets a value indicating whether the lateness is severe (> 1 second).
    /// </summary>
    public bool IsSevereLateness => LatenessDuration > TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets a value indicating whether the lateness is moderate (> 100ms but <= 1 second).
    /// </summary>
    public bool IsModerateLateness => LatenessDuration > TimeSpan.FromMilliseconds(100) && LatenessDuration <= TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets a value indicating whether the lateness is minor (<= 100ms).
    /// </summary>
    public bool IsMinorLateness => LatenessDuration <= TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     Returns a string representation of the late data record.
    /// </summary>
    /// <returns>String representation of the late data record.</returns>
    public override string ToString()
    {
        var severity = IsSevereLateness
            ? "SEVERE"
            : IsModerateLateness
                ? "MODERATE"
                : "MINOR";

        return $"Late Data: {DeviceId} | Original: {OriginalTimestamp:HH:mm:ss.fff} | " +
               $"Arrival: {ArrivalTimestamp:HH:mm:ss.fff} | " +
               $"Lateness: {LatenessDuration.TotalMilliseconds:F0}ms ({severity}) | " +
               $"Action: {HandlingAction} | Reason: {Reason}";
    }
}

/// <summary>
///     Represents the action taken for late data handling.
/// </summary>
public enum LateDataHandlingAction
{
    /// <summary>
    ///     Late data was accepted and processed.
    /// </summary>
    Accepted,

    /// <summary>
    ///     Late data was rejected and discarded.
    /// </summary>
    Rejected,

    /// <summary>
    ///     Late data was sent to a side output for special handling.
    /// </summary>
    SideOutput,

    /// <summary>
    ///     Late data triggered an alert or notification.
    /// </summary>
    Alerted,

    /// <summary>
    ///     Late data was logged for analysis only.
    /// </summary>
    LoggedOnly,
}

/// <summary>
///     Represents statistics about late data events.
/// </summary>
public class LateDataStatistics
{
    /// <summary>
    ///     Initializes a new instance of the LateDataStatistics class.
    /// </summary>
    public LateDataStatistics()
    {
        TotalLateEvents = 0;
        AcceptedEvents = 0;
        RejectedEvents = 0;
        SideOutputEvents = 0;
        AlertedEvents = 0;
        AverageLateness = TimeSpan.Zero;
        MaximumLateness = TimeSpan.Zero;
        MinimumLateness = TimeSpan.MaxValue;
    }

    /// <summary>
    ///     Gets or sets the total number of late events.
    /// </summary>
    public long TotalLateEvents { get; set; }

    /// <summary>
    ///     Gets or sets the number of accepted late events.
    /// </summary>
    public long AcceptedEvents { get; set; }

    /// <summary>
    ///     Gets or sets the number of rejected late events.
    /// </summary>
    public long RejectedEvents { get; set; }

    /// <summary>
    ///     Gets or sets the number of side output late events.
    /// </summary>
    public long SideOutputEvents { get; set; }

    /// <summary>
    ///     Gets or sets the number of alerted late events.
    /// </summary>
    public long AlertedEvents { get; set; }

    /// <summary>
    ///     Gets or sets the average lateness duration.
    /// </summary>
    public TimeSpan AverageLateness { get; set; }

    /// <summary>
    ///     Gets or sets the maximum lateness duration.
    /// </summary>
    public TimeSpan MaximumLateness { get; set; }

    /// <summary>
    ///     Gets or sets the minimum lateness duration.
    /// </summary>
    public TimeSpan MinimumLateness { get; set; }

    /// <summary>
    ///     Gets the acceptance rate for late data (0.0 to 1.0).
    /// </summary>
    public double AcceptanceRate => TotalLateEvents > 0
        ? (double)AcceptedEvents / TotalLateEvents
        : 0.0;

    /// <summary>
    ///     Gets the rejection rate for late data (0.0 to 1.0).
    /// </summary>
    public double RejectionRate => TotalLateEvents > 0
        ? (double)RejectedEvents / TotalLateEvents
        : 0.0;

    /// <summary>
    ///     Returns a string representation of the late data statistics.
    /// </summary>
    /// <returns>String representation of the late data statistics.</returns>
    public override string ToString()
    {
        return $"Late Data Stats: Total={TotalLateEvents} | " +
               $"Accepted={AcceptedEvents} ({AcceptanceRate:P0}) | " +
               $"Rejected={RejectedEvents} ({RejectionRate:P0}) | " +
               $"Avg Lateness={AverageLateness.TotalMilliseconds:F0}ms | " +
               $"Max Lateness={MaximumLateness.TotalMilliseconds:F0}ms";
    }
}

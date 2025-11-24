using NPipeline.DataFlow;

namespace Sample_WatermarkHandling.Models;

/// <summary>
///     Represents the type of sensor reading for IoT devices.
/// </summary>
public enum ReadingType
{
    /// <summary>
    ///     Temperature sensor reading.
    /// </summary>
    Temperature,

    /// <summary>
    ///     Pressure sensor reading.
    /// </summary>
    Pressure,

    /// <summary>
    ///     Humidity sensor reading.
    /// </summary>
    Humidity,

    /// <summary>
    ///     Vibration sensor reading.
    /// </summary>
    Vibration,

    /// <summary>
    ///     Flow meter reading.
    /// </summary>
    FlowMeter,

    /// <summary>
    ///     Quality camera reading.
    /// </summary>
    QualityCamera,

    /// <summary>
    ///     Power monitor reading.
    /// </summary>
    PowerMonitor,

    /// <summary>
    ///     Air quality sensor reading.
    /// </summary>
    AirQuality,
}

/// <summary>
///     Represents a sensor reading from IoT devices in the manufacturing platform.
///     This record implements ITimestamped for watermark-aware processing.
/// </summary>
/// <param name="DeviceId">Unique identifier for the sensor device.</param>
/// <param name="Timestamp">Event timestamp when the reading was captured.</param>
/// <param name="Value">The measured value.</param>
/// <param name="Unit">Unit of measurement.</param>
/// <param name="ReadingType">Type of sensor reading (e.g., Temperature, Pressure, etc.).</param>
/// <param name="QualityIndicators">Data quality indicators for the reading.</param>
public record SensorReading(
    string DeviceId,
    DateTimeOffset Timestamp,
    double Value,
    string Unit,
    ReadingType ReadingType,
    DataQualityIndicators QualityIndicators) : ITimestamped
{
    /// <summary>
    ///     Gets the timestamp for watermark processing.
    /// </summary>
    DateTimeOffset ITimestamped.Timestamp => Timestamp;
}

/// <summary>
///     Represents data quality indicators for sensor readings.
/// </summary>
public class DataQualityIndicators
{
    /// <summary>
    ///     Initializes a new instance of the DataQualityIndicators class.
    /// </summary>
    public DataQualityIndicators()
    {
        CompletenessScore = 1.0;
        TimelinessScore = 1.0;
        AccuracyScore = 1.0;
        ConsistencyScore = 1.0;
        IsStale = false;
        HasGaps = false;
        IsSuspiciousValue = false;
        IsOutOfOrder = false;
        IsDuplicate = false;
        IsIncomplete = false;
        IsInconsistent = false;
        IsDelayed = false;
        HasErrors = false;
    }

    /// <summary>
    ///     Gets or sets the completeness score (0.0 to 1.0).
    /// </summary>
    public double CompletenessScore { get; set; }

    /// <summary>
    ///     Gets or sets the timeliness score (0.0 to 1.0).
    /// </summary>
    public double TimelinessScore { get; set; }

    /// <summary>
    ///     Gets or sets the accuracy score (0.0 to 1.0).
    /// </summary>
    public double AccuracyScore { get; set; }

    /// <summary>
    ///     Gets or sets the consistency score (0.0 to 1.0).
    /// </summary>
    public double ConsistencyScore { get; set; }

    /// <summary>
    ///     Gets the overall quality score (0.0 to 1.0).
    /// </summary>
    public double OverallQualityScore => (CompletenessScore + TimelinessScore + AccuracyScore + ConsistencyScore) / 4.0;

    /// <summary>
    ///     Gets or sets a value indicating whether the data is stale.
    /// </summary>
    public bool IsStale { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data has gaps.
    /// </summary>
    public bool HasGaps { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the value is suspicious.
    /// </summary>
    public bool IsSuspiciousValue { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data is out of order.
    /// </summary>
    public bool IsOutOfOrder { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data is a duplicate.
    /// </summary>
    public bool IsDuplicate { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data is incomplete.
    /// </summary>
    public bool IsIncomplete { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data is inconsistent.
    /// </summary>
    public bool IsInconsistent { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data is delayed.
    /// </summary>
    public bool IsDelayed { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether the data has errors.
    /// </summary>
    public bool HasErrors { get; set; }

    /// <summary>
    ///     Returns a string representation of the data quality indicators.
    /// </summary>
    /// <returns>String representation of the data quality indicators.</returns>
    public override string ToString()
    {
        var issues = new List<string>();

        if (IsStale)
            issues.Add("Stale");

        if (HasGaps)
            issues.Add("Gaps");

        if (IsSuspiciousValue)
            issues.Add("SuspiciousValue");

        if (IsOutOfOrder)
            issues.Add("OutOfOrder");

        if (IsDuplicate)
            issues.Add("Duplicate");

        if (IsIncomplete)
            issues.Add("Incomplete");

        if (IsInconsistent)
            issues.Add("Inconsistent");

        if (IsDelayed)
            issues.Add("Delayed");

        if (HasErrors)
            issues.Add("Errors");

        return issues.Count > 0
            ? string.Join(", ", issues)
            : $"Good ({OverallQualityScore:P0})";
    }
}

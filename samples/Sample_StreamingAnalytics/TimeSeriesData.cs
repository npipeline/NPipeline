namespace Sample_StreamingAnalytics;

/// <summary>
///     Represents a single time-series data point with timestamp and value.
/// </summary>
public record TimeSeriesData
{
    /// <summary>
    ///     Gets the timestamp when the data point was generated.
    /// </summary>
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;

    /// <summary>
    ///     Gets the numeric value of the data point.
    /// </summary>
    public double Value { get; init; }

    /// <summary>
    ///     Gets the source identifier for the data point.
    /// </summary>
    public string Source { get; init; } = string.Empty;

    /// <summary>
    ///     Gets the unique identifier for this data point.
    /// </summary>
    public string Id { get; init; } = Guid.NewGuid().ToString();

    /// <summary>
    ///     Gets a value indicating whether this data point is considered late-arriving.
    /// </summary>
    public bool IsLate { get; init; }

    /// <summary>
    ///     Gets the original timestamp for late-arriving data (if applicable).
    /// </summary>
    public DateTime? OriginalTimestamp { get; init; }

    public override string ToString()
    {
        return $"TimeSeriesData(Id={Id}, Timestamp={Timestamp:O}, Value={Value:F2}, Source={Source}, IsLate={IsLate})";
    }
}

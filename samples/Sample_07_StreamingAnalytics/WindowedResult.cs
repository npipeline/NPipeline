namespace Sample_07_StreamingAnalytics;

/// <summary>
///     Represents the result of windowed aggregation operations.
/// </summary>
public record WindowedResult
{
    /// <summary>
    ///     Gets the start time of the window.
    /// </summary>
    public DateTime WindowStart { get; init; }

    /// <summary>
    ///     Gets the end time of the window.
    /// </summary>
    public DateTime WindowEnd { get; init; }

    /// <summary>
    ///     Gets the type of window (tumbling or sliding).
    /// </summary>
    public string WindowType { get; init; } = string.Empty;

    /// <summary>
    ///     Gets the number of data points in the window.
    /// </summary>
    public int Count { get; init; }

    /// <summary>
    ///     Gets the sum of all values in the window.
    /// </summary>
    public double Sum { get; init; }

    /// <summary>
    ///     Gets the average value in the window.
    /// </summary>
    public double Average { get; init; }

    /// <summary>
    ///     Gets the minimum value in the window.
    /// </summary>
    public double Min { get; init; }

    /// <summary>
    ///     Gets the maximum value in the window.
    /// </summary>
    public double Max { get; init; }

    /// <summary>
    ///     Gets the number of late-arriving data points in the window.
    /// </summary>
    public int LateCount { get; init; }

    /// <summary>
    ///     Gets the sources that contributed data to this window.
    /// </summary>
    public HashSet<string> Sources { get; init; } = new();

    /// <summary>
    ///     Gets the window duration in milliseconds.
    /// </summary>
    public long WindowDurationMs => (long)(WindowEnd - WindowStart).TotalMilliseconds;

    public override string ToString()
    {
        return $"WindowedResult({WindowType}: {WindowStart:O} - {WindowEnd:O}, Count={Count}, " +
               $"Avg={Average:F2}, Min={Min:F2}, Max={Max:F2}, Late={LateCount})";
    }
}

namespace NPipeline.DataFlow;

/// <summary>
///     Represents a data item that has a timestamp, typically indicating when an event occurred.
///     Implementing this interface allows nodes to perform time-based operations like windowing and joining.
/// </summary>
public interface ITimestamped
{
    /// <summary>
    ///     Gets the timestamp associated with this data item.
    ///     This represents the event time (when the event occurred) rather than processing time.
    /// </summary>
    DateTimeOffset Timestamp { get; }
}

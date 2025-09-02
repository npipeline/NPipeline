namespace NPipeline.DataFlow.Timestamping;

/// <summary>
///     Represents a function that can extract a timestamp from a data item.
///     This is used when data items don't implement the <see cref="ITimestamped" /> interface.
/// </summary>
/// <typeparam name="T">The type of the data item.</typeparam>
/// <param name="item">The data item to extract the timestamp from.</param>
/// <returns>The extracted timestamp.</returns>
public delegate DateTimeOffset TimestampExtractor<in T>(T item);

using System.Text.Json;

namespace NPipeline.Connectors.Kafka.Partitioning;

/// <summary>
///     Default implementation of <see cref="IPartitionKeyProvider{T}" /> that extracts keys using a selector function.
/// </summary>
/// <typeparam name="T">The message type.</typeparam>
public sealed class DefaultPartitionKeyProvider<T> : IPartitionKeyProvider<T>
{
    private readonly Func<T, string> _keySelector;

    /// <summary>
    ///     Initializes a new instance with a key selector function.
    /// </summary>
    /// <param name="keySelector">A function that extracts the partition key from a message.</param>
    public DefaultPartitionKeyProvider(Func<T, string> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        _keySelector = keySelector;
    }

    /// <inheritdoc />
    public string GetPartitionKey(T message)
    {
        return _keySelector(message);
    }
}

/// <summary>
///     Factory methods for creating partition key providers.
/// </summary>
public static class PartitionKeyProvider
{
    /// <summary>
    ///     Creates a partition key provider that uses a property selector.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <typeparam name="TKey">The type of the key property.</typeparam>
    /// <param name="keySelector">A function that selects the key property.</param>
    /// <returns>A partition key provider.</returns>
    public static DefaultPartitionKeyProvider<T> FromProperty<T, TKey>(Func<T, TKey> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        return new DefaultPartitionKeyProvider<T>(msg => keySelector(msg)?.ToString() ?? string.Empty);
    }

    /// <summary>
    ///     Creates a partition key provider that serializes the entire message to JSON as the key.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <returns>A partition key provider.</returns>
    public static DefaultPartitionKeyProvider<T> FromJson<T>()
    {
        return new DefaultPartitionKeyProvider<T>(msg => JsonSerializer.Serialize(msg));
    }

    /// <summary>
    ///     Creates a partition key provider that uses a constant key.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="key">The constant key to use.</param>
    /// <returns>A partition key provider.</returns>
    public static DefaultPartitionKeyProvider<T> Constant<T>(string key)
    {
        return new DefaultPartitionKeyProvider<T>(_ => key);
    }
}

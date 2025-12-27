namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Fluent builder for configuring parallel execution options.
///     Provides a simpler alternative to manually setting all ParallelOptions properties.
/// </summary>
public sealed class ParallelOptionsBuilder
{
    private int? _maxDegreeOfParallelism;
    private int? _maxQueueLength;
    private TimeSpan? _metricsInterval;
    private int? _outputBufferCapacity;
    private bool _preserveOrdering = true;
    private BoundedQueuePolicy _queuePolicy = BoundedQueuePolicy.Block;

    /// <summary>
    ///     Sets the maximum degree of parallelism.
    ///     If not set, defaults to Environment.ProcessorCount.
    /// </summary>
    /// <param name="value">The maximum degree of parallelism (must be positive).</param>
    /// <returns>This builder for method chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when value is not positive.</exception>
    public ParallelOptionsBuilder MaxDegreeOfParallelism(int value)
    {
        if (value <= 0)
            throw new ArgumentOutOfRangeException(nameof(value), "Max degree of parallelism must be positive.");

        _maxDegreeOfParallelism = value;
        return this;
    }

    /// <summary>
    ///     Sets the maximum input queue length for bounded queue policies.
    ///     When the queue is full, the behavior depends on the chosen queue policy.
    /// </summary>
    /// <param name="value">The queue length (must be positive).</param>
    /// <returns>This builder for method chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when value is not positive.</exception>
    public ParallelOptionsBuilder MaxQueueLength(int value)
    {
        if (value <= 0)
            throw new ArgumentOutOfRangeException(nameof(value), "Queue length must be positive.");

        _maxQueueLength = value;
        return this;
    }

    /// <summary>
    ///     Configures the queue policy to drop the oldest item when the queue is full.
    ///     Useful for scenarios where new data is more important than old data.
    /// </summary>
    /// <returns>This builder for method chaining.</returns>
    public ParallelOptionsBuilder DropOldestOnBackpressure()
    {
        _queuePolicy = BoundedQueuePolicy.DropOldest;
        return this;
    }

    /// <summary>
    ///     Configures the queue policy to drop the newest item when the queue is full.
    ///     Useful for scenarios where historical data is more important than recent data.
    /// </summary>
    /// <returns>This builder for method chaining.</returns>
    public ParallelOptionsBuilder DropNewestOnBackpressure()
    {
        _queuePolicy = BoundedQueuePolicy.DropNewest;
        return this;
    }

    /// <summary>
    ///     Blocks the producer when the queue is full, applying backpressure.
    ///     This is the default behavior.
    /// </summary>
    /// <returns>This builder for method chaining.</returns>
    public ParallelOptionsBuilder BlockOnBackpressure()
    {
        _queuePolicy = BoundedQueuePolicy.Block;
        return this;
    }

    /// <summary>
    ///     Sets the maximum number of processed results that can be buffered ahead of downstream consumption.
    ///     Only applies when using the Block policy with ordered output.
    /// </summary>
    /// <param name="value">The output buffer capacity (must be positive).</param>
    /// <returns>This builder for method chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when value is not positive.</exception>
    public ParallelOptionsBuilder OutputBufferCapacity(int value)
    {
        if (value <= 0)
            throw new ArgumentOutOfRangeException(nameof(value), "Output buffer capacity must be positive.");

        _outputBufferCapacity = value;
        return this;
    }

    /// <summary>
    ///     Allows unordered output, which can improve throughput at the cost of input ordering.
    ///     By default, input ordering is preserved in the output.
    /// </summary>
    /// <returns>This builder for method chaining.</returns>
    public ParallelOptionsBuilder AllowUnorderedOutput()
    {
        _preserveOrdering = false;
        return this;
    }

    /// <summary>
    ///     Sets the interval at which metrics are emitted for this parallel node.
    /// </summary>
    /// <param name="interval">The metrics emission interval.</param>
    /// <returns>This builder for method chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when interval is not positive.</exception>
    public ParallelOptionsBuilder MetricsInterval(TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(interval), "Metrics interval must be positive.");

        _metricsInterval = interval;
        return this;
    }

    /// <summary>
    ///     Builds the ParallelOptions with the configured settings.
    /// </summary>
    /// <returns>A new ParallelOptions instance.</returns>
    public ParallelOptions Build()
    {
        return new ParallelOptions(
            _maxDegreeOfParallelism,
            _maxQueueLength,
            _queuePolicy,
            _outputBufferCapacity,
            _preserveOrdering,
            _metricsInterval);
    }
}

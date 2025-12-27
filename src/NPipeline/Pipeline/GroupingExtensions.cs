using NPipeline.Configuration;
using NPipeline.DataFlow.Timestamping;
using NPipeline.DataFlow.Windowing;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Pipeline;

/// <summary>
///     Extension methods for grouping operations that guide users toward correct choices.
/// </summary>
public static class GroupingExtensions
{
    /// <summary>
    ///     Starts a grouping configuration. Choose one of the ForXxx methods to declare intent.
    /// </summary>
    /// <typeparam name="T">The type of items to group.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <returns>A <see cref="GroupingBuilder{T}" /> to configure the grouping operation.</returns>
    /// <remarks>
    ///     This API guides you toward the correct grouping strategy by requiring explicit intent declaration.
    ///     Use <see cref="GroupingBuilder{T}.ForOperationalEfficiency" /> when you need to batch items
    ///     to reduce I/O overhead (e.g., bulk database inserts, batch API calls).
    ///     Use <see cref="GroupingBuilder{T}.ForTemporalCorrectness{TKey,TResult}" /> when data timing
    ///     is critical and you need time-based windowing (e.g., hourly aggregations, session analysis).
    /// </remarks>
    /// <example>
    ///     <code>
    /// // For operational efficiency (batching):
    /// var batcher = builder.GroupItems&lt;Order&gt;()
    ///     .ForOperationalEfficiency(batchSize: 100, maxWait: TimeSpan.FromSeconds(5));
    /// 
    /// // For temporal correctness (aggregation):
    /// var aggregator = builder.GroupItems&lt;Sale&gt;()
    ///     .ForTemporalCorrectness(
    ///         windowSize: TimeSpan.FromHours(1),
    ///         keySelector: sale => sale.Category,
    ///         initialValue: () => 0m,
    ///         accumulator: (sum, sale) => sum + sale.Amount);
    /// </code>
    /// </example>
    public static GroupingBuilder<T> GroupItems<T>(this PipelineBuilder builder)
    {
        return new GroupingBuilder<T>(builder);
    }
}

/// <summary>
///     Builder that requires explicit intent declaration for grouping operations.
/// </summary>
/// <typeparam name="T">The type of items to group.</typeparam>
public sealed class GroupingBuilder<T>
{
    private readonly PipelineBuilder _builder;

    internal GroupingBuilder(PipelineBuilder builder)
    {
        _builder = builder;
    }

    /// <summary>
    ///     Group items for operational efficiency (e.g., bulk database inserts, batch API calls).
    ///     Items are grouped by count or time interval, whichever comes first.
    /// </summary>
    /// <param name="batchSize">Maximum items per batch.</param>
    /// <param name="maxWait">Maximum time to wait before emitting a partial batch.</param>
    /// <param name="name">Optional node name.</param>
    /// <returns>A handle to the batching node.</returns>
    /// <remarks>
    ///     <para>
    ///         Use this when you need to reduce I/O overhead by processing items in batches.
    ///         The batching is purely for performance optimization and does not provide temporal correctness guarantees.
    ///     </para>
    ///     <para>
    ///         <strong>Example use cases:</strong>
    ///     </para>
    ///     <list type="bullet">
    ///         <item>Bulk inserts to a database (e.g., 100 rows per INSERT statement)</item>
    ///         <item>Batch API calls to reduce HTTP request overhead</item>
    ///         <item>File writes where buffering improves performance</item>
    ///         <item>Message queue publishing in batches</item>
    ///     </list>
    ///     <para>
    ///         <strong>Important:</strong> Do NOT use this for temporal correctness requirements like
    ///         "calculate hourly averages" - use <see cref="ForTemporalCorrectness{TKey,TResult}" /> instead.
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    /// // Batch orders for bulk database insert
    /// var batcher = builder.GroupItems&lt;Order&gt;()
    ///     .ForOperationalEfficiency(
    ///         batchSize: 100,
    ///         maxWait: TimeSpan.FromSeconds(5),
    ///         name: "order-batcher");
    /// 
    /// // Connect to bulk insert sink
    /// var bulkInsert = builder.AddSink&lt;BulkInsertSink&lt;Order&gt;, IReadOnlyCollection&lt;Order&gt;&gt;("bulk-insert");
    /// builder.Connect(batcher, bulkInsert);
    /// </code>
    /// </example>
    public TransformNodeHandle<T, IReadOnlyCollection<T>> ForOperationalEfficiency(
        int batchSize,
        TimeSpan maxWait,
        string? name = null)
    {
        return _builder.AddBatcher<T>(
            name ?? $"batch_{batchSize}x{maxWait.TotalSeconds}s",
            batchSize,
            maxWait);
    }

    /// <summary>
    ///     Group items for temporal correctness with time-based tumbling windows.
    ///     Use this when data timing is critical (e.g., calculating hourly averages, session-based aggregation).
    /// </summary>
    /// <typeparam name="TKey">The grouping key type.</typeparam>
    /// <typeparam name="TResult">The aggregation result type.</typeparam>
    /// <param name="windowSize">The size of each time window.</param>
    /// <param name="keySelector">Function to extract the grouping key from each item.</param>
    /// <param name="initialValue">Factory function that creates the initial accumulator value for each group.</param>
    /// <param name="accumulator">Function to accumulate items into the result.</param>
    /// <param name="timestampExtractor">
    ///     Optional function to extract timestamps from items.
    ///     If null, arrival time is used (not recommended for late-arriving data).
    /// </param>
    /// <param name="name">Optional node name.</param>
    /// <returns>A handle to the aggregate node.</returns>
    /// <remarks>
    ///     <para>
    ///         Tumbling windows are non-overlapping, fixed-size time windows. Each item belongs to exactly one window.
    ///         Use this when you need aggregations over distinct time periods (e.g., hourly totals, daily summaries).
    ///     </para>
    ///     <para>
    ///         <strong>Example use cases:</strong>
    ///     </para>
    ///     <list type="bullet">
    ///         <item>Hourly sales totals by product category</item>
    ///         <item>5-minute average sensor readings by sensor ID</item>
    ///         <item>Daily user activity counts by region</item>
    ///         <item>Per-minute request rates by endpoint</item>
    ///     </list>
    ///     <para>
    ///         <strong>Important:</strong> Always provide a <paramref name="timestampExtractor" /> if your data
    ///         contains embedded timestamps and you need to handle late-arriving events correctly.
    ///         Using arrival time can lead to incorrect aggregations if data arrives out of order.
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    /// // Calculate hourly sales totals by category
    /// var aggregator = builder.GroupItems&lt;Sale&gt;()
    ///     .ForTemporalCorrectness(
    ///         windowSize: TimeSpan.FromHours(1),
    ///         keySelector: sale => sale.Category,
    ///         initialValue: () => 0m,
    ///         accumulator: (sum, sale) => sum + sale.Amount,
    ///         timestampExtractor: sale => sale.Timestamp);
    /// 
    /// // Calculate 5-minute average temperature by sensor
    /// var tempAvg = builder.GroupItems&lt;SensorReading&gt;()
    ///     .ForTemporalCorrectness(
    ///         windowSize: TimeSpan.FromMinutes(5),
    ///         keySelector: reading => reading.SensorId,
    ///         initialValue: () => new { Sum = 0.0, Count = 0 },
    ///         accumulator: (acc, reading) => new { Sum = acc.Sum + reading.Temperature, Count = acc.Count + 1 },
    ///         timestampExtractor: reading => reading.RecordedAt)
    ///     .Select(group => group.Sum / group.Count);
    /// </code>
    /// </example>
    public AggregateNodeHandle<T, TResult> ForTemporalCorrectness<TKey, TResult>(
        TimeSpan windowSize,
        Func<T, TKey> keySelector,
        Func<TResult> initialValue,
        Func<TResult, T, TResult> accumulator,
        Func<T, DateTimeOffset>? timestampExtractor = null,
        string? name = null)
        where TKey : notnull
    {
        var config = new AggregateNodeConfiguration<T>(
            WindowAssigner.Tumbling(windowSize),
            timestampExtractor != null
                ? new TimestampExtractor<T>(timestampExtractor)
                : null);

        var nodeName = name ?? $"aggregate_{windowSize.TotalSeconds}s";
        var handle = _builder.AddAggregate<LambdaAggregateNode<T, TKey, TResult>, T, TKey, TResult>(nodeName);

        var node = new LambdaAggregateNode<T, TKey, TResult>(config, keySelector, initialValue, accumulator);
        _builder.RegisterBuilderDisposable(node);
        _builder.AddPreconfiguredNodeInstance(handle.Id, node);

        return handle;
    }

    /// <summary>
    ///     Group items for temporal correctness with sliding (overlapping) windows.
    ///     Use this when you need rolling aggregations (e.g., moving averages, rolling counts).
    /// </summary>
    /// <typeparam name="TKey">The grouping key type.</typeparam>
    /// <typeparam name="TResult">The aggregation result type.</typeparam>
    /// <param name="windowSize">The size of each time window.</param>
    /// <param name="slideInterval">
    ///     The interval at which windows slide (must be less than <paramref name="windowSize" />).
    /// </param>
    /// <param name="keySelector">Function to extract the grouping key from each item.</param>
    /// <param name="initialValue">Factory function that creates the initial accumulator value for each group.</param>
    /// <param name="accumulator">Function to accumulate items into the result.</param>
    /// <param name="timestampExtractor">
    ///     Optional function to extract timestamps from items.
    ///     If null, arrival time is used (not recommended for late-arriving data).
    /// </param>
    /// <param name="name">Optional node name.</param>
    /// <returns>A handle to the aggregate node.</returns>
    /// <remarks>
    ///     <para>
    ///         Sliding windows are overlapping, fixed-size time windows. Each item can belong to multiple windows.
    ///         Use this when you need continuous aggregations with overlapping time periods (e.g., rolling averages).
    ///     </para>
    ///     <para>
    ///         <strong>Example use cases:</strong>
    ///     </para>
    ///     <list type="bullet">
    ///         <item>5-minute rolling average of the last 15 minutes of data</item>
    ///         <item>Sliding window anomaly detection (e.g., count in last N minutes)</item>
    ///         <item>Moving percentiles for real-time monitoring</item>
    ///         <item>Continuous rate calculations with overlapping periods</item>
    ///     </list>
    ///     <para>
    ///         <strong>Performance note:</strong> Sliding windows generate more output than tumbling windows
    ///         because items participate in multiple windows. Each slide creates new window results.
    ///     </para>
    ///     <para>
    ///         <strong>Important:</strong> Always provide a <paramref name="timestampExtractor" /> if your data
    ///         contains embedded timestamps and you need to handle late-arriving events correctly.
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    /// // Calculate 15-minute rolling average, sliding every 5 minutes
    /// var rollingAvg = builder.GroupItems&lt;Metric&gt;()
    ///     .ForRollingWindow(
    ///         windowSize: TimeSpan.FromMinutes(15),
    ///         slideInterval: TimeSpan.FromMinutes(5),
    ///         keySelector: metric => metric.MetricName,
    ///         initialValue: () => new { Sum = 0.0, Count = 0 },
    ///         accumulator: (acc, metric) => new { Sum = acc.Sum + metric.Value, Count = acc.Count + 1 },
    ///         timestampExtractor: metric => metric.Timestamp)
    ///     .Select(group => group.Sum / group.Count);
    /// 
    /// // Count requests in last 10 minutes, sliding every 1 minute
    /// var requestRate = builder.GroupItems&lt;Request&gt;()
    ///     .ForRollingWindow(
    ///         windowSize: TimeSpan.FromMinutes(10),
    ///         slideInterval: TimeSpan.FromMinutes(1),
    ///         keySelector: req => req.Endpoint,
    ///         initialValue: () => 0,
    ///         accumulator: (count, req) => count + 1,
    ///         timestampExtractor: req => req.ReceivedAt);
    /// </code>
    /// </example>
    public AggregateNodeHandle<T, TResult> ForRollingWindow<TKey, TResult>(
        TimeSpan windowSize,
        TimeSpan slideInterval,
        Func<T, TKey> keySelector,
        Func<TResult> initialValue,
        Func<TResult, T, TResult> accumulator,
        Func<T, DateTimeOffset>? timestampExtractor = null,
        string? name = null)
        where TKey : notnull
    {
        var config = new AggregateNodeConfiguration<T>(
            WindowAssigner.Sliding(windowSize, slideInterval),
            timestampExtractor != null
                ? new TimestampExtractor<T>(timestampExtractor)
                : null);

        var nodeName = name ?? $"sliding_{windowSize.TotalSeconds}s_by_{slideInterval.TotalSeconds}s";
        var handle = _builder.AddAggregate<LambdaAggregateNode<T, TKey, TResult>, T, TKey, TResult>(nodeName);

        var node = new LambdaAggregateNode<T, TKey, TResult>(config, keySelector, initialValue, accumulator);
        _builder.RegisterBuilderDisposable(node);
        _builder.AddPreconfiguredNodeInstance(handle.Id, node);

        return handle;
    }
}

/// <summary>
///     Lambda-based aggregate node for use with the intent-driven grouping API.
/// </summary>
/// <typeparam name="T">The input item type.</typeparam>
/// <typeparam name="TKey">The grouping key type.</typeparam>
/// <typeparam name="TResult">The aggregation result type.</typeparam>
internal sealed class LambdaAggregateNode<T, TKey, TResult> : AggregateNode<T, TKey, TResult>
    where TKey : notnull
{
    private readonly Func<TResult, T, TResult> _accumulator;
    private readonly Func<TResult> _initialValue;
    private readonly Func<T, TKey> _keySelector;

    public LambdaAggregateNode(
        AggregateNodeConfiguration<T> config,
        Func<T, TKey> keySelector,
        Func<TResult> initialValue,
        Func<TResult, T, TResult> accumulator)
        : base(config)
    {
        _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
        _initialValue = initialValue ?? throw new ArgumentNullException(nameof(initialValue));
        _accumulator = accumulator ?? throw new ArgumentNullException(nameof(accumulator));
    }

    public override TKey GetKey(T item)
    {
        return _keySelector(item);
    }

    public override TResult CreateAccumulator()
    {
        return _initialValue();
    }

    public override TResult Accumulate(TResult acc, T item)
    {
        return _accumulator(acc, item);
    }
}

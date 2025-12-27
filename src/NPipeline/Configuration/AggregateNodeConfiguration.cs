using System.Collections.Concurrent;
using NPipeline.DataFlow.Timestamping;
using NPipeline.DataFlow.Windowing;

namespace NPipeline.Configuration;

/// <summary>
///     Configuration for aggregate nodes controlling windowing and watermark behavior.
/// </summary>
/// <typeparam name="TIn">The type of input items to be aggregated.</typeparam>
/// <param name="WindowAssigner">
///     The window assigner strategy to use for defining time windows (required).
/// </param>
/// <param name="TimestampExtractor">
///     Optional timestamp extractor for the input type. If not provided, system time of arrival will be used.
///     Default is null.
/// </param>
/// <param name="MaxOutOfOrderness">
///     Maximum time span for out-of-order items. Events arriving later than this relative to the current watermark
///     may be treated as late. Default is 5 minutes.
/// </param>
/// <param name="WatermarkInterval">
///     Interval for watermark updates. Watermarks advance event time and trigger window cleanup.
///     Default is 30 seconds.
/// </param>
/// <param name="UseThreadSafeAccumulator">
///     If true, uses thread-safe <see cref="ConcurrentDictionary{TKey,TValue}" /> for storing accumulators.
///     If false, uses regular <see cref="Dictionary{TKey, TValue}" /> with no synchronization.
///     Set to false in single-threaded scenarios for better performance.
///     Default is true for safety in multi-threaded pipelines.
/// </param>
public sealed record AggregateNodeConfiguration<TIn>(
    WindowAssigner WindowAssigner,
    TimestampExtractor<TIn>? TimestampExtractor = null,
    TimeSpan? MaxOutOfOrderness = null,
    TimeSpan? WatermarkInterval = null,
    bool UseThreadSafeAccumulator = true)
{
    /// <summary>
    ///     Gets the effective maximum out-of-orderness value, using the default if not specified.
    /// </summary>
    public TimeSpan EffectiveMaxOutOfOrderness => MaxOutOfOrderness ?? TimeSpan.FromMinutes(5);

    /// <summary>
    ///     Gets the effective watermark interval value, using the default if not specified.
    /// </summary>
    public TimeSpan EffectiveWatermarkInterval => WatermarkInterval ?? TimeSpan.FromSeconds(30);
}

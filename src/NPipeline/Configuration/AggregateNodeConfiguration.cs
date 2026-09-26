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
///     How far behind the latest event time an item may arrive and still be counted. The watermark trails the latest
///     event time by this much, and is re-evaluated on every item. Must not be negative. Default is 5 minutes.
/// </param>
public sealed record AggregateNodeConfiguration<TIn>(
    WindowAssigner WindowAssigner,
    TimestampExtractor<TIn>? TimestampExtractor = null,
    TimeSpan? MaxOutOfOrderness = null)
{
    /// <summary>
    ///     Gets the effective maximum out-of-orderness value, using the default if not specified.
    /// </summary>
    public TimeSpan EffectiveMaxOutOfOrderness => MaxOutOfOrderness ?? TimeSpan.FromMinutes(5);
}

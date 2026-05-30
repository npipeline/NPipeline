namespace NPipeline.Observability;

/// <summary>
///     Per-node timing buckets used for exclusive timing attribution.
/// </summary>
/// <param name="WorkDuration">Time spent doing node-owned work.</param>
/// <param name="InputWaitDuration">Time spent waiting for upstream input.</param>
/// <param name="OutputBlockDuration">Time spent blocked by downstream output pressure.</param>
/// <param name="WallDuration">Total elapsed node dataflow time.</param>
public readonly record struct NodeTimingBreakdown(
    TimeSpan WorkDuration,
    TimeSpan InputWaitDuration,
    TimeSpan OutputBlockDuration,
    TimeSpan WallDuration)
{
    /// <summary>
    ///     Empty timing breakdown.
    /// </summary>
    public static NodeTimingBreakdown Empty { get; } = new(TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero);
}
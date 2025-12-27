namespace NPipeline.Configuration;

/// <summary>
///     Options controlling lineage behavior.
/// </summary>
/// <param name="Strict">
///     When true, any detected One-To-One mismatch throws instead of logging.
///     Default is false.
/// </param>
/// <param name="WarnOnMismatch">
///     When true (default) a warning is logged for mismatches (if Strict=false).
///     Default is true.
/// </param>
/// <param name="OnMismatch">
///     Optional callback invoked on mismatch (after logging / before throw).
///     Default is null.
/// </param>
/// <param name="MaterializationCap">
///     Optional cap (maximum) on the number of items to materialize for lineage mapping in non-1:1 scenarios.
///     When set, overflow behavior is defined by <see cref="OverflowPolicy" />. Null means unbounded.
///     Default is null.
/// </param>
/// <param name="OverflowPolicy">
///     Policy applied when <see cref="MaterializationCap" /> is exceeded.
///     Default is Degrade (switch to streaming positional mapping).
/// </param>
/// <param name="CaptureHopTimestamps">
///     Capture per-hop enter/exit timestamps in streaming 1:1 paths.
///     Default is true.
/// </param>
/// <param name="CaptureDecisions">
///     Capture decision outcomes (e.g., Emitted, FilteredOut, Joined, Aggregated).
///     Default is true.
/// </param>
/// <param name="CaptureObservedCardinality">
///     Capture observed cardinality (Zero/One/Many) and basic contributor/emission counts.
///     Default is true.
/// </param>
/// <param name="CaptureAncestryMapping">
///     Capture ancestry mapping when an ILineageMapper is declared and materialization allows it.
///     Default is false.
/// </param>
/// <param name="SampleEvery">
///     Deterministic sampling rate for item-level lineage collection. 1 means all items.
///     Default is 100 (1/100).
/// </param>
/// <param name="DeterministicSampling">
///     When true, sampling is performed deterministically using LineageId hashing.
///     Default is true.
/// </param>
/// <param name="RedactData">
///     When true, payload Data is omitted (null) in emitted LineageInfo records.
///     Default is true.
/// </param>
/// <param name="MaxHopRecordsPerItem">
///     Maximum number of hop records retained per item. Additional hops are truncated.
///     Default is 256.
/// </param>
public sealed record LineageOptions(
    bool Strict = false,
    bool WarnOnMismatch = true,
    Action<LineageMismatchContext>? OnMismatch = null,
    int? MaterializationCap = null,
    LineageOverflowPolicy OverflowPolicy = LineageOverflowPolicy.Degrade,
    bool CaptureHopTimestamps = true,
    bool CaptureDecisions = true,
    bool CaptureObservedCardinality = true,
    bool CaptureAncestryMapping = false,
    int SampleEvery = 100,
    bool DeterministicSampling = true,
    bool RedactData = true,
    int MaxHopRecordsPerItem = 256)
{
    /// <summary>
    ///     Default lineage options with sensible defaults for most use cases.
    /// </summary>
    public static LineageOptions Default { get; } = new();

    /// <summary>
    ///     Creates a new instance with updated options, preserving unspecified values.
    /// </summary>
    public LineageOptions With(
        bool? strict = null,
        bool? warnOnMismatch = null,
        Action<LineageMismatchContext>? onMismatch = null,
        int? materializationCap = null,
        LineageOverflowPolicy? overflowPolicy = null,
        bool? captureHopTimestamps = null,
        bool? captureDecisions = null,
        bool? captureObservedCardinality = null,
        bool? captureAncestryMapping = null,
        int? sampleEvery = null,
        bool? deterministicSampling = null,
        bool? redactData = null,
        int? maxHopRecordsPerItem = null)
    {
        return new LineageOptions(
            strict ?? Strict,
            warnOnMismatch ?? WarnOnMismatch,
            onMismatch ?? OnMismatch,
            materializationCap ?? MaterializationCap,
            overflowPolicy ?? OverflowPolicy,
            captureHopTimestamps ?? CaptureHopTimestamps,
            captureDecisions ?? CaptureDecisions,
            captureObservedCardinality ?? CaptureObservedCardinality,
            captureAncestryMapping ?? CaptureAncestryMapping,
            sampleEvery ?? SampleEvery,
            deterministicSampling ?? DeterministicSampling,
            redactData ?? RedactData,
            maxHopRecordsPerItem ?? MaxHopRecordsPerItem);
    }
}

/// <summary>
///     Behavior when lineage materialization exceeds configured cap.
/// </summary>
public enum LineageOverflowPolicy
{
    /// <summary>
    ///     Switch to streaming positional mapping beyond the cap and continue processing.
    /// </summary>
    Degrade = 0,

    /// <summary>
    ///     Throw immediately when cap is exceeded.
    /// </summary>
    Strict = 1,

    /// <summary>
    ///     Emit warnings/mismatch context and continue; may continue materializing (risk of memory growth).
    /// </summary>
    WarnContinue = 2,
}

/// <summary>
///     Provides details about a detected lineage cardinality mismatch.
/// </summary>
/// <param name="NodeId">The identifier of the node where the mismatch was detected.</param>
/// <param name="InputCount">The total number of inputs expected by the node.</param>
/// <param name="OutputCount">The total number of outputs produced by the node.</param>
/// <param name="MissingInputIndices">Indices of inputs that were expected but not received.</param>
/// <param name="ExtraOutputIndices">Indices of outputs that were produced but not expected.</param>
/// <param name="AggregatedOutputs">Collection of aggregated output groups that map multiple inputs to single outputs.</param>
public sealed record LineageMismatchContext(
    string NodeId,
    int InputCount,
    int OutputCount,
    IReadOnlyList<int> MissingInputIndices,
    IReadOnlyList<int> ExtraOutputIndices,
    IReadOnlyList<LineageAggregatedGroup> AggregatedOutputs);

/// <summary>
///     Represents a group where multiple inputs have been aggregated into a single output.
/// </summary>
/// <param name="OutputIndex">The index of the output that represents the aggregation.</param>
/// <param name="InputIndices">The collection of input indices that were aggregated to produce the output.</param>
public sealed record LineageAggregatedGroup(int OutputIndex, IReadOnlyList<int> InputIndices);

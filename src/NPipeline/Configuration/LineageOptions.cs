namespace NPipeline.Configuration;

/// <summary>
///     Options controlling lineage behavior.
/// </summary>
public sealed class LineageOptions
{
    /// <summary>
    ///     When true, any detected One-To-One mismatch throws instead of logging.
    /// </summary>
    public bool Strict { get; set; }

    /// <summary>
    ///     When true (default) a warning is logged for mismatches (if Strict=false).
    /// </summary>
    public bool WarnOnMismatch { get; set; } = true;

    /// <summary>
    ///     Optional callback invoked on mismatch (after logging / before throw).
    /// </summary>
    public Action<LineageMismatchContext>? OnMismatch { get; set; }

    /// <summary>
    ///     Optional cap (maximum) on the number of items to materialize for lineage mapping in non-1:1 scenarios.
    ///     When set, overflow behavior is defined by <see cref="OverflowPolicy" />. Null means unbounded.
    /// </summary>
    public int? MaterializationCap { get; set; }

    /// <summary>
    ///     Policy applied when <see cref="MaterializationCap" /> is exceeded.
    ///     Default is Degrade (switch to streaming positional mapping).
    /// </summary>
    public LineageOverflowPolicy OverflowPolicy { get; set; } = LineageOverflowPolicy.Degrade;

    // Deep-debugging capture configuration

    /// <summary>
    ///     Capture per-hop enter/exit timestamps in streaming 1:1 paths. Default true.
    /// </summary>
    public bool CaptureHopTimestamps { get; set; } = true;

    /// <summary>
    ///     Capture decision outcomes (e.g., Emitted, FilteredOut, Joined, Aggregated). Default true.
    /// </summary>
    public bool CaptureDecisions { get; set; } = true;

    /// <summary>
    ///     Capture observed cardinality (Zero/One/Many) and basic contributor/emission counts. Default true.
    /// </summary>
    public bool CaptureObservedCardinality { get; set; } = true;

    /// <summary>
    ///     Capture ancestry mapping when an ILineageMapper is declared and materialization allows it. Default false.
    /// </summary>
    public bool CaptureAncestryMapping { get; set; }

    /// <summary>
    ///     Deterministic sampling rate for item-level lineage collection. 1 means all items. Default 100 (1/100).
    /// </summary>
    public int SampleEvery { get; set; } = 100;

    /// <summary>
    ///     When true, sampling is performed deterministically using LineageId hashing. Default true.
    /// </summary>
    public bool DeterministicSampling { get; set; } = true;

    /// <summary>
    ///     When true, payload Data is omitted (null) in emitted LineageInfo records. Default true.
    /// </summary>
    public bool RedactData { get; set; } = true;

    /// <summary>
    ///     Maximum number of hop records retained per item. Additional hops are truncated. Default 256.
    /// </summary>
    public int MaxHopRecordsPerItem { get; set; } = 256;
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

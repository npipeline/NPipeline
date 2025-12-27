using NPipeline.Nodes;

namespace NPipeline.Execution;

/// <summary>
///     Defines the available strategies for merging multiple input streams into a single node.
/// </summary>
public enum MergeType
{
    /// <summary>
    ///     Processes items as they arrive from any source, interleaving them. This is the default.
    ///     This strategy provides the best responsiveness for real-time or mixed workloads.
    /// </summary>
    Interleave,

    /// <summary>
    ///     Processes all items from the first input stream to completion before moving to the next.
    ///     This strategy preserves the order of items within each source stream but can introduce latency.
    /// </summary>
    Concatenate,

    /// <summary>
    ///     Joins items from two or more streams based on a shared key.
    ///     This is a stateful operation that holds items in memory until a match is found.
    ///     The node using this strategy is expected to receive a tuple of the joined items.
    /// </summary>
    KeyedJoin,

    /// <summary>
    ///     Indicates that the node provides its own custom merge logic by implementing <see cref="ICustomMergeNode{TIn}" />.
    ///     When this is used, the runner will delegate merging to the node's <c>MergeAsync</c> method.
    /// </summary>
    Custom,
}

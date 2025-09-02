using NPipeline.DataFlow.Branching;

namespace NPipeline.Execution;

/// <summary>
///     Encapsulates execution-related options for a single node.
/// </summary>
public sealed class NodeExecutionOptions
{
    /// <summary>
    ///     The merge capacity for a join node.
    /// </summary>
    public int? MergeCapacity { get; init; }

    /// <summary>
    ///     The branching options for a node with multiple outputs.
    /// </summary>
    public BranchOptions? BranchOptions { get; init; }
}

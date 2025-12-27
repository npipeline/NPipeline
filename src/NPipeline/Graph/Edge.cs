namespace NPipeline.Graph;

/// <summary>
///     Represents a directed edge connecting two nodes in the pipeline graph.
/// </summary>
/// <param name="SourceNodeId">The ID of the source node.</param>
/// <param name="TargetNodeId">The ID of the target node.</param>
/// <param name="SourceOutputName">The name of the source node's output port (optional, for nodes with multiple outputs).</param>
/// <param name="TargetInputName">The name of the target node's input port (optional, for nodes with multiple inputs).</param>
public sealed record Edge(
    string SourceNodeId,
    string TargetNodeId,
    string? SourceOutputName = null,
    string? TargetInputName = null);

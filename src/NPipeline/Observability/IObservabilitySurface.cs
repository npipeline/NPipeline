using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Observability;

/// <summary>
///     Central surface for all runner-level observability (logging, tracing, execution observer events).
///     Keeps hot path reflection-free and concentrates formatting logic and tag emission.
/// </summary>
public interface IObservabilitySurface
{
    /// <summary>
    ///     Begins a pipeline run and returns the created activity.
    /// </summary>
    IPipelineActivity BeginPipeline<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new();

    /// <summary>
    ///     Records successful pipeline completion (branch metrics, final log) and disposes nothing (caller disposes activity scope).
    /// </summary>
    void CompletePipeline<TDefinition>(PipelineContext context, PipelineGraph graph, IPipelineActivity pipelineActivity)
        where TDefinition : IPipelineDefinition, new();

    /// <summary>
    ///     Records a pipeline failure (logs + activity exception).
    /// </summary>
    void FailPipeline<TDefinition>(PipelineContext context, Exception ex, IPipelineActivity pipelineActivity) where TDefinition : IPipelineDefinition, new();

    /// <summary>
    ///     Starts node execution, returning a scope with timing and activity.
    /// </summary>
    NodeObservationScope BeginNode(PipelineContext context, NodeDefinition nodeDef, INode nodeInstance);

    /// <summary>
    ///     Records node success and returns the NodeExecutionCompleted event for downstream persistence.
    /// </summary>
    NodeExecutionCompleted CompleteNodeSuccess(PipelineContext context, NodeObservationScope scope);

    /// <summary>
    ///     Records node failure and returns the NodeExecutionCompleted event for downstream persistence.
    /// </summary>
    NodeExecutionCompleted CompleteNodeFailure(PipelineContext context, NodeObservationScope scope, Exception ex);
}

/// <summary>
///     Lightweight struct capturing node timing + activity for observability.
/// </summary>
public readonly struct NodeObservationScope(string nodeId, string nodeType, DateTimeOffset startTime, IPipelineActivity activity)
    : IEquatable<NodeObservationScope>
{
    public string NodeId { get; } = nodeId;
    public string NodeType { get; } = nodeType;
    public DateTimeOffset StartTime { get; } = startTime;
    public IPipelineActivity Activity { get; } = activity;

    public override bool Equals(object? obj)
    {
        return obj is NodeObservationScope other && Equals(other);
    }

    public bool Equals(NodeObservationScope other)
    {
        return NodeId == other.NodeId &&
               NodeType == other.NodeType &&
               StartTime.Equals(other.StartTime) &&
               Equals(Activity, other.Activity);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(NodeId, NodeType, StartTime, Activity);
    }

    public static bool operator ==(NodeObservationScope left, NodeObservationScope right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(NodeObservationScope left, NodeObservationScope right)
    {
        return !left.Equals(right);
    }
}

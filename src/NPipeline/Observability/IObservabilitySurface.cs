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
    /// <summary>
    ///     Gets the unique identifier of the node being observed.
    /// </summary>
    public string NodeId { get; } = nodeId;

    /// <summary>
    ///     Gets the type name of the node being observed.
    /// </summary>
    public string NodeType { get; } = nodeType;

    /// <summary>
    ///     Gets the timestamp when the node execution started.
    /// </summary>
    public DateTimeOffset StartTime { get; } = startTime;

    /// <summary>
    ///     Gets the pipeline activity associated with this node observation.
    /// </summary>
    public IPipelineActivity Activity { get; } = activity;

    /// <summary>
    ///     Determines whether the specified object is equal to the current <see cref="NodeObservationScope" />.
    /// </summary>
    /// <param name="obj">The object to compare with the current <see cref="NodeObservationScope" />.</param>
    /// <returns>true if the specified object is equal to the current <see cref="NodeObservationScope" />; otherwise, false.</returns>
    public override bool Equals(object? obj)
    {
        return obj is NodeObservationScope other && Equals(other);
    }

    /// <summary>
    ///     Determines whether the specified <see cref="NodeObservationScope" /> is equal to the current <see cref="NodeObservationScope" />.
    /// </summary>
    /// <param name="other">The <see cref="NodeObservationScope" /> to compare with the current <see cref="NodeObservationScope" />.</param>
    /// <returns>true if the specified <see cref="NodeObservationScope" /> is equal to the current <see cref="NodeObservationScope" />; otherwise, false.</returns>
    public bool Equals(NodeObservationScope other)
    {
        return NodeId == other.NodeId &&
               NodeType == other.NodeType &&
               StartTime.Equals(other.StartTime) &&
               Equals(Activity, other.Activity);
    }

    /// <summary>
    ///     Returns the hash code for the current <see cref="NodeObservationScope" />.
    /// </summary>
    /// <returns>A hash code for the current <see cref="NodeObservationScope" />.</returns>
    public override int GetHashCode()
    {
        return HashCode.Combine(NodeId, NodeType, StartTime, Activity);
    }

    /// <summary>
    ///     Determines whether two <see cref="NodeObservationScope" /> instances are equal.
    /// </summary>
    /// <param name="left">The first <see cref="NodeObservationScope" /> to compare.</param>
    /// <param name="right">The second <see cref="NodeObservationScope" /> to compare.</param>
    /// <returns>true if the instances are equal; otherwise, false.</returns>
    public static bool operator ==(NodeObservationScope left, NodeObservationScope right)
    {
        return left.Equals(right);
    }

    /// <summary>
    ///     Determines whether two <see cref="NodeObservationScope" /> instances are not equal.
    /// </summary>
    /// <param name="left">The first <see cref="NodeObservationScope" /> to compare.</param>
    /// <param name="right">The second <see cref="NodeObservationScope" /> to compare.</param>
    /// <returns>true if the instances are not equal; otherwise, false.</returns>
    public static bool operator !=(NodeObservationScope left, NodeObservationScope right)
    {
        return !left.Equals(right);
    }
}

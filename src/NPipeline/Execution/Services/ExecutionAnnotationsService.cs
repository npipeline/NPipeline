using NPipeline.DataFlow.Branching;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;

namespace NPipeline.Execution.Services;

/// <summary>
///     A service for resolving execution annotations from a pipeline graph.
/// </summary>
public sealed class ExecutionAnnotationsService : IExecutionAnnotationsService
{
    /// <inheritdoc />
    public NodeExecutionOptions GetOptions(PipelineGraph graph, string nodeId)
    {
        var branchOptions = GetBranchOptions(graph, nodeId);
        var mergeCapacity = GetMergeCapacity(graph, nodeId);

        return new NodeExecutionOptions
        {
            BranchOptions = branchOptions,
            MergeCapacity = mergeCapacity,
        };
    }

    private static BranchOptions? GetBranchOptions(PipelineGraph graph, string nodeId)
    {
        if (graph.ExecutionOptions.NodeExecutionAnnotations is not null &&
            graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(ExecutionAnnotationKeys.BranchOptionsForNode(nodeId), out var fo) &&
            fo is BranchOptions f)
            return f;

        if (graph.ExecutionOptions.NodeExecutionAnnotations is not null &&
            graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(ExecutionAnnotationKeys.GlobalBranchingCapacity, out var gcap) &&
            gcap is int gc and > 0)
            return new BranchOptions(gc);

        return null;
    }

    private static int? GetMergeCapacity(PipelineGraph graph, string nodeId)
    {
        if (graph.ExecutionOptions.NodeExecutionAnnotations != null)
        {
            if (graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(ExecutionAnnotationKeys.MergeCapacityForNode(nodeId), out var capObj) &&
                capObj is int cap1)
                return cap1;

            if (graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(ExecutionAnnotationKeys.GlobalMergeCapacity, out var gcapObj) &&
                gcapObj is int cap2)
                return cap2;
        }

        return null;
    }
}

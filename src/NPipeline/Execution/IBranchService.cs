using NPipeline.DataFlow;
using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Service for handling branching operations with multicast capabilities.
/// </summary>
public interface IBranchService
{
    /// <summary>
    ///     Wraps a data pipe in multicast if it has multiple consumers.
    /// </summary>
    /// <param name="pipe">The data pipe to potentially multicast.</param>
    /// <param name="graph">The pipeline graph containing execution annotations.</param>
    /// <param name="nodeId">The node ID for which to check branch options.</param>
    /// <param name="context">The pipeline context for metrics tracking.</param>
    /// <returns>The original pipe or a multicast wrapper if needed.</returns>
    IDataPipe MaybeMulticast(IDataPipe pipe, PipelineGraph graph, string nodeId, PipelineContext context);
}

using NPipeline.DataFlow.Branching;

namespace NPipeline.DataFlow.DataStreams;

/// <summary>
///     Implemented by data streams that expose <see cref="BranchMetrics" /> for a fan-out (branch or route) wrapper.
/// </summary>
internal interface IHasBranchMetrics
{
    BranchMetrics Metrics { get; }
}

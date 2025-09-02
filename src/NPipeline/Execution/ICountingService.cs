using NPipeline.DataFlow;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Provides a service for wrapping data pipes with counting functionality.
/// </summary>
public interface ICountingService
{
    /// <summary>
    ///     Wraps a data pipe to count the items that flow through it.
    /// </summary>
    /// <param name="pipe">The data pipe to wrap.</param>
    /// <param name="context">The pipeline context, used to access the global item counter.</param>
    /// <returns>A new data pipe that includes counting functionality.</returns>
    IDataPipe Wrap(IDataPipe pipe, PipelineContext context);
}

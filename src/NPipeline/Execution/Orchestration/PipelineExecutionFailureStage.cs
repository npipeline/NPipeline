using System.Runtime.ExceptionServices;
using NPipeline.ErrorHandling;
using NPipeline.Observability;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Orchestration;

internal sealed class PipelineExecutionFailureStage(IObservabilitySurface observabilitySurface)
{
    public async Task HandleAsync(
        Type definitionType,
        PipelineContext context,
        Exception ex,
        IPipelineActivity pipelineActivity)
    {
        ArgumentNullException.ThrowIfNull(definitionType);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(ex);
        ArgumentNullException.ThrowIfNull(pipelineActivity);

        try
        {
            await observabilitySurface.FailPipeline(definitionType, context, ex, pipelineActivity).ConfigureAwait(false);
        }
        catch
        {
            // A failing observer must not replace the pipeline error that is about to be thrown.
        }

        // A cancellation of this run is preserved raw. A foreign OperationCanceledException, such as a client
        // timeout, is wrapped like any other failure.
        if (ex is OperationCanceledException && context.CancellationToken.IsCancellationRequested)
            ExceptionDispatchInfo.Capture(ex).Throw();

        if (ex is not PipelineException)
            throw new PipelineExecutionException(ErrorMessages.PipelineExecutionFailed(definitionType.Name, ex), ex);

        ExceptionDispatchInfo.Capture(ex).Throw();
    }
}

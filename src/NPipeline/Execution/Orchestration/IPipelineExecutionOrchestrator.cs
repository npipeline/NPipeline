using NPipeline.Pipeline;

namespace NPipeline.Execution.Orchestration;

internal interface IPipelineExecutionOrchestrator
{
    Task RunAsync(
        Type definitionType,
        PipelineContext context,
        Func<IPipelineFactory, PipelineContext, Pipeline.Pipeline> createPipeline,
        CancellationToken cancellationToken);
}

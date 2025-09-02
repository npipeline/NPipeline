using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

public sealed class PersistenceService : IPersistenceService
{
    private PersistenceService()
    {
    }

    public static PersistenceService Instance { get; } = new();

    public void TryPersistAfterNode(PipelineContext context, NodeExecutionCompleted completedEvent)
    {
        _ = TryPersistAfterNodeAsync(context, completedEvent).AsTask();
    }

    internal static async ValueTask TryPersistAfterNodeAsync(PipelineContext context, NodeExecutionCompleted completedEvent)
    {
        var sm = context.StateManager;

        if (sm is null)
            return;

        try
        {
            await sm.CreateSnapshotAsync(context, context.CancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            try
            {
                var logger = context.LoggerFactory.CreateLogger(nameof(PersistenceService));
                logger.Log(LogLevel.Error, ex, "State snapshot failed for node {NodeId}", completedEvent.NodeId);
            }
            catch
            {
                // Swallow exceptions in logging to avoid masking the original error
            }
        }
    }
}

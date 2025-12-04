using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Provides persistence services for pipeline state management.
/// </summary>
public sealed class PersistenceService : IPersistenceService
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="PersistenceService" /> class.
    /// </summary>
    private PersistenceService()
    {
    }

    /// <summary>
    ///     Gets the singleton instance of the <see cref="PersistenceService" />.
    /// </summary>
    public static PersistenceService Instance { get; } = new();

    /// <summary>
    ///     Attempts to persist state after node execution completion.
    /// </summary>
    /// <param name="context">The pipeline context containing state management information.</param>
    /// <param name="completedEvent">The event containing node execution completion details.</param>
    public void TryPersistAfterNode(PipelineContext context, NodeExecutionCompleted completedEvent)
    {
        _ = TryPersistAfterNodeAsync(context, completedEvent).AsTask();
    }

    /// <summary>
    ///     Asynchronously attempts to persist state after node execution completion.
    /// </summary>
    /// <param name="context">The pipeline context containing state management information.</param>
    /// <param name="completedEvent">The event containing node execution completion details.</param>
    /// <returns>A <see cref="ValueTask" /> representing the asynchronous operation.</returns>
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

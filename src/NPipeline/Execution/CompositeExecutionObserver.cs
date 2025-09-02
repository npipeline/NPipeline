using System.Diagnostics;

namespace NPipeline.Execution;

/// <summary>
///     Composite implementation of <see cref="IExecutionObserver" /> that aggregates multiple observers.
///     Allows subscribing multiple observers to pipeline execution events without requiring wrapper code.
/// </summary>
/// <remarks>
///     This observer forwards all events to each contained observer in sequence.
///     Null entries in the observers array are silently skipped.
///     If an observer throws an exception, the exception is logged and the remaining observers are still notified.
///     This resilience ensures that one misbehaving observer doesn't break the entire pipeline's observability.
/// </remarks>
public sealed class CompositeExecutionObserver : IExecutionObserver
{
    private readonly IExecutionObserver[] _observers;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CompositeExecutionObserver" /> class.
    /// </summary>
    /// <param name="observers">The observers to aggregate. Null entries are filtered out.</param>
    public CompositeExecutionObserver(params IExecutionObserver[] observers)
    {
        // Filter out null entries to ensure we never try to invoke null references
        _observers = (observers ?? Array.Empty<IExecutionObserver>())
            .Where(o => o is not null)
            .ToArray();
    }

    /// <summary>
    ///     Forwards the event to all aggregated observers.
    /// </summary>
    public void OnNodeStarted(NodeExecutionStarted e)
    {
        foreach (var observer in _observers)
        {
            try
            {
                observer.OnNodeStarted(e);
            }
            catch (Exception ex)
            {
                // Log the failure but continue notifying remaining observers
                LogObserverFailure(observer, nameof(OnNodeStarted), ex);
            }
        }
    }

    /// <summary>
    ///     Forwards the event to all aggregated observers.
    /// </summary>
    public void OnNodeCompleted(NodeExecutionCompleted e)
    {
        foreach (var observer in _observers)
        {
            try
            {
                observer.OnNodeCompleted(e);
            }
            catch (Exception ex)
            {
                LogObserverFailure(observer, nameof(OnNodeCompleted), ex);
            }
        }
    }

    /// <summary>
    ///     Forwards the event to all aggregated observers.
    /// </summary>
    public void OnRetry(NodeRetryEvent e)
    {
        foreach (var observer in _observers)
        {
            try
            {
                observer.OnRetry(e);
            }
            catch (Exception ex)
            {
                LogObserverFailure(observer, nameof(OnRetry), ex);
            }
        }
    }

    /// <summary>
    ///     Forwards the event to all aggregated observers.
    /// </summary>
    public void OnDrop(QueueDropEvent e)
    {
        foreach (var observer in _observers)
        {
            try
            {
                observer.OnDrop(e);
            }
            catch (Exception ex)
            {
                LogObserverFailure(observer, nameof(OnDrop), ex);
            }
        }
    }

    /// <summary>
    ///     Forwards the event to all aggregated observers.
    /// </summary>
    public void OnQueueMetrics(QueueMetricsEvent e)
    {
        foreach (var observer in _observers)
        {
            try
            {
                observer.OnQueueMetrics(e);
            }
            catch (Exception ex)
            {
                LogObserverFailure(observer, nameof(OnQueueMetrics), ex);
            }
        }
    }

    /// <summary>
    ///     Logs observer failures using the standard diagnostics mechanism.
    ///     This ensures observability issues don't break the pipeline.
    /// </summary>
    private static void LogObserverFailure(IExecutionObserver observer, string methodName, Exception ex)
    {
        // Use System.Diagnostics to report the failure
        Debug.WriteLine(
            $"[ExecutionObserver] {observer.GetType().Name}.{methodName}() threw an exception. " +
            $"The observer will not receive further notifications. Exception: {ex.Message}");
    }
}

using System.Runtime.CompilerServices;

namespace NPipeline.Execution;

/// <summary>
///     Null object pattern implementation of <see cref="IExecutionObserver" />.
///     Provides zero-overhead observer when no observation is needed.
///     This observer is always available and is the default when no explicit observer is configured.
/// </summary>
/// <remarks>
///     This is a singleton that performs no operations. Methods are decorated with <see cref="MethodImplAttribute" />
///     with <see cref="MethodImplOptions.AggressiveInlining" /> to ensure the JIT compiler inlines these calls,
///     reducing overhead to near-zero when no actual observation is needed.
/// </remarks>
public sealed class NullExecutionObserver : IExecutionObserver
{
    /// <summary>
    ///     The singleton instance of the null observer.
    /// </summary>
    public static readonly NullExecutionObserver Instance = new();

    private NullExecutionObserver()
    {
    }

    /// <summary>
    ///     No-op implementation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNodeStarted(NodeExecutionStarted e)
    {
    }

    /// <summary>
    ///     No-op implementation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNodeCompleted(NodeExecutionCompleted e)
    {
    }

    /// <summary>
    ///     No-op implementation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnRetry(NodeRetryEvent e)
    {
    }

    /// <summary>
    ///     No-op implementation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnDrop(QueueDropEvent e)
    {
    }

    /// <summary>
    ///     No-op implementation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnQueueMetrics(QueueMetricsEvent e)
    {
    }
}

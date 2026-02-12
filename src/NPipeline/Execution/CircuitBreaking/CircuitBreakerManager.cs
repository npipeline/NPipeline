using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NPipeline.Configuration;
using NPipeline.Observability.Logging;

namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     Manages the lifecycle of circuit breaker instances for different nodes.
///     Provides centralized creation, caching, and cleanup of circuit breakers.
/// </summary>
internal sealed class CircuitBreakerManager : ICircuitBreakerManager, IDisposable
{
    private readonly ConcurrentDictionary<string, ICircuitBreaker> _circuitBreakers = new();
    private readonly object _cleanupLock = new();
    private readonly Timer? _cleanupTimer;
    private readonly ILogger _logger;
    private readonly CircuitBreakerMemoryManagementOptions _memoryOptions;
    private readonly CircuitBreakerTracker _tracker;

    /// <summary>
    ///     Initializes a new instance of CircuitBreakerManager class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    /// <param name="memoryOptions">The memory management options for cleanup.</param>
    public CircuitBreakerManager(ILogger logger, CircuitBreakerMemoryManagementOptions? memoryOptions = null)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;
        _memoryOptions = (memoryOptions ?? CircuitBreakerMemoryManagementOptions.Default).Validate();
        _tracker = new CircuitBreakerTracker();

        // Setup cleanup timer if automatic cleanup is enabled
        if (_memoryOptions.EnableAutomaticCleanup)
        {
            _cleanupTimer = new Timer(CleanupUnusedCircuitBreakers, null,
                _memoryOptions.EffectiveCleanupInterval, _memoryOptions.EffectiveCleanupInterval);

            CircuitBreakerManagerLogMessages.CleanupTimerInitialized(_logger, _memoryOptions.EffectiveCleanupInterval);
        }
    }

    /// <summary>
    ///     Gets or creates a circuit breaker for the specified node ID with the given options.
    /// </summary>
    /// <param name="nodeId">The unique identifier for the node.</param>
    /// <param name="options">The circuit breaker configuration options.</param>
    /// <returns>The circuit breaker instance for the node.</returns>
    public ICircuitBreaker GetCircuitBreaker(string nodeId, PipelineCircuitBreakerOptions options)
    {
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(options);

        var circuitBreaker = _circuitBreakers.GetOrAdd(nodeId, id => CreateCircuitBreaker(id, options));

        // Update the last access time for the circuit breaker
        _tracker.UpdateAccessTime(nodeId);

        return circuitBreaker;
    }

    /// <summary>
    ///     Removes the circuit breaker for the specified node ID.
    /// </summary>
    /// <param name="nodeId">The unique identifier for the node.</param>
    public void RemoveCircuitBreaker(string nodeId)
    {
        ArgumentNullException.ThrowIfNull(nodeId);

        if (_circuitBreakers.TryRemove(nodeId, out var circuitBreaker))
        {
            (circuitBreaker as IDisposable)?.Dispose();
            _ = _tracker.RemoveTracking(nodeId);
            CircuitBreakerManagerLogMessages.CircuitBreakerRemoved(_logger, nodeId);
        }
    }

    /// <summary>
    ///     Manually triggers cleanup of inactive circuit breakers.
    /// </summary>
    /// <returns>The number of circuit breakers that were removed.</returns>
    public int TriggerCleanup()
    {
        if (!_memoryOptions.EnableAutomaticCleanup)
        {
            CircuitBreakerManagerLogMessages.ManualCleanupTriggered(_logger);
            return PerformCleanup();
        }

        CircuitBreakerManagerLogMessages.ManualCleanupTriggeredWithAutoEnabled(_logger);
        return PerformCleanup();
    }

    /// <summary>
    ///     Gets the current count of tracked circuit breakers.
    /// </summary>
    /// <returns>The number of tracked circuit breakers.</returns>
    public int GetTrackedCircuitBreakerCount()
    {
        return _tracker.GetTrackedCount();
    }

    /// <summary>
    ///     Releases all resources used by the CircuitBreakerManager.
    /// </summary>
    public void Dispose()
    {
        _cleanupTimer?.Dispose();

        foreach (var circuitBreaker in _circuitBreakers.Values)
        {
            (circuitBreaker as IDisposable)?.Dispose();
        }

        _circuitBreakers.Clear();
        _tracker.Dispose();
    }

    /// <summary>
    ///     Creates a new circuit breaker instance for the specified node.
    /// </summary>
    /// <param name="nodeId">The unique identifier for the node.</param>
    /// <param name="options">The circuit breaker configuration options.</param>
    /// <returns>A new circuit breaker instance.</returns>
    private ICircuitBreaker CreateCircuitBreaker(string nodeId, PipelineCircuitBreakerOptions options)
    {
        // Check if we've exceeded the maximum number of tracked circuit breakers
        if (_tracker.GetTrackedCount() >= _memoryOptions.MaxTrackedCircuitBreakers)
        {
            CircuitBreakerManagerLogMessages.MaxCircuitBreakerLimitReached(_logger, _memoryOptions.MaxTrackedCircuitBreakers, nodeId);

            var removed = PerformCleanup(true);

            if (removed == 0 && _tracker.GetTrackedCount() >= _memoryOptions.MaxTrackedCircuitBreakers)
            {
                var message =
                    $"Unable to create circuit breaker for node '{nodeId}' because the manager exhausted its maximum capacity of {_memoryOptions.MaxTrackedCircuitBreakers}.";

                CircuitBreakerManagerLogMessages.CircuitBreakerCreationFailed(_logger, nodeId, _memoryOptions.MaxTrackedCircuitBreakers);
                throw new InvalidOperationException(message);
            }
        }

        CircuitBreakerManagerLogMessages.CreatingCircuitBreaker(_logger, nodeId, options);
        var circuitBreaker = new CircuitBreaker(options, _logger);

        // Track the new circuit breaker
        _tracker.UpdateAccessTime(nodeId);

        return circuitBreaker;
    }

    /// <summary>
    ///     Cleanup timer callback to remove unused circuit breakers.
    /// </summary>
    /// <param name="state">Timer state (unused).</param>
    private void CleanupUnusedCircuitBreakers(object? state)
    {
        try
        {
            _ = PerformCleanup();
        }
        catch (Exception ex)
        {
            CircuitBreakerManagerLogMessages.CleanupError(_logger, ex);
        }
    }

    /// <summary>
    ///     Performs the actual cleanup of inactive circuit breakers.
    /// </summary>
    /// <returns>The number of circuit breakers that were removed.</returns>
    private int PerformCleanup(bool allowAggressiveEviction = false)
    {
        // Prevent concurrent cleanup operations
        if (!Monitor.TryEnter(_cleanupLock, _memoryOptions.EffectiveCleanupTimeout))
        {
            CircuitBreakerManagerLogMessages.CleanupSkippedInProgress(_logger);
            return 0;
        }

        try
        {
            var inactiveCircuitBreakers = _tracker.GetInactiveCircuitBreakers(_memoryOptions.EffectiveInactivityThreshold);
            var removedCount = 0;

            foreach (var nodeId in inactiveCircuitBreakers)
            {
                var removedBreaker = false;

                if (_circuitBreakers.TryRemove(nodeId, out var circuitBreaker))
                {
                    (circuitBreaker as IDisposable)?.Dispose();
                    removedBreaker = true;
                    removedCount++;
                    CircuitBreakerManagerLogMessages.InactiveCircuitBreakerRemoved(_logger, nodeId);
                }

                var removedTracking = _tracker.RemoveTracking(nodeId);

                if (!removedBreaker && removedTracking)
                    CircuitBreakerManagerLogMessages.StaleTrackingRemoved(_logger, nodeId);
            }

            if (removedCount == 0 && allowAggressiveEviction)
            {
                if (_tracker.TryGetLeastRecentlyUsedNode(out var victimNodeId, out var lastAccess))
                {
                    var removedBreaker = false;

                    if (_circuitBreakers.TryRemove(victimNodeId, out var victimCircuitBreaker))
                    {
                        (victimCircuitBreaker as IDisposable)?.Dispose();
                        removedBreaker = true;
                        removedCount++;

                        CircuitBreakerManagerLogMessages.AggressiveCleanupRemoved(_logger, victimNodeId, lastAccess);
                    }

                    var removedTracking = _tracker.RemoveTracking(victimNodeId);

                    if (!removedBreaker && removedTracking)
                        CircuitBreakerManagerLogMessages.AggressiveCleanupStaleTrackingRemoved(_logger, victimNodeId, lastAccess);
                }
                else
                    CircuitBreakerManagerLogMessages.AggressiveCleanupNoVictims(_logger);
            }

            if (removedCount > 0)
                CircuitBreakerManagerLogMessages.CleanupCompleted(_logger, removedCount);

            return removedCount;
        }
        finally
        {
            Monitor.Exit(_cleanupLock);
        }
    }
}

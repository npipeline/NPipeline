namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     Provides rolling window operation tracking for circuit breaker failure analysis.
///     Thread-safe implementation using ConcurrentQueue and lock for simplicity and reliability.
/// </summary>
internal sealed class RollingWindow : IDisposable
{
    private readonly object _gate = new();
    private readonly Queue<OperationRecord> _operations = new();
    private readonly TimeSpan _windowSize;

    /// <summary>
    ///     Initializes a new instance of the <see cref="RollingWindow" /> class.
    /// </summary>
    /// <param name="windowSize">The size of the rolling window. Must be positive.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when windowSize is not positive.</exception>
    public RollingWindow(TimeSpan windowSize)
    {
        if (windowSize <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(windowSize), "Rolling window size must be positive.");

        _windowSize = windowSize;
    }

    /// <summary>
    ///     Releases all resources used by the RollingWindow.
    /// </summary>
    public void Dispose()
    {
        // Nothing to dispose currently. Method exists for future extensibility.
    }

    /// <summary>
    ///     Adds an operation outcome to rolling window.
    /// </summary>
    /// <param name="outcome">The outcome of operation.</param>
    public void AddOperation(OperationOutcome outcome)
    {
        var record = new OperationRecord(DateTime.UtcNow, outcome);

        lock (_gate)
        {
            _operations.Enqueue(record);
            PurgeExpiredOperationsUnsafe();
        }
    }

    /// <summary>
    ///     Gets statistics for operations within the current rolling window.
    /// </summary>
    /// <returns>Window statistics including counts and failure rate.</returns>
    public WindowStatistics GetStatistics()
    {
        lock (_gate)
        {
            // First purge expired operations
            PurgeExpiredOperationsUnsafe();

            if (_operations.Count == 0)
                return new WindowStatistics(0, 0, 0, 0);

            var totalOperations = _operations.Count;
            var failureCount = 0;

            foreach (var operation in _operations)
            {
                if (operation.Outcome == OperationOutcome.Failure)
                    failureCount++;
            }

            var successCount = totalOperations - failureCount;
            var failureRate = (double)failureCount / totalOperations;

            return new WindowStatistics(totalOperations, failureCount, successCount, failureRate);
        }
    }

    /// <summary>
    ///     Clears all operations from the rolling window.
    /// </summary>
    public void Clear()
    {
        lock (_gate)
        {
            _operations.Clear();
        }
    }

    /// <summary>
    ///     Gets count of consecutive failures at the end of the window.
    /// </summary>
    /// <returns>Number of consecutive failures.</returns>
    public int GetConsecutiveFailures()
    {
        lock (_gate)
        {
            // First purge expired operations
            PurgeExpiredOperationsUnsafe();

            if (_operations.Count == 0)
                return 0;

            var operationsArray = _operations.ToArray();
            var consecutiveFailures = 0;

            // Count from the end backwards
            for (var i = operationsArray.Length - 1; i >= 0; i--)
            {
                if (operationsArray[i].Outcome == OperationOutcome.Failure)
                    consecutiveFailures++;
                else
                    break;
            }

            return consecutiveFailures;
        }
    }

    /// <summary>
    ///     Removes operations that are outside of the sampling window.
    ///     This method assumes the caller holds the lock.
    /// </summary>
    private void PurgeExpiredOperationsUnsafe()
    {
        var cutoff = DateTime.UtcNow - _windowSize;

        while (_operations.Count > 0 && _operations.Peek().Timestamp < cutoff)
        {
            _ = _operations.Dequeue();
        }
    }
}

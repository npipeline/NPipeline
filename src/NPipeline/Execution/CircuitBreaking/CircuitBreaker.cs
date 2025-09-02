using NPipeline.Configuration;
using NPipeline.Observability.Logging;

namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     Implements a circuit breaker with state machine for resilient execution.
///     Provides thread-safe operation tracking and automatic recovery mechanisms.
/// </summary>
internal sealed class CircuitBreaker : ICircuitBreaker, IDisposable
{
    private readonly object _gate = new();
    private readonly IPipelineLogger _logger;
    private readonly Timer? _recoveryTimer;
    private readonly RollingWindow? _rollingWindow;
    private int _consecutiveFailures;
    private bool _disposed;
    private int _halfOpenAttempts;
    private int _halfOpenSuccesses;

    private CircuitBreakerState _state = CircuitBreakerState.Closed;

    /// <summary>
    ///     Initializes a new instance of the CircuitBreaker class.
    /// </summary>
    /// <param name="options">The circuit breaker configuration options.</param>
    /// <param name="logger">The logger for diagnostic information.</param>
    public CircuitBreaker(PipelineCircuitBreakerOptions options, IPipelineLogger logger)
    {
        Options = (options ?? throw new ArgumentNullException(nameof(options))).Validate();
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _rollingWindow = Options.TrackOperationsInWindow
            ? new RollingWindow(Options.SamplingWindow)
            : null;

        // Start recovery timer if needed
        if (Options.OpenDuration > TimeSpan.Zero)
            _recoveryTimer = new Timer(OnRecoveryTimerElapsed, null, Timeout.Infinite, Timeout.Infinite);
    }

    /// <summary>
    ///     Gets current state of the circuit breaker.
    /// </summary>
    public CircuitBreakerState State
    {
        get
        {
            lock (_gate)
            {
                return _state;
            }
        }
    }

    /// <summary>
    ///     Gets the circuit breaker configuration options.
    /// </summary>
    public PipelineCircuitBreakerOptions Options { get; }

    /// <summary>
    ///     Gets the current statistics from the circuit breaker.
    /// </summary>
    /// <returns>The current window statistics.</returns>
    public WindowStatistics GetStatistics()
    {
        return _rollingWindow?.GetStatistics() ?? new WindowStatistics(0, 0, 0, 0);
    }

    /// <summary>
    ///     Determines whether an operation can be executed based on the current state.
    /// </summary>
    /// <returns>True if operation is allowed, false otherwise.</returns>
    public bool CanExecute()
    {
        lock (_gate)
        {
            ThrowIfDisposed();

            return _state switch
            {
                CircuitBreakerState.Closed => true,
                CircuitBreakerState.Open => false,
                CircuitBreakerState.HalfOpen => _halfOpenAttempts < Options.HalfOpenMaxAttempts,
                _ => false,
            };
        }
    }

    /// <summary>
    ///     Records a successful operation and updates the circuit breaker state accordingly.
    /// </summary>
    /// <returns>The result of the operation recording including any state changes.</returns>
    public CircuitBreakerExecutionResult RecordSuccess()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            TrackOutcome(OperationOutcome.Success);
            _consecutiveFailures = 0;

            return _state switch
            {
                CircuitBreakerState.Closed => new CircuitBreakerExecutionResult(true, false, CircuitBreakerState.Closed,
                    "Success recorded, circuit remains closed"),
                CircuitBreakerState.HalfOpen => HandleHalfOpenSuccess(),
                CircuitBreakerState.Open => new CircuitBreakerExecutionResult(false, false, CircuitBreakerState.Open,
                    "Success ignored while circuit breaker is open"),
                _ => new CircuitBreakerExecutionResult(false, false, _state, "Success recorded in unexpected state"),
            };
        }
    }

    /// <summary>
    ///     Records a failed operation and updates the circuit breaker state accordingly.
    /// </summary>
    /// <returns>The result of the operation recording including any state changes.</returns>
    public CircuitBreakerExecutionResult RecordFailure()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            TrackOutcome(OperationOutcome.Failure);
            _consecutiveFailures++;

            return _state switch
            {
                CircuitBreakerState.Closed => HandleClosedFailure(),
                CircuitBreakerState.HalfOpen => TransitionToOpen("Failure in Half-Open state"),
                CircuitBreakerState.Open => new CircuitBreakerExecutionResult(false, false, CircuitBreakerState.Open,
                    "Failure recorded while circuit breaker is open"),
                _ => new CircuitBreakerExecutionResult(false, false, _state, "Failure recorded in unexpected state"),
            };
        }
    }

    /// <summary>
    ///     Releases all resources used by the CircuitBreaker.
    /// </summary>
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
                return;

            _disposed = true;
        }

        _recoveryTimer?.Dispose();
        _rollingWindow?.Dispose();
    }

    private CircuitBreakerExecutionResult HandleClosedFailure()
    {
        if (ShouldTripBreaker())
            return TransitionToOpen("Failure threshold exceeded");

        return new CircuitBreakerExecutionResult(true, false, _state, "Failure recorded, circuit remains closed");
    }

    private CircuitBreakerExecutionResult HandleHalfOpenSuccess()
    {
        _halfOpenSuccesses++;
        _halfOpenAttempts++;

        if (_halfOpenSuccesses >= Options.HalfOpenSuccessThreshold)
            return TransitionToClosed("Recovery confirmed");

        return new CircuitBreakerExecutionResult(true, false, _state,
            $"Success recorded in Half-Open state ({_halfOpenSuccesses}/{Options.HalfOpenSuccessThreshold})");
    }

    private bool ShouldTripBreaker()
    {
        return Options.ThresholdType switch
        {
            CircuitBreakerThresholdType.ConsecutiveFailures => _consecutiveFailures >= Options.FailureThreshold,
            CircuitBreakerThresholdType.RollingWindowCount => GetWindowStatistics().FailureCount >= Options.FailureThreshold,
            CircuitBreakerThresholdType.RollingWindowRate => HasMetFailureRateThreshold(GetWindowStatistics()),
            CircuitBreakerThresholdType.Hybrid => HasMetHybridThreshold(GetWindowStatistics()),
            _ => false,
        };
    }

    private CircuitBreakerExecutionResult TransitionToOpen(string reason)
    {
        var previousState = _state;
        _state = CircuitBreakerState.Open;
        _consecutiveFailures = 0;
        _halfOpenAttempts = 0;
        _halfOpenSuccesses = 0;

        // Start recovery timer
        if (_recoveryTimer != null && Options.OpenDuration > TimeSpan.Zero)
            _ = _recoveryTimer.Change(Options.OpenDuration, Timeout.InfiniteTimeSpan);

        _logger.Log(LogLevel.Warning, "Circuit breaker transitioned from {PreviousState} to Open: {Reason}", previousState, reason);

        return new CircuitBreakerExecutionResult(false, true, _state, reason);
    }

    private CircuitBreakerExecutionResult TransitionToHalfOpen(string reason)
    {
        var previousState = _state;
        _state = CircuitBreakerState.HalfOpen;
        _consecutiveFailures = 0;
        _halfOpenSuccesses = 0;
        _halfOpenAttempts = 0;

        _logger.Log(LogLevel.Information, "Circuit breaker transitioned from {PreviousState} to Half-Open: {Reason}. Success threshold: {Threshold}",
            previousState, reason, Options.HalfOpenSuccessThreshold);

        return new CircuitBreakerExecutionResult(true, true, _state, reason);
    }

    private CircuitBreakerExecutionResult TransitionToClosed(string reason)
    {
        var previousState = _state;
        _state = CircuitBreakerState.Closed;

        _consecutiveFailures = 0;
        _halfOpenAttempts = 0;
        _halfOpenSuccesses = 0;

        // Clear the rolling window when transitioning to Closed
        _rollingWindow?.Clear();

        // Stop recovery timer if it's running
        _recoveryTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

        _logger.Log(LogLevel.Information, "Circuit breaker transitioned from {PreviousState} to Closed: {Reason}. Metrics reset.",
            previousState, reason);

        return new CircuitBreakerExecutionResult(true, true, _state, reason);
    }

    private void OnRecoveryTimerElapsed(object? state)
    {
        lock (_gate)
        {
            if (_disposed)
                return;

            if (_state == CircuitBreakerState.Open)
                _ = TransitionToHalfOpen("Recovery timer elapsed");
        }
    }

    private void TrackOutcome(OperationOutcome outcome)
    {
        _rollingWindow?.AddOperation(outcome);
    }

    private WindowStatistics GetWindowStatistics()
    {
        if (_rollingWindow is null)
            throw new InvalidOperationException("Rolling window statistics requested but tracking is disabled.");

        return _rollingWindow.GetStatistics();
    }

    private bool HasMetFailureRateThreshold(WindowStatistics statistics)
    {
        return statistics.TotalOperations >= Options.FailureThreshold
               && statistics.FailureRate >= Options.FailureRateThreshold;
    }

    private bool HasMetHybridThreshold(WindowStatistics statistics)
    {
        if (statistics.FailureCount >= Options.FailureThreshold)
            return true;

        return HasMetFailureRateThreshold(statistics);
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(CircuitBreaker));
    }
}

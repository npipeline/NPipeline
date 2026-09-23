using Microsoft.Extensions.Logging;
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
    private readonly ILogger _logger;
    private readonly Timer? _recoveryTimer;
    private readonly RollingWindow? _rollingWindow;
    private readonly Action<CircuitState, CircuitState, string>? _stateChanged;
    private int _consecutiveFailures;
    private bool _disposed;
    private int _halfOpenAttempts;
    private int _halfOpenSuccesses;
    private long _lastActivityTicks = DateTime.UtcNow.Ticks;
    private CircuitState _state = CircuitState.Closed;

    /// <summary>
    ///     Initializes a new instance of CircuitBreaker class.
    /// </summary>
    /// <param name="options">The circuit breaker configuration options.</param>
    /// <param name="logger">The logger for diagnostic information.</param>
    /// <param name="stateChanged">Called with the previous state, the new state, and the reason after every transition.</param>
    public CircuitBreaker(PipelineCircuitBreakerOptions options, ILogger logger, Action<CircuitState, CircuitState, string>? stateChanged = null)
    {
        Options = (options ?? throw new ArgumentNullException(nameof(options))).Validate();
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _stateChanged = stateChanged;

        _rollingWindow = Options.TrackOperationsInWindow
            ? new RollingWindow(Options.SamplingWindow)
            : null;

        // Start recovery timer if needed
        if (Options.OpenDuration > TimeSpan.Zero)
            _recoveryTimer = new Timer(OnRecoveryTimerElapsed, null, Timeout.Infinite, Timeout.Infinite);
    }

    /// <summary>
    ///     Gets current state of circuit breaker.
    /// </summary>
    public CircuitState State
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
    ///     Gets circuit breaker configuration options.
    /// </summary>
    public PipelineCircuitBreakerOptions Options { get; }

    /// <summary>
    ///     Gets the UTC time the breaker was last consulted or had an outcome recorded.
    /// </summary>
    /// <remarks>
    ///     The manager's inactivity cleanup reads this. A stream looks its breaker up once and then only records
    ///     outcomes, so the lookup time alone would make a busy breaker look idle.
    /// </remarks>
    public DateTime LastActivityUtc => new(Volatile.Read(ref _lastActivityTicks), DateTimeKind.Utc);

    /// <summary>
    ///     Gets current statistics from circuit breaker.
    /// </summary>
    /// <returns>The current window statistics.</returns>
    public WindowStatistics GetStatistics()
    {
        return _rollingWindow?.GetStatistics() ?? new WindowStatistics(0, 0, 0, 0);
    }

    /// <summary>
    ///     Determines whether an operation can be executed based on current state.
    /// </summary>
    /// <returns>True if operation is allowed, false otherwise.</returns>
    public bool CanExecute()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            MarkActivity();

            return _state switch
            {
                CircuitState.Closed => true,
                CircuitState.Open => false,
                CircuitState.HalfOpen => _halfOpenAttempts < Options.HalfOpenMaxAttempts,
                _ => false,
            };
        }
    }

    /// <summary>
    ///     Records a successful operation and updates circuit breaker state accordingly.
    /// </summary>
    /// <returns>The result of operation recording including any state changes.</returns>
    public CircuitBreakerExecutionResult RecordSuccess()
    {
        CircuitState previous;
        CircuitBreakerExecutionResult result;

        lock (_gate)
        {
            ThrowIfDisposed();
            MarkActivity();
            TrackOutcome(OperationOutcome.Success);
            _consecutiveFailures = 0;
            previous = _state;

            result = _state switch
            {
                CircuitState.Closed => new CircuitBreakerExecutionResult(true, false, CircuitState.Closed,
                    "Success recorded, circuit remains closed"),
                CircuitState.HalfOpen => HandleHalfOpenSuccess(),
                CircuitState.Open => new CircuitBreakerExecutionResult(false, false, CircuitState.Open,
                    "Success ignored while circuit breaker is open"),
                _ => new CircuitBreakerExecutionResult(false, false, _state, "Success recorded in unexpected state"),
            };
        }

        NotifyIfChanged(previous, result);
        return result;
    }

    /// <summary>
    ///     Records a failed operation and updates circuit breaker state accordingly.
    /// </summary>
    /// <returns>The result of operation recording including any state changes.</returns>
    public CircuitBreakerExecutionResult RecordFailure()
    {
        CircuitState previous;
        CircuitBreakerExecutionResult result;

        lock (_gate)
        {
            ThrowIfDisposed();
            MarkActivity();
            TrackOutcome(OperationOutcome.Failure);
            _consecutiveFailures++;
            previous = _state;

            result = _state switch
            {
                CircuitState.Closed => HandleClosedFailure(),
                CircuitState.HalfOpen => TransitionToOpen("Failure in Half-Open state"),
                CircuitState.Open => new CircuitBreakerExecutionResult(false, false, CircuitState.Open,
                    "Failure recorded while circuit breaker is open"),
                _ => new CircuitBreakerExecutionResult(false, false, _state, "Failure recorded in unexpected state"),
            };
        }

        NotifyIfChanged(previous, result);
        return result;
    }

    /// <summary>
    ///     Releases all resources used by CircuitBreaker.
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

        if (_halfOpenSuccesses < Options.HalfOpenSuccessThreshold)
        {
            return new CircuitBreakerExecutionResult(true, false, _state,
                $"Success recorded in Half-Open state ({_halfOpenSuccesses}/{Options.HalfOpenSuccessThreshold})");
        }

        return TransitionToClosed("Recovery confirmed");
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
        _state = CircuitState.Open;
        _consecutiveFailures = 0;
        _halfOpenAttempts = 0;
        _halfOpenSuccesses = 0;

        // Start recovery timer
        if (_recoveryTimer is not null && Options.OpenDuration > TimeSpan.Zero)
            _recoveryTimer.Change(Options.OpenDuration, Timeout.InfiniteTimeSpan);

        CircuitBreakerLogMessages.TransitionedToOpen(_logger, previousState.ToString(), reason);

        return new CircuitBreakerExecutionResult(false, true, _state, reason);
    }

    private CircuitBreakerExecutionResult TransitionToHalfOpen(string reason)
    {
        var previousState = _state;
        _state = CircuitState.HalfOpen;
        _consecutiveFailures = 0;
        _halfOpenSuccesses = 0;
        _halfOpenAttempts = 0;

        CircuitBreakerLogMessages.TransitionedToHalfOpen(_logger, previousState.ToString(), reason, Options.HalfOpenSuccessThreshold);

        return new CircuitBreakerExecutionResult(true, true, _state, reason);
    }

    private CircuitBreakerExecutionResult TransitionToClosed(string reason)
    {
        var previousState = _state;
        _state = CircuitState.Closed;

        _consecutiveFailures = 0;
        _halfOpenAttempts = 0;
        _halfOpenSuccesses = 0;

        // Clear rolling window when transitioning to Closed
        _rollingWindow?.Clear();

        // Stop recovery timer if it's running
        _recoveryTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

        CircuitBreakerLogMessages.TransitionedToClosed(_logger, previousState.ToString(), reason);

        return new CircuitBreakerExecutionResult(true, true, _state, reason);
    }

    private void OnRecoveryTimerElapsed(object? state)
    {
        CircuitBreakerExecutionResult result;

        lock (_gate)
        {
            if (_disposed || _state != CircuitState.Open)
                return;

            result = TransitionToHalfOpen("Recovery timer elapsed");
        }

        NotifyIfChanged(CircuitState.Open, result);
    }

    // Runs outside the lock, so a listener cannot deadlock against the breaker. It also runs on the recovery timer's
    // thread, where an escaping exception would crash the process, so listener failures are logged, never thrown.
    private void NotifyIfChanged(CircuitState previous, CircuitBreakerExecutionResult result)
    {
        if (_stateChanged is null || !result.StateChanged || result.NewState is not { } next)
            return;

        try
        {
            _stateChanged(previous, next, result.Message);
        }
        catch (Exception ex)
        {
            CircuitBreakerLogMessages.StateChangeListenerFailed(_logger, ex, previous.ToString(), next.ToString());
        }
    }

    private void MarkActivity()
    {
        Volatile.Write(ref _lastActivityTicks, DateTime.UtcNow.Ticks);
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

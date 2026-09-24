using NPipeline.Reliability;

namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     Permission for one item attempt, handed out by <see cref="CircuitBreaker.TryAcquire" />. Every permit is
///     returned exactly once, through <see cref="CircuitBreaker.RecordSuccess" />,
///     <see cref="CircuitBreaker.RecordFailure" />, or <see cref="CircuitBreaker.Release" />.
/// </summary>
/// <param name="Generation">The breaker's generation when the permit was issued. An outcome from an earlier generation is ignored.</param>
/// <param name="IsProbe">Whether the attempt is a half-open probe.</param>
internal readonly record struct BreakerPermit(int Generation, bool IsProbe);

/// <summary>
///     A state change caused by a breaker call, for the caller to report. <see cref="Occurred" /> is false when the call
///     changed nothing.
/// </summary>
internal readonly record struct BreakerTransition(CircuitState From, CircuitState To, string? Reason)
{
    public static BreakerTransition None => default;

    public bool Occurred => Reason is not null;
}

/// <summary>
///     One node's circuit breaker. It runs on a <see cref="TimeProvider" /> and has no timers: an open breaker becomes
///     half-open the first time an attempt is asked for after <see cref="CircuitBreakerOptions.OpenDuration" />.
/// </summary>
/// <remarks>
///     <para>
///         The breaker outlives a pipeline run, so it never reports its own transitions: each call returns the
///         transition it caused, and the caller reports it to its run's observer.
///     </para>
///     <para>
///         The closed path takes no lock unless failures are pending, so a healthy node pays a few volatile reads per
///         item, plus an interlocked increment when a failure rate is configured.
///     </para>
/// </remarks>
internal sealed class CircuitBreaker
{
    private readonly object _gate = new();
    private readonly RollingWindow? _window;
    private TaskCompletionSource _admissionChanged = NewSignal();
    private int _consecutiveFailures;
    private int _generation;
    private long _halfOpenAtTimestamp;
    private int _probeSuccesses;
    private int _probesInFlight;
    private volatile CircuitState _state = CircuitState.Closed;

    public CircuitBreaker(string nodeId, CircuitBreakerOptions options, TimeProvider time)
    {
        NodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
        Options = (options ?? throw new ArgumentNullException(nameof(options))).Validate();
        Time = time ?? throw new ArgumentNullException(nameof(time));

        if (options.FailureRate is not null)
            _window = new RollingWindow(options.Window, time);
    }

    public string NodeId { get; }

    public CircuitBreakerOptions Options { get; }

    public TimeProvider Time { get; }

    /// <summary>
    ///     The state as of the last call. An open breaker whose open period has passed still reads as open until an
    ///     attempt is asked for.
    /// </summary>
    public CircuitState State => _state;

    /// <summary>
    ///     Completes the next time an attempt that was refused might now be admitted: on any transition, or when a
    ///     half-open probe finishes. Read it before <see cref="TryAcquire" /> so a change in between is not missed.
    /// </summary>
    public Task AdmissionChanged => Volatile.Read(ref _admissionChanged).Task;

    /// <summary>
    ///     Asks to make one attempt.
    /// </summary>
    /// <param name="permit">The permit, when admitted. Return it through one of the record methods.</param>
    /// <param name="transition">The transition this call caused: open to half-open, when the open period has passed.</param>
    /// <returns>Whether the attempt may be made.</returns>
    public bool TryAcquire(out BreakerPermit permit, out BreakerTransition transition)
    {
        transition = BreakerTransition.None;

        // The generation is read first and written last (see TransitionTo), so a permit issued here either carries
        // the closed generation or a stale one whose outcome is ignored.
        var generation = Volatile.Read(ref _generation);

        if (_state == CircuitState.Closed)
        {
            permit = new BreakerPermit(generation, false);
            return true;
        }

        lock (_gate)
        {
            if (_state == CircuitState.Open)
            {
                if (Time.GetTimestamp() < _halfOpenAtTimestamp)
                {
                    permit = default;
                    return false;
                }

                transition = TransitionTo(CircuitState.HalfOpen, $"open for {Options.OpenDuration}; letting probes through");
            }

            switch (_state)
            {
                case CircuitState.Closed:
                    permit = new BreakerPermit(_generation, false);
                    return true;

                case CircuitState.HalfOpen when _probesInFlight < Options.HalfOpenProbes:
                    _probesInFlight++;
                    permit = new BreakerPermit(_generation, true);
                    return true;

                default:
                    permit = default;
                    return false;
            }
        }
    }

    /// <summary>
    ///     The attempt succeeded.
    /// </summary>
    public BreakerTransition RecordSuccess(BreakerPermit permit)
    {
        if (!permit.IsProbe)
        {
            // Lock-free unless failures are pending: a healthy node only bumps the rate window, if it has one.
            if (permit.Generation != Volatile.Read(ref _generation) || _state != CircuitState.Closed)
                return BreakerTransition.None;

            _window?.RecordSuccess();

            if (Volatile.Read(ref _consecutiveFailures) == 0)
                return BreakerTransition.None;

            lock (_gate)
            {
                if (permit.Generation == _generation)
                    _consecutiveFailures = 0;
            }

            return BreakerTransition.None;
        }

        lock (_gate)
        {
            if (permit.Generation != _generation)
                return BreakerTransition.None;

            _probesInFlight--;
            _probeSuccesses++;

            if (_probeSuccesses >= Options.ProbeSuccesses)
                return TransitionTo(CircuitState.Closed, $"{_probeSuccesses} probe(s) succeeded");

            SignalAdmissionChanged();
            return BreakerTransition.None;
        }
    }

    /// <summary>
    ///     The attempt failed transiently. Call <see cref="Release" /> instead for any other failure.
    /// </summary>
    public BreakerTransition RecordFailure(BreakerPermit permit)
    {
        lock (_gate)
        {
            if (permit.Generation != _generation)
                return BreakerTransition.None;

            if (permit.IsProbe)
                return TransitionTo(CircuitState.Open, "a probe failed");

            if (_state != CircuitState.Closed)
                return BreakerTransition.None;

            _consecutiveFailures++;
            _window?.RecordFailure();

            if (Options.ConsecutiveFailures is { } limit && _consecutiveFailures >= limit)
                return TransitionTo(CircuitState.Open, $"{_consecutiveFailures} consecutive transient failures");

            if (Options.FailureRate is { } rate && _window is not null)
            {
                var (total, failures) = _window.Read();

                if (total >= Options.MinimumCalls && (double)failures / total >= rate)
                    return TransitionTo(CircuitState.Open, $"{failures} of {total} attempts failed transiently within {Options.Window}");
            }

            return BreakerTransition.None;
        }
    }

    /// <summary>
    ///     The attempt ended with an outcome that says nothing about the dependency: a permanent failure, or
    ///     cancellation.
    /// </summary>
    public void Release(BreakerPermit permit)
    {
        if (!permit.IsProbe)
            return;

        lock (_gate)
        {
            if (permit.Generation != _generation)
                return;

            _probesInFlight--;
            SignalAdmissionChanged();
        }
    }

    /// <summary>
    ///     How long until an open breaker lets a probe through, or null when it is not open.
    /// </summary>
    public TimeSpan? TimeUntilHalfOpen()
    {
        lock (_gate)
        {
            if (_state != CircuitState.Open)
                return null;

            var remaining = _halfOpenAtTimestamp - Time.GetTimestamp();

            return remaining <= 0
                ? TimeSpan.Zero
                : TimeSpan.FromSeconds((double)remaining / Time.TimestampFrequency);
        }
    }

    private BreakerTransition TransitionTo(CircuitState next, string reason)
    {
        var previous = _state;

        _consecutiveFailures = 0;
        _probesInFlight = 0;
        _probeSuccesses = 0;
        _window?.Clear();

        if (next == CircuitState.Open)
            _halfOpenAtTimestamp = Time.GetTimestamp() + (long)(Options.OpenDuration.TotalSeconds * Time.TimestampFrequency);

        // State before generation: the lock-free read in TryAcquire depends on this order.
        _state = next;
        Volatile.Write(ref _generation, _generation + 1);
        SignalAdmissionChanged();
        return new BreakerTransition(previous, next, reason);
    }

    private void SignalAdmissionChanged()
    {
        Interlocked.Exchange(ref _admissionChanged, NewSignal()).TrySetResult();
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

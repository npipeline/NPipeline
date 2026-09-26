using Microsoft.Extensions.Logging;
using NPipeline.ErrorHandling;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     How the item executor uses a node's breaker: admission before an attempt, the outcome after it, and reporting
///     each transition to the run's observer.
/// </summary>
internal static class CircuitBreakerGate
{
    /// <summary>
    ///     Admits one attempt, waiting for the breaker when it is open and its options say
    ///     <see cref="BreakerOpenBehavior.Pause" />.
    /// </summary>
    /// <exception cref="CircuitBreakerOpenException">The attempt was refused.</exception>
    public static ValueTask<BreakerPermit> AcquireAsync(CircuitBreaker breaker, PipelineContext context, CancellationToken cancellationToken)
    {
        if (breaker.TryAcquire(out var permit, out var transition))
        {
            Report(context, breaker, transition);
            return new ValueTask<BreakerPermit>(permit);
        }

        Report(context, breaker, transition);

        return breaker.Options.WhenOpen == BreakerOpenBehavior.Pause
            ? PauseAsync(breaker, context, cancellationToken)
            : ValueTask.FromException<BreakerPermit>(Refused(breaker,
                $"Circuit breaker for node '{breaker.NodeId}' is {Describe(breaker)}; the attempt was not made."));
    }

    public static void RecordSuccess(CircuitBreaker breaker, BreakerPermit permit, PipelineContext context)
    {
        Report(context, breaker, breaker.RecordSuccess(permit));
    }

    /// <summary>
    ///     Records a failed attempt. Only a transient failure counts against the breaker.
    /// </summary>
    public static void RecordFailure(CircuitBreaker breaker, BreakerPermit permit, bool isTransient, PipelineContext context)
    {
        if (isTransient)
            Report(context, breaker, breaker.RecordFailure(permit));
        else
            breaker.Release(permit);
    }

    private static async ValueTask<BreakerPermit> PauseAsync(CircuitBreaker breaker, PipelineContext context, CancellationToken cancellationToken)
    {
        var time = breaker.Time;
        var maxPause = breaker.Options.MaxPause;
        var deadline = time.GetTimestamp() + (long)(maxPause.TotalSeconds * time.TimestampFrequency);

        CircuitBreakerLogMessages.Pausing(CreateLogger(context), breaker.NodeId, maxPause);

        while (true)
        {
            var remaining = time.GetElapsedTime(time.GetTimestamp(), deadline);

            if (remaining <= TimeSpan.Zero)
            {
                throw Refused(breaker,
                    $"Circuit breaker for node '{breaker.NodeId}' stayed {Describe(breaker)} for the whole MaxPause of {maxPause}; the attempt was not made.");
            }

            // Read before trying, so a change between the refusal and the wait still wakes this attempt.
            var admissionChanged = breaker.AdmissionChanged;

            if (breaker.TryAcquire(out var permit, out var transition))
            {
                Report(context, breaker, transition);
                return permit;
            }

            Report(context, breaker, transition);

            // An open breaker admits a probe once its open period has passed; a half-open one when a probe finishes.
            var wait = breaker.TimeUntilHalfOpen() is { } untilHalfOpen && untilHalfOpen < remaining
                ? untilHalfOpen
                : remaining;

            try
            {
                await admissionChanged.WaitAsync(wait, time, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                // Time to look again.
            }
        }
    }

    private static CircuitBreakerOpenException Refused(CircuitBreaker breaker, string message) => new(breaker.NodeId, breaker.State, message);

    private static string Describe(CircuitBreaker breaker) => breaker.State == CircuitState.HalfOpen
        ? "half-open with every probe slot in use"
        : "open";

    /// <summary>
    ///     Logs a transition and reports it to the run's observer. Never throws.
    /// </summary>
    /// <remarks>
    ///     The transition has already happened in state shared across runs, and on acquire the caller holds a permit it
    ///     has not received yet. A logger or observer that throws must not fail the item, or the permit would leak and
    ///     could wedge the breaker half-open.
    /// </remarks>
    private static void Report(PipelineContext context, CircuitBreaker breaker, BreakerTransition transition)
    {
        if (!transition.Occurred)
            return;

        var reason = transition.Reason!;
        ILogger? logger = null;

        try
        {
            logger = CreateLogger(context);

            switch (transition.To)
            {
                case CircuitState.Open:
                    CircuitBreakerLogMessages.Opened(logger, breaker.NodeId, reason);
                    break;
                case CircuitState.HalfOpen:
                    CircuitBreakerLogMessages.HalfOpened(logger, breaker.NodeId, reason);
                    break;
                default:
                    CircuitBreakerLogMessages.Closed(logger, breaker.NodeId, reason);
                    break;
            }
        }
        catch
        {
            // A failing logger has nowhere to report to.
        }

        try
        {
            context.Observability.ExecutionObserver.OnCircuitStateChanged(new CircuitStateChangedEvent(
                breaker.NodeId, transition.From, transition.To, reason, context.RunIdentity.PipelineId, context.RunIdentity.PipelineName));
        }
        catch (Exception ex)
        {
            try
            {
                if (logger is not null)
                    CircuitBreakerLogMessages.StateChangeListenerFailed(logger, ex, breaker.NodeId, transition.From.ToString(), transition.To.ToString());
            }
            catch
            {
                // A failing logger has nowhere to report to.
            }
        }
    }

    private static ILogger CreateLogger(PipelineContext context) => context.Observability.LoggerFactory.CreateLogger(nameof(CircuitBreaker));
}

using NResilience;

namespace NPipeline.Connectors.Http.Reliability;

/// <summary>
///     Resilience presets for the HTTP connector nodes. Assign one to
///     <see cref="Configuration.HttpSourceConfiguration.Resilience" /> or
///     <see cref="Configuration.HttpSinkConfiguration.Resilience" />, or derive your own with a <c>with</c> expression.
/// </summary>
/// <remarks>
///     <para>
///         Each request runs through NResilience's <see cref="HttpResilienceHandler" />. It retries 408, 429, and 5xx
///         responses and network failures, honors <c>Retry-After</c>, and never retries POST or PATCH unless the request
///         carries an idempotency key.
///     </para>
///     <para>
///         <see cref="NResilience.Resilience.AttemptTimeout" /> is the per-request timeout. <see cref="NResilience.Resilience.None" />
///         has no timeout at all, so set one when you turn retries off:
///         <c>Resilience.None with { AttemptTimeout = TimeSpan.FromSeconds(30) }</c>.
///     </para>
/// </remarks>
public static class HttpConnectorResilience
{
    /// <summary>
    ///     <see cref="NResilience.Classifier.Http" />, plus a client timeout (a <see cref="TaskCanceledException" /> caused by
    ///     <see cref="HttpClient.Timeout" />) as transient.
    /// </summary>
    public static Classifier Classifier { get; } = Classifier.Http
        .On<TaskCanceledException>(static e => e.InnerException is TimeoutException
            ? Verdict.Transient
            : Verdict.Permanent);

    /// <summary>
    ///     Four attempts (three retries), a 30-second timeout on each, and exponential backoff with full jitter from
    ///     200 milliseconds up to 30 seconds. There is no overall deadline, so the attempt count bounds the call.
    /// </summary>
    public static Resilience Default { get; } = new()
    {
        Name = "npipeline.http",
        Attempts = 4,
        AttemptTimeout = TimeSpan.FromSeconds(30),
        Deadline = Timeout.InfiniteTimeSpan,
        Backoff = Backoff.Default with
        {
            TransientBase = TimeSpan.FromMilliseconds(200),
            MaximumDelay = TimeSpan.FromSeconds(30),
        },

        // Declared above so it is initialized first; a static initializer reads fields in declaration order.
        Classifier = Classifier,
        Adaptive = false,
    };

    /// <summary>
    ///     Three attempts (two retries) with exponential backoff from one second up to 60 seconds, for APIs that
    ///     prefer fewer, slower retries.
    /// </summary>
    public static Resilience Conservative { get; } = Default with
    {
        Name = "npipeline.http.conservative",
        Attempts = 3,
        Backoff = Default.Backoff with
        {
            TransientBase = TimeSpan.FromSeconds(1),
            MaximumDelay = TimeSpan.FromSeconds(60),
        },
    };
}

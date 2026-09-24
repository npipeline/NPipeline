namespace NPipeline.Reliability;

/// <summary>
///     The shape of a backoff curve.
/// </summary>
public enum RetryBackoffKind
{
    /// <summary>
    ///     No delay between retries.
    /// </summary>
    None,

    /// <summary>
    ///     The same delay before every retry.
    /// </summary>
    Constant,

    /// <summary>
    ///     A delay that grows by <see cref="RetryBackoff.BaseDelay" /> with each retry.
    /// </summary>
    Linear,

    /// <summary>
    ///     A delay that grows by <see cref="RetryBackoff.Factor" /> with each retry.
    /// </summary>
    Exponential,

    /// <summary>
    ///     A delay computed by <see cref="RetryBackoff.CustomDelay" />.
    /// </summary>
    Custom,
}

/// <summary>
///     How a computed delay is randomized, so that many retrying callers do not retry in lockstep.
/// </summary>
public enum RetryJitter
{
    /// <summary>
    ///     The computed delay is used as is.
    /// </summary>
    None,

    /// <summary>
    ///     A delay drawn uniformly between zero and the computed delay.
    /// </summary>
    Full,

    /// <summary>
    ///     Half the computed delay, plus a delay drawn uniformly between zero and the other half.
    /// </summary>
    Equal,
}

/// <summary>
///     How long to wait before each retry. A value type with no per-sequence state: the delay depends only on the
///     retry number, so nothing is shared between the items or nodes that use the same backoff.
/// </summary>
/// <remarks>
///     <para>
///         Build one with a factory method and adjust it with <c>with</c>:
///     </para>
///     <code>
///     var backoff = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(200), maxDelay: TimeSpan.FromSeconds(30));
///     var slower = backoff with { Factor = 3 };
///     </code>
///     <para>
///         <see cref="MaxDelay" /> caps the delay after jitter is applied. <see cref="TimeSpan.Zero" />, the default,
///         means no cap.
///     </para>
///     <para>
///         Each property rejects an out-of-range value when it is set, so a <c>with</c> expression fails where it is
///         written. <see cref="Validate" /> also checks the combination (an exponential backoff needs a factor, a
///         custom one a delay function); the factories call it, and so does <c>Build()</c>.
///     </para>
/// </remarks>
public readonly record struct RetryBackoff
{
    // Task.Delay rejects anything longer, and no retry should wait longer than this anyway.
    private static readonly TimeSpan Ceiling = TimeSpan.FromMilliseconds(int.MaxValue);

    /// <summary>
    ///     The shape of the curve.
    /// </summary>
    public RetryBackoffKind Kind
    {
        get;
        init => field = Enum.IsDefined(value)
            ? value
            : throw new ArgumentOutOfRangeException(nameof(Kind), value, "Unknown backoff kind.");
    }

    /// <summary>
    ///     The delay before the first retry, and the step for <see cref="RetryBackoffKind.Linear" />.
    /// </summary>
    public TimeSpan BaseDelay
    {
        get;
        init => field = value >= TimeSpan.Zero
            ? value
            : throw new ArgumentOutOfRangeException(nameof(BaseDelay), value, "The base delay cannot be negative.");
    }

    /// <summary>
    ///     The multiplier between consecutive delays for <see cref="RetryBackoffKind.Exponential" />.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The factor is less than 1 or not a number.</exception>
    public double Factor
    {
        get;
        init => field = value >= 1
            ? value
            : throw new ArgumentOutOfRangeException(nameof(Factor), value, "The factor must be at least 1.");
    }

    /// <summary>
    ///     The longest delay, applied after jitter. <see cref="TimeSpan.Zero" /> means no cap.
    /// </summary>
    public TimeSpan MaxDelay
    {
        get;
        init => field = value >= TimeSpan.Zero
            ? value
            : throw new ArgumentOutOfRangeException(nameof(MaxDelay), value, "The maximum delay cannot be negative. Use TimeSpan.Zero for no cap.");
    }

    /// <summary>
    ///     How the computed delay is randomized.
    /// </summary>
    public RetryJitter Jitter
    {
        get;
        init => field = Enum.IsDefined(value)
            ? value
            : throw new ArgumentOutOfRangeException(nameof(Jitter), value, "Unknown jitter kind.");
    }

    /// <summary>
    ///     Computes the delay before a retry from its 1-based number, for <see cref="RetryBackoffKind.Custom" />.
    /// </summary>
    public Func<int, TimeSpan>? CustomDelay { get; init; }

    /// <summary>
    ///     No delay between retries.
    /// </summary>
    public static RetryBackoff None => default;

    /// <summary>
    ///     The same delay before every retry.
    /// </summary>
    /// <param name="delay">The delay.</param>
    /// <param name="jitter">How the delay is randomized. Default: none.</param>
    public static RetryBackoff Constant(TimeSpan delay, RetryJitter jitter = RetryJitter.None) =>
        new RetryBackoff { Kind = RetryBackoffKind.Constant, BaseDelay = delay, Jitter = jitter }.Validated();

    /// <summary>
    ///     A delay of <paramref name="step" /> times the retry number.
    /// </summary>
    /// <param name="step">The delay before the first retry, and the amount added for each retry after it.</param>
    /// <param name="maxDelay">The longest delay. Default: no cap.</param>
    /// <param name="jitter">How the delay is randomized. Default: <see cref="RetryJitter.Equal" />.</param>
    public static RetryBackoff Linear(TimeSpan step, TimeSpan? maxDelay = null, RetryJitter jitter = RetryJitter.Equal) =>
        new RetryBackoff { Kind = RetryBackoffKind.Linear, BaseDelay = step, MaxDelay = maxDelay ?? TimeSpan.Zero, Jitter = jitter }
            .Validated();

    /// <summary>
    ///     A delay of <paramref name="baseDelay" /> times <paramref name="factor" /> raised to the retry number minus one.
    /// </summary>
    /// <param name="baseDelay">The delay before the first retry.</param>
    /// <param name="factor">The multiplier between consecutive delays. Default: 2.</param>
    /// <param name="maxDelay">The longest delay. Default: no cap.</param>
    /// <param name="jitter">How the delay is randomized. Default: <see cref="RetryJitter.Full" />.</param>
    public static RetryBackoff Exponential(TimeSpan baseDelay, double factor = 2, TimeSpan? maxDelay = null, RetryJitter jitter = RetryJitter.Full) =>
        new RetryBackoff
        {
            Kind = RetryBackoffKind.Exponential,
            BaseDelay = baseDelay,
            Factor = factor,
            MaxDelay = maxDelay ?? TimeSpan.Zero,
            Jitter = jitter,
        }.Validated();

    /// <summary>
    ///     A delay computed by <paramref name="delayForRetry" /> from the 1-based retry number. The result is not
    ///     jittered; a negative result is treated as zero.
    /// </summary>
    /// <param name="delayForRetry">Computes the delay before a retry.</param>
    public static RetryBackoff Custom(Func<int, TimeSpan> delayForRetry)
    {
        ArgumentNullException.ThrowIfNull(delayForRetry);
        return new RetryBackoff { Kind = RetryBackoffKind.Custom, CustomDelay = delayForRetry };
    }

    /// <summary>
    ///     The delay before retry number <paramref name="retry" />.
    /// </summary>
    /// <param name="retry">The 1-based number of the retry about to be made.</param>
    /// <returns>The delay. Jitter draws from <see cref="Random.Shared" />.</returns>
    public TimeSpan DelayFor(int retry)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(retry, 1);

        var raw = Kind switch
        {
            RetryBackoffKind.None => 0d,
            RetryBackoffKind.Constant => BaseDelay.TotalMilliseconds,
            RetryBackoffKind.Linear => BaseDelay.TotalMilliseconds * retry,
            RetryBackoffKind.Exponential => BaseDelay.TotalMilliseconds * Math.Pow(Factor, retry - 1),
            RetryBackoffKind.Custom => CustomDelay is null
                ? 0d
                : CustomDelay(retry).TotalMilliseconds,
            _ => throw new InvalidOperationException($"Unknown backoff kind {Kind}."),
        };

        if (raw <= 0 || double.IsNaN(raw))
            return TimeSpan.Zero;

        var cap = MaxDelay > TimeSpan.Zero && MaxDelay < Ceiling
            ? MaxDelay
            : Ceiling;

        raw = Math.Min(raw, cap.TotalMilliseconds);

        if (Kind != RetryBackoffKind.Custom)
        {
            raw = Jitter switch
            {
                RetryJitter.Full => Random.Shared.NextDouble() * raw,
                RetryJitter.Equal => raw / 2 + Random.Shared.NextDouble() * raw / 2,
                _ => raw,
            };
        }

        return TimeSpan.FromMilliseconds(raw);
    }

    /// <summary>
    ///     Throws when the backoff cannot produce a delay.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">A delay or the factor is out of range.</exception>
    /// <exception cref="ArgumentException">A custom backoff has no delay function.</exception>
    public void Validate()
    {
        if (!Enum.IsDefined(Kind))
            throw new ArgumentOutOfRangeException(nameof(Kind), Kind, "Unknown backoff kind.");

        if (!Enum.IsDefined(Jitter))
            throw new ArgumentOutOfRangeException(nameof(Jitter), Jitter, "Unknown jitter kind.");

        if (BaseDelay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(BaseDelay), BaseDelay, "The base delay cannot be negative.");

        if (MaxDelay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(MaxDelay), MaxDelay, "The maximum delay cannot be negative. Use TimeSpan.Zero for no cap.");

        if (Kind == RetryBackoffKind.Exponential && (double.IsNaN(Factor) || Factor < 1))
            throw new ArgumentOutOfRangeException(nameof(Factor), Factor, "An exponential backoff needs a factor of at least 1.");

        if (Kind == RetryBackoffKind.Custom && CustomDelay is null)
            throw new ArgumentException("A custom backoff needs a delay function.", nameof(CustomDelay));
    }

    private RetryBackoff Validated()
    {
        Validate();
        return this;
    }
}

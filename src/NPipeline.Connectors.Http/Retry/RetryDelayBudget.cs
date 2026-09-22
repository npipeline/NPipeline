namespace NPipeline.Connectors.Http.Retry;

/// <summary>
///     Tracks the retry delay spent on one request against <see cref="IHttpRetryStrategy.MaxTotalRetryDelay" />.
/// </summary>
/// <remarks>
///     Each request owns its budget. A budget kept on the strategy would be shared by every request that uses it,
///     so one struggling endpoint would spend the budget for all of them.
/// </remarks>
internal struct RetryDelayBudget(TimeSpan? limit)
{
    private readonly TimeSpan? _limit = limit;
    private TimeSpan _spent;

    /// <summary>
    ///     Gets whether the request has used its whole budget, in which case it must stop retrying.
    /// </summary>
    public readonly bool IsSpent => _limit is { } limit && _spent >= limit;

    /// <summary>
    ///     Clamps <paramref name="delay" /> to the budget that remains and records it as spent.
    /// </summary>
    /// <param name="delay">The delay the retry strategy asked for.</param>
    /// <returns>The delay to wait before the next attempt.</returns>
    public TimeSpan Spend(TimeSpan delay)
    {
        if (_limit is { } limit && delay > limit - _spent)
            delay = limit - _spent;

        _spent += delay;
        return delay;
    }
}

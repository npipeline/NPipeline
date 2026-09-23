using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Postgres.Reliability;

/// <summary>
///     Runs one unit of work on a connection through the configured policy.
/// </summary>
/// <remarks>
///     <para>
///         The caller passes a unit that commits all or nothing, so a failed attempt has written nothing and retrying it
///         cannot duplicate rows.
///     </para>
///     <para>
///         Inside a transaction the caller does not own, a failed statement may have rolled back or poisoned the whole
///         transaction, taking earlier units with it. Retrying the statement there would either fail again or commit it
///         without the units it depends on, so the unit runs once and the failure goes to the transaction's owner.
///     </para>
/// </remarks>
internal sealed class ConnectionResilience
{
    private readonly IDatabaseConnection _connection;
    private readonly NResilience.Resilience _policy;
    private readonly NResilience.Resilience _singleAttempt;

    public ConnectionResilience(NResilience.Resilience policy, IDatabaseConnection connection)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _policy = policy;
        _singleAttempt = policy with { Attempts = 1 };
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    public async ValueTask RunAsync(Func<CancellationToken, Task> unit, CancellationToken cancellationToken)
    {
        _ = await RunAsync(async ct =>
        {
            await unit(ct).ConfigureAwait(false);
            return true;
        }, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<TResult> RunAsync<TResult>(Func<CancellationToken, Task<TResult>> unit, CancellationToken cancellationToken)
    {
        if (_connection.CurrentTransaction is not null)
            return _singleAttempt.RunAsync(unit, cancellationToken);

        var attempt = 0;

        return _policy.RunAsync(async ct =>
        {
            // A connection-level failure closes the connection; reopen it so the retry has somewhere to run.
            if (attempt++ > 0 && !_connection.IsOpen)
            {
                await _connection.CloseAsync(ct).ConfigureAwait(false);
                await _connection.OpenAsync(ct).ConfigureAwait(false);
            }

            return await unit(ct).ConfigureAwait(false);
        }, cancellationToken);
    }
}

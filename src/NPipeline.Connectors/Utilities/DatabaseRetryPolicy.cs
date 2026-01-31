namespace NPipeline.Connectors.Utilities;

/// <summary>
///     Retry policy for handling transient database errors.
/// </summary>
public class DatabaseRetryPolicy
{
    /// <summary>
    ///     Gets or sets maximum retry attempts.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    ///     Gets or sets initial delay between retries.
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets or sets maximum delay between retries.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     Gets or sets function to determine if exception should trigger retry.
    /// </summary>
    public Func<Exception, bool>? ShouldRetry { get; set; }

    /// <summary>
    ///     Executes operation with retry logic.
    /// </summary>
    /// <typeparam name="T">The return type.</typeparam>
    /// <param name="operation">The operation to execute.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result of the operation.</returns>
    public async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        var delay = InitialDelay;

        while (true)
        {
            attempt++;

            try
            {
                return await operation(cancellationToken);
            }
            catch (Exception ex) when (ShouldRetry?.Invoke(ex) == true && attempt < MaxRetryAttempts)
            {
                await Task.Delay(delay, cancellationToken);

                delay = TimeSpan.FromMilliseconds(
                    Math.Min(delay.TotalMilliseconds * 2, MaxDelay.TotalMilliseconds));
            }
        }
    }
}

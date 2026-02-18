using System.Net;
using Google;

namespace NPipeline.StorageProviders.Gcp;

internal sealed class GcsRetryPolicy
{
    private readonly GcsRetrySettings? _settings;

    public GcsRetryPolicy(GcsRetrySettings? settings)
    {
        _settings = settings;
    }

    private bool IsEnabled =>
        _settings is { MaxAttempts: > 0 } settings &&
        (settings.RetryOnRateLimit || settings.RetryOnServerErrors);

    public async Task ExecuteAsync(
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);

        if (!IsEnabled)
        {
            await operation(cancellationToken).ConfigureAwait(false);
            return;
        }

        var settings = _settings!;
        var nextDelay = settings.InitialDelay;

        for (var attempt = 0;; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await operation(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (GoogleApiException ex) when (attempt < settings.MaxAttempts && ShouldRetry(ex))
            {
                var delay = GetClampedDelay(nextDelay, settings.MaxDelay);

                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);

                nextDelay = GetNextDelay(delay, settings.DelayMultiplier, settings.MaxDelay);
            }
        }
    }

    public async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);

        if (!IsEnabled)
            return await operation(cancellationToken).ConfigureAwait(false);

        var settings = _settings!;
        var nextDelay = settings.InitialDelay;

        for (var attempt = 0;; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await operation(cancellationToken).ConfigureAwait(false);
            }
            catch (GoogleApiException ex) when (attempt < settings.MaxAttempts && ShouldRetry(ex))
            {
                var delay = GetClampedDelay(nextDelay, settings.MaxDelay);

                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);

                nextDelay = GetNextDelay(delay, settings.DelayMultiplier, settings.MaxDelay);
            }
        }
    }

    private bool ShouldRetry(GoogleApiException exception)
    {
        var statusCode = (int)exception.HttpStatusCode;

        if (_settings!.RetryOnRateLimit && statusCode == (int)HttpStatusCode.TooManyRequests)
            return true;

        if (_settings.RetryOnServerErrors && statusCode is >= 500 and < 600)
            return true;

        return false;
    }

    private static TimeSpan GetClampedDelay(TimeSpan delay, TimeSpan maxDelay)
    {
        if (delay < TimeSpan.Zero)
            return TimeSpan.Zero;

        return delay > maxDelay
            ? maxDelay
            : delay;
    }

    private static TimeSpan GetNextDelay(TimeSpan currentDelay, double multiplier, TimeSpan maxDelay)
    {
        if (currentDelay == TimeSpan.Zero)
            return TimeSpan.Zero;

        var nextTicks = currentDelay.Ticks * multiplier;
        var boundedTicks = Math.Min(nextTicks, maxDelay.Ticks);
        return new TimeSpan((long)boundedTicks);
    }
}

using System.Runtime.CompilerServices;
using Google;
using Google.Apis.Http;

namespace NPipeline.StorageProviders.Gcp.Reliability;

/// <summary>
///     Carries a response's <c>Retry-After</c> header to the classifier. <see cref="GoogleApiException" /> keeps only
///     the status code, so a response handler on the client records the header for the request in flight, and
///     <see cref="CaptureAsync{T}" /> copies it onto the exception's <see cref="Exception.Data" />.
/// </summary>
internal static class GcsRetryAfter
{
    internal const string DataKey = "NPipeline.Gcs.RetryAfter";

    private static readonly AsyncLocal<StrongBox<TimeSpan?>?> Current = new();

    /// <summary>The response handler that clients built by <see cref="GcsClientFactory" /> carry.</summary>
    internal static IHttpUnsuccessfulResponseHandler Handler { get; } = new RetryAfterHandler();

    internal static async Task<T> CaptureAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)
    {
        var box = new StrongBox<TimeSpan?>();

        // Set inside this async method, so the value flows into the request and is gone when the method returns.
        Current.Value = box;

        try
        {
            return await operation(cancellationToken).ConfigureAwait(false);
        }
        catch (GoogleApiException ex) when (box.Value is { } retryAfter)
        {
            ex.Data[DataKey] = retryAfter;
            throw;
        }
    }

    internal static TimeSpan? Read(Exception exception) => exception.Data[DataKey] as TimeSpan?;

    private sealed class RetryAfterHandler : IHttpUnsuccessfulResponseHandler
    {
        public Task<bool> HandleResponseAsync(HandleUnsuccessfulResponseArgs args)
        {
            if (Current.Value is { } box && args.Response.Headers.RetryAfter is { } header)
            {
                var delay = header.Delta ?? (header.Date is { } date
                    ? date - DateTimeOffset.UtcNow
                    : null);

                if (delay is { } value)
                    box.Value = value < TimeSpan.Zero
                        ? TimeSpan.Zero
                        : value;
            }

            // Never asks the SDK to retry: NResilience owns retries.
            return Task.FromResult(false);
        }
    }
}

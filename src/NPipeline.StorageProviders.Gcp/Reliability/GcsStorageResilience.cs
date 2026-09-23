using System.Net;
using Google;
using NResilience;

namespace NPipeline.StorageProviders.Gcp.Reliability;

/// <summary>
///     Resilience presets for the Google Cloud Storage provider. Assign one to
///     <see cref="GcsStorageProviderOptions.Resilience" />, or derive your own with a <c>with</c> expression.
/// </summary>
/// <remarks>
///     <para>
///         The provider runs each GCS request (metadata, list page, download, and upload) through this policy. Exactly one
///         layer retries: clients built by <see cref="GcsClientFactory" /> send each HTTP request once
///         (<c>ConfigurableMessageHandler.NumTries = 1</c>), which turns off the Google SDK's own retry of metadata
///         calls and its in-session resume of resumable uploads, and metadata calls pass
///         <c>RetryOptions.Never</c>. A retried download starts again with an empty buffer, and a retried upload
///         re-sends the whole object from its first byte, so neither can leave a partial or duplicated object.
///     </para>
///     <para>
///         <see cref="NResilience.Resilience.AttemptTimeout" /> and <see cref="NResilience.Resilience.Deadline" />
///         are infinite in these presets, because a download or upload of a large object can run for a long time. The
///         SDK's HTTP client timeout (100 seconds by default) still bounds each HTTP request.
///     </para>
/// </remarks>
public static class GcsStorageResilience
{
    /// <summary>
    ///     <see cref="NResilience.Classifier.Default" /> (timeouts, <see cref="IOException" />, and socket errors are
    ///     transient) plus GCS knowledge: a <see cref="GoogleApiException" /> with status 429 is throttled and honors
    ///     <c>Retry-After</c> when the server sends one; 408 and 5xx are transient; every other status is permanent.
    ///     An <see cref="HttpRequestException" /> (a network failure) and an HTTP client timeout are transient.
    /// </summary>
    public static Classifier Classifier { get; } = Classifier.Default
        .On<GoogleApiException>(Classify)
        .On<HttpRequestException>(Verdict.Transient)
        .On<TaskCanceledException>(static e => e.InnerException is TimeoutException
            ? Verdict.Transient
            : Verdict.Permanent);

    /// <summary>
    ///     Three attempts (two retries) with exponential backoff and full jitter from one second up to 32 seconds. This
    ///     matches the Google SDK's default retry of idempotent requests (three tries, one second doubling to 32
    ///     seconds), which was the effective retry before this policy replaced it, and the delays of the removed
    ///     <c>GcsRetrySettings</c>.
    /// </summary>
    public static NResilience.Resilience Default { get; } = new()
    {
        Name = "npipeline.gcs",
        Attempts = 3,
        AttemptTimeout = Timeout.InfiniteTimeSpan,
        Deadline = Timeout.InfiniteTimeSpan,
        Backoff = Backoff.Default with
        {
            TransientBase = TimeSpan.FromSeconds(1),
            ThrottledBase = TimeSpan.FromSeconds(1),
            MaximumDelay = TimeSpan.FromSeconds(32),
        },
        // Declared above so it is initialized first; a static initializer reads fields in declaration order.
        Classifier = Classifier,
        Adaptive = false,
    };

    private static Verdict Classify(GoogleApiException exception)
    {
        var status = (int)exception.HttpStatusCode;

        if (status == (int)HttpStatusCode.TooManyRequests)
            return Verdict.Throttled(GcsRetryAfter.Read(exception));

        return status is (int)HttpStatusCode.RequestTimeout or >= 500 and < 600
            ? Verdict.Transient
            : Verdict.Permanent;
    }
}

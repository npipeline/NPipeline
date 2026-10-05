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
///         The provider runs each metadata, list, delete and copy request through this policy, and reopens a download
///         that fails part-way from the failed offset (<see cref="GcsResumingReadStream" />). Exactly one layer retries
///         each request: metadata calls pass <c>RetryOptions.Never</c>, so the Google SDK does not retry them as well.
///         Writes are not run through this policy, because a stream cannot be replayed: they retry inside the SDK's
///         resumable-upload session, which re-sends from the offset the server reports.
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
    public static Resilience Default { get; } = new()
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

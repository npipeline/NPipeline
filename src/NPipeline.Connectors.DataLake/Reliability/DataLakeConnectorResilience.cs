using NResilience;

namespace NPipeline.Connectors.DataLake.Reliability;

/// <summary>
///     Resilience presets for the Data Lake connector. <see cref="ManifestWrite" /> is what
///     <see cref="Manifest.ManifestWriter" /> uses to append to the table's main manifest, and <see cref="ManifestRead" /> is
///     what <see cref="Manifest.ManifestReader" /> uses; pass your own to their constructors, or derive one with a
///     <c>with</c> expression.
/// </summary>
/// <remarks>
///     <para>
///         A manifest append is a read-modify-write of <c>_manifest/manifest.ndjson</c> with no conditional write, so a
///         retry never detects or resolves a conflict with another writer; it only rides over a transient storage error.
///         Each attempt re-reads the manifest and skips the append when the entries are already there, so retrying an
///         attempt that committed before it failed does not duplicate entries.
///     </para>
///     <para>
///         The Azure, AWS, and Google storage SDKs behind the cloud storage providers also retry each request natively.
///         These presets retry the whole append (read, write, and rename), which the SDKs cannot.
///     </para>
/// </remarks>
public static class DataLakeConnectorResilience
{
    /// <summary>
    ///     <see cref="NResilience.Classifier.Default" /> (<see cref="IOException" />, <see cref="TimeoutException" />, and
    ///     socket errors are transient; anything else is permanent), with the <see cref="IOException" /> subclasses that
    ///     mean a missing or invalid path marked permanent. The storage providers report 401 and 403 as
    ///     <see cref="UnauthorizedAccessException" />, which is permanent. An <see cref="AggregateException" /> is transient
    ///     when any of its inner exceptions is.
    /// </summary>
    public static Classifier ManifestClassifier { get; } = CreateManifestClassifier();

    /// <summary>
    ///     Three attempts (two retries), exponential backoff with full jitter from 100 milliseconds, and no attempt timeout
    ///     or deadline. Replaces the manifest writer's fixed three attempts 100 milliseconds apart.
    /// </summary>
    /// <remarks>
    ///     There is no attempt timeout because cancelling an append part-way through a non-atomic overwrite (on a provider
    ///     without <see cref="StorageProviders.Abstractions.IMoveableStorageProvider" />) can truncate the manifest. The
    ///     attempt count bounds the call; the caller's cancellation token still applies.
    /// </remarks>
    public static NResilience.Resilience ManifestWrite { get; } = new()
    {
        Name = "npipeline.datalake.manifest",
        Attempts = 3,
        AttemptTimeout = Timeout.InfiniteTimeSpan,
        Deadline = Timeout.InfiniteTimeSpan,
        Backoff = Backoff.Default with
        {
            TransientBase = TimeSpan.FromMilliseconds(100),
        },
        // Declared above so it is initialized first; a static initializer reads fields in declaration order.
        Classifier = ManifestClassifier,
        Adaptive = false,
    };

    /// <summary>
    ///     <see cref="ManifestWrite" />'s attempts, backoff, and classifier for reading the manifest and its snapshot files.
    ///     Reads are idempotent, so each <see cref="Manifest.ManifestReader" /> call is retried as a whole.
    /// </summary>
    public static NResilience.Resilience ManifestRead { get; } = ManifestWrite with
    {
        Name = "npipeline.datalake.manifest.read",
    };

    private static Classifier CreateManifestClassifier()
    {
        var paths = Classifier.Default
            .On<FileNotFoundException>(Verdict.Permanent)
            .On<DirectoryNotFoundException>(Verdict.Permanent)
            .On<PathTooLongException>(Verdict.Permanent);

        return paths.On<AggregateException>(e => e.Flatten().InnerExceptions
            .Any(inner => paths.ClassifyException(inner).Kind != VerdictKind.Permanent)
            ? Verdict.Transient
            : Verdict.Permanent);
    }
}

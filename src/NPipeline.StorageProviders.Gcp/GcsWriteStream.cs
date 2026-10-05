using Google;
using Google.Cloud.Storage.V1;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Gcp.Reliability;
using NResilience;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace NPipeline.StorageProviders.Gcp;

/// <summary>
///     A write-only stream that buffers data to a local temp file and uploads it to Google Cloud Storage in
///     <see cref="StorageWriteStream.CommitAsync" />. Disposing without committing uploads nothing.
/// </summary>
public sealed class GcsWriteStream : SpooledWriteStream
{
    private readonly string _bucket;
    private readonly int _chunkSizeBytes;
    private readonly string? _contentType;
    private readonly string _objectName;
    private readonly Resilience _resilience;
    private readonly StorageClient _storageClient;

    /// <summary>
    ///     Initializes a new instance of the <see cref="GcsWriteStream" /> class. The upload is sent once, without retries;
    ///     streams opened through <see cref="GcsStorageProvider" /> retry according to
    ///     <see cref="GcsStorageProviderOptions.Resilience" />.
    /// </summary>
    /// <param name="storageClient">The Google Cloud Storage client.</param>
    /// <param name="bucket">The GCS bucket name.</param>
    /// <param name="objectName">The GCS object name (key).</param>
    /// <param name="contentType">Optional content type for the upload.</param>
    /// <param name="chunkSizeBytes">Chunk size for resumable uploads. Default is 16 MB.</param>
    public GcsWriteStream(
        StorageClient storageClient,
        string bucket,
        string objectName,
        string? contentType = null,
        int chunkSizeBytes = 16 * 1024 * 1024)
        : this(storageClient, bucket, objectName, contentType, chunkSizeBytes, Resilience.None)
    {
    }

    internal GcsWriteStream(
        StorageClient storageClient,
        string bucket,
        string objectName,
        string? contentType,
        int chunkSizeBytes,
        Resilience resilience)
        : base("gcs-upload")
    {
        _storageClient = storageClient ?? throw new ArgumentNullException(nameof(storageClient));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _objectName = objectName ?? throw new ArgumentNullException(nameof(objectName));
        _contentType = contentType;
        _resilience = resilience ?? throw new ArgumentNullException(nameof(resilience));

        if (chunkSizeBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(chunkSizeBytes), "Chunk size must be a positive number of bytes.");

        _chunkSizeBytes = chunkSizeBytes;
    }

    /// <inheritdoc />
    protected override async Task<string?> UploadAsync(Stream content, CancellationToken cancellationToken)
    {
        try
        {
            var uploadOptions = new UploadObjectOptions { ChunkSize = _chunkSizeBytes };
            var obj = new Object { Bucket = _bucket, Name = _objectName };

            if (!string.IsNullOrEmpty(_contentType))
                obj.ContentType = _contentType;

            // Each attempt uploads the whole object from its first byte, in a new upload session. Re-sending an
            // object replaces it, so a retry cannot leave a partial or duplicated object.
            var uploaded = await _resilience.RunAsync(
                token => GcsRetryAfter.CaptureAsync(
                    t =>
                    {
                        content.Position = 0;
                        return _storageClient.UploadObjectAsync(obj, content, uploadOptions, t);
                    },
                    token),
                cancellationToken).ConfigureAwait(false);

            var etag = uploaded?.ETag;

            return string.IsNullOrEmpty(etag) ? null : etag;
        }
        catch (GoogleApiException ex)
        {
            throw GcsErrors.Translate(ex, _bucket, _objectName, "upload");
        }
    }
}

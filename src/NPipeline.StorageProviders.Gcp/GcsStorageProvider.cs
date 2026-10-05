using System.Collections;
using System.Net;
using System.Runtime.CompilerServices;
using Google;
using Google.Apis.Storage.v1.Data;
using Google.Cloud.Storage.V1;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Gcp.Reliability;
using NPipeline.StorageProviders.Models;
using NResilience;

namespace NPipeline.StorageProviders.Gcp;

/// <summary>
///     Storage provider for Google Cloud Storage that derives from <see cref="StorageProvider" />.
///     Handles "gs" scheme URIs and supports reading, writing, listing, deleting, moving and metadata operations.
/// </summary>
/// <remarks>
///     <para>
///         - Async-first API design
///         - Stream-based I/O for scalability
///         - Proper error handling and exception translation
///         - Cancellation token support throughout
///         - Thread-safe implementation
///         - Consistent with existing S3/Azure provider patterns
///     </para>
///     <para>
///         URI format: gs://bucket-name/path/to/object
///         Supported URI parameters: projectId, contentType, serviceUrl, accessToken, credentialsPath
///     </para>
/// </remarks>
public sealed class GcsStorageProvider : StorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> SupportedSchemes = [StorageScheme.Gcs];

    // The SDK's own retry of metadata calls is off, so NResilience is the only layer that retries them.
    private static readonly GetObjectOptions GetObjectOnce = new() { RetryOptions = RetryOptions.Never };

    private static readonly DeleteObjectOptions DeleteObjectOnce = new() { RetryOptions = RetryOptions.Never };

    private readonly GcsClientFactory _clientFactory;
    private readonly GcsStorageProviderOptions _options;
    private readonly Resilience _resilience;

    /// <summary>
    ///     Initializes a new instance of the <see cref="GcsStorageProvider" /> class.
    /// </summary>
    /// <param name="clientFactory">The GCS client factory.</param>
    /// <param name="options">The GCS storage provider options.</param>
    public GcsStorageProvider(GcsClientFactory clientFactory, GcsStorageProviderOptions options)
    {
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _resilience = _options.Resilience ?? throw new ArgumentException("Resilience must not be null.", nameof(options));
    }

    /// <inheritdoc />
    public override string Name => "Google Cloud Storage";

    /// <inheritdoc />
    public override IReadOnlyList<StorageScheme> Schemes => SupportedSchemes;

    /// <inheritdoc />
    /// <remarks>GCS is a flat object store, so it declares neither hierarchy nor atomic move: a move is a copy followed by a delete.</remarks>
    public override StorageCapabilities Capabilities =>
        StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete | StorageCapabilities.Move;

    /// <inheritdoc />
    protected override async Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (bucket, objectName) = GetBucketAndObjectName(uri, true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);
        var tempFilePath = Path.Combine(Path.GetTempPath(), $"gcs-download-{Guid.NewGuid():N}.tmp");
        FileStream? tempFileStream = null;

        try
        {
            tempFileStream = new FileStream(
                tempFilePath,
                FileMode.Create,
                FileAccess.ReadWrite,
                FileShare.None,
                81920,
                FileOptions.Asynchronous | FileOptions.DeleteOnClose);

            _ = await RunAsync(
                token =>
                {
                    // A failed attempt can leave part of the object in the buffer, so each attempt starts empty.
                    tempFileStream.SetLength(0);
                    return client.DownloadObjectAsync(bucket, objectName, tempFileStream, cancellationToken: token);
                },
                cancellationToken).ConfigureAwait(false);

            tempFileStream.Position = 0;
            return new GcsReadStream(tempFileStream);
        }
        catch (GoogleApiException ex)
        {
            if (tempFileStream is not null)
                await tempFileStream.DisposeAsync().ConfigureAwait(false);

            throw GcsErrors.Translate(ex, bucket, objectName, "read");
        }
        catch
        {
            if (tempFileStream is not null)
                await tempFileStream.DisposeAsync().ConfigureAwait(false);

            throw;
        }
    }

    /// <inheritdoc />
    protected override async Task<StorageWriteStream> OpenWriteCoreAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken)
    {
        var (bucket, objectName) = GetBucketAndObjectName(uri, true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        // The write options win; the contentType URI parameter is the fallback.
        var contentType = !string.IsNullOrEmpty(options?.ContentType)
            ? options.ContentType
            : uri.Parameters.TryGetValue("contentType", out var ct) && !string.IsNullOrEmpty(ct)
                ? ct
                : null;

        return new GcsWriteStream(
            client,
            bucket,
            objectName,
            contentType,
            _options.UploadChunkSizeBytes,
            _resilience);
    }

    /// <inheritdoc />
    protected override async Task<bool> ExistsCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (bucket, objectName) = GetBucketAndObjectName(uri, true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            _ = await RunAsync(
                token => client.GetObjectAsync(bucket, objectName, GetObjectOnce, token),
                cancellationToken).ConfigureAwait(false);

            return true;
        }
        catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
        catch (GoogleApiException ex)
        {
            throw GcsErrors.Translate(ex, bucket, objectName, "exists");
        }
    }

    /// <inheritdoc />
    protected override IAsyncEnumerable<StorageItem> ListCoreAsync(StorageUri directory, bool recursive, CancellationToken cancellationToken) =>
        ListAsyncCore(directory, recursive, cancellationToken);

    /// <inheritdoc />
    protected override async Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (bucket, objectName) = GetBucketAndObjectName(uri, true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            var obj = await RunAsync(
                token => client.GetObjectAsync(bucket, objectName, GetObjectOnce, token),
                cancellationToken).ConfigureAwait(false);

            var customMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Add GCS object metadata
            if (obj.Metadata is not null)
            {
                foreach (var kvp in obj.Metadata)
                {
                    customMetadata[kvp.Key] = kvp.Value;
                }
            }

            return new StorageMetadata
            {
                Size = (long)(obj.Size ?? 0),
                LastModified = obj.UpdatedDateTimeOffset,
                ContentType = obj.ContentType,
                ETag = obj.ETag,
                CustomMetadata = customMetadata,
                IsDirectory = false,
            };
        }
        catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
        catch (GoogleApiException ex)
        {
            throw GcsErrors.Translate(ex, bucket, objectName, "metadata");
        }
    }

    /// <inheritdoc />
    protected override async Task DeleteCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (bucket, objectName) = GetBucketAndObjectName(uri, true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            await DeleteObjectAsync(client, bucket, objectName, cancellationToken).ConfigureAwait(false);
        }
        catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
        {
            // Deleting a missing object succeeds.
        }
        catch (GoogleApiException ex)
        {
            throw GcsErrors.Translate(ex, bucket, objectName, "delete");
        }
    }

    /// <inheritdoc />
    /// <remarks>
    ///     GCS has no rename. The object is copied with a server-side rewrite and then the source is deleted, so a failure
    ///     between the two steps leaves both objects. The destination is overwritten.
    /// </remarks>
    protected override async Task MoveCoreAsync(StorageUri source, StorageUri destination, CancellationToken cancellationToken)
    {
        var (sourceBucket, sourceName) = GetBucketAndObjectName(source, true);
        var (destinationBucket, destinationName) = GetBucketAndObjectName(destination, true);
        var client = await _clientFactory.GetClientAsync(source, cancellationToken).ConfigureAwait(false);

        try
        {
            // Copying an object onto itself and then deleting the source would lose it.
            if (string.Equals(sourceBucket, destinationBucket, StringComparison.Ordinal)
                && string.Equals(sourceName, destinationName, StringComparison.Ordinal))
            {
                _ = await RunAsync(
                    token => client.GetObjectAsync(sourceBucket, sourceName, GetObjectOnce, token),
                    cancellationToken).ConfigureAwait(false);

                return;
            }

            _ = await RunAsync(
                token => client.CopyObjectAsync(sourceBucket, sourceName, destinationBucket, destinationName, cancellationToken: token),
                cancellationToken).ConfigureAwait(false);

            try
            {
                await DeleteObjectAsync(client, sourceBucket, sourceName, cancellationToken).ConfigureAwait(false);
            }
            catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
            {
                // A retried delete can find that its first attempt already removed the source.
            }
        }
        catch (GoogleApiException ex)
        {
            throw GcsErrors.Translate(ex, sourceBucket, sourceName, "move");
        }
    }

    private async Task DeleteObjectAsync(StorageClient client, string bucket, string objectName, CancellationToken cancellationToken) =>
        _ = await RunAsync(
            async token =>
            {
                await client.DeleteObjectAsync(bucket, objectName, DeleteObjectOnce, token).ConfigureAwait(false);
                return true;
            },
            cancellationToken).ConfigureAwait(false);


    private static (string bucket, string objectName) GetBucketAndObjectName(StorageUri uri, bool requireObjectName)
    {
        var bucket = uri.Host;

        if (string.IsNullOrEmpty(bucket))
            throw new ArgumentException("GCS URI must specify a bucket name in the host component.", nameof(uri));

        var objectName = uri.Path.TrimStart('/');

        if (requireObjectName && string.IsNullOrWhiteSpace(objectName))
            throw new ArgumentException("GCS URI must specify an object name in the path component.", nameof(uri));

        return (bucket, objectName);
    }

    private async IAsyncEnumerable<StorageItem> ListAsyncCore(
        StorageUri prefix,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (bucket, prefixPath) = GetBucketAndObjectName(prefix, false);
        var client = await _clientFactory.GetClientAsync(prefix, cancellationToken).ConfigureAwait(false);
        var request = client.Service.Objects.List(bucket);
        request.Prefix = prefixPath;

        if (!recursive)
            request.Delimiter = "/";

        string? pageToken = null;

        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            request.PageToken = pageToken;

            Objects response;

            try
            {
                response = await RunAsync(
                    token => request.ExecuteAsync(token),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
            {
                yield break;
            }
            catch (GoogleApiException ex)
            {
                throw GcsErrors.Translate(ex, bucket, prefixPath, "list");
            }

            foreach (var obj in response.Items ?? [])
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Names ending in '/' are folder placeholder objects, not files.
                if (obj is null || string.IsNullOrWhiteSpace(obj.Name) || obj.Name.EndsWith('/'))
                    continue;

                var itemUri = prefix.WithPath("/" + obj.Name);

                yield return new StorageItem
                {
                    Uri = itemUri,
                    Size = obj.Size is { } size ? (long)size : null,
                    LastModified = obj.UpdatedDateTimeOffset,
                    IsDirectory = false,
                };
            }

            if (!recursive)
            {
                foreach (var commonPrefix in response.Prefixes ?? [])
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (string.IsNullOrWhiteSpace(commonPrefix))
                        continue;

                    yield return new StorageItem
                    {
                        Uri = prefix.WithPath("/" + commonPrefix),
                        IsDirectory = true,
                    };
                }
            }

            pageToken = response.NextPageToken;
        } while (!string.IsNullOrWhiteSpace(pageToken));
    }

    private ValueTask<T> RunAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)
    {
        return _resilience.RunAsync(
            static (operation, token) => GcsRetryAfter.CaptureAsync(operation, token),
            operation,
            cancellationToken);
    }

    /// <summary>
    ///     Wrapper stream for GCS downloads that ensures proper disposal.
    /// </summary>
    private sealed class GcsReadStream : Stream
    {
        private readonly Stream _inner;
        private bool _disposed;

        public GcsReadStream(Stream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => false;
        public override long Length => _inner.Length;

        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public override void Flush()
        {
            // No-op for read-only stream
        }

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

        public override int Read(Span<byte> buffer) => _inner.Read(buffer);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            _inner.ReadAsync(buffer, offset, count, cancellationToken);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            _inner.ReadAsync(buffer, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            throw new NotSupportedException();
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => throw new NotSupportedException();

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                _disposed = true;
                _inner.Dispose();
            }

            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                _disposed = true;
                await _inner.DisposeAsync().ConfigureAwait(false);
            }

            await base.DisposeAsync().ConfigureAwait(false);
        }
    }
}

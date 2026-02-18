using System.Net;
using System.Runtime.CompilerServices;
using Google.Apis.Storage.v1.Data;
using Google.Cloud.Storage.V1;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Gcs;

/// <summary>
///     Storage provider for Google Cloud Storage that implements the <see cref="IStorageProvider" /> interface.
///     Handles "gs" scheme URIs and supports reading, writing, listing, and metadata operations.
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
public sealed class GcsStorageProvider : IStorageProvider, IStorageProviderMetadataProvider
{
    private readonly GcsClientFactory _clientFactory;
    private readonly GcsStorageProviderOptions _options;
    private readonly GcsRetryPolicy _retryPolicy;

    /// <summary>
    ///     Initializes a new instance of the <see cref="GcsStorageProvider" /> class.
    /// </summary>
    /// <param name="clientFactory">The GCS client factory.</param>
    /// <param name="options">The GCS storage provider options.</param>
    public GcsStorageProvider(GcsClientFactory clientFactory, GcsStorageProviderOptions options)
    {
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _retryPolicy = new GcsRetryPolicy(_options.RetrySettings);
    }

    /// <summary>
    ///     Gets the storage scheme supported by this provider.
    /// </summary>
    public StorageScheme Scheme => StorageScheme.Gcs;

    /// <summary>
    ///     Determines whether this provider can handle the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI to check.</param>
    /// <returns>True if the URI scheme matches "gs"; otherwise false.</returns>
    public bool CanHandle(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return Scheme.Equals(uri.Scheme);
    }

    /// <summary>
    ///     Opens a readable stream for the specified GCS object.
    /// </summary>
    /// <param name="uri">The storage URI pointing to the GCS object.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a readable stream for the GCS object.</returns>
    public async Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (bucket, objectName) = GetBucketAndObjectName(uri, requireObjectName: true);
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

            await _retryPolicy.ExecuteAsync(
                token => client.DownloadObjectAsync(bucket, objectName, tempFileStream, cancellationToken: token),
                cancellationToken).ConfigureAwait(false);
            tempFileStream.Position = 0;
            return new GcsReadStream(tempFileStream);
        }
        catch (Google.GoogleApiException ex)
        {
            if (tempFileStream is not null)
            {
                await tempFileStream.DisposeAsync().ConfigureAwait(false);
            }

            throw TranslateGcsException(ex, bucket, objectName, "read");
        }
        catch
        {
            if (tempFileStream is not null)
            {
                await tempFileStream.DisposeAsync().ConfigureAwait(false);
            }

            throw;
        }
    }

    /// <summary>
    ///     Opens a writable stream for the specified GCS object.
    /// </summary>
    /// <param name="uri">The storage URI pointing to the GCS object.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a writable stream for the GCS object.</returns>
    public async Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (bucket, objectName) = GetBucketAndObjectName(uri, requireObjectName: true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        var contentType = uri.Parameters.TryGetValue("contentType", out var ct) && !string.IsNullOrEmpty(ct)
            ? ct
            : null;

        return new GcsWriteStream(
            client,
            bucket,
            objectName,
            contentType,
            _options.UploadChunkSizeBytes,
            _retryPolicy,
            cancellationToken);
    }

    /// <summary>
    ///     Checks whether a GCS object exists at the specified URI.
    /// </summary>
    /// <param name="uri">The storage URI to check.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>True if the GCS object exists; otherwise false.</returns>
    public async Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (bucket, objectName) = GetBucketAndObjectName(uri, requireObjectName: true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            _ = await _retryPolicy.ExecuteAsync(
                token => client.GetObjectAsync(bucket, objectName, cancellationToken: token),
                cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
        catch (Google.GoogleApiException ex)
        {
            throw TranslateGcsException(ex, bucket, objectName, "exists");
        }
    }

    /// <summary>
    ///     Deletes the GCS object at the specified URI.
    ///     Delete operations are not supported by this provider.
    /// </summary>
    /// <param name="uri">The storage URI of the GCS object to delete.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <exception cref="NotSupportedException">Delete operation is not supported.</exception>
    public Task DeleteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Delete operation is not supported by the GCS storage provider.");
    }

    /// <summary>
    ///     Lists GCS objects at the specified prefix.
    /// </summary>
    /// <param name="prefix">The URI prefix to list.</param>
    /// <param name="recursive">If true, recursively lists all objects; if false, lists only objects in the specified prefix.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>An async enumerable of <see cref="StorageItem" /> representing GCS objects.</returns>
    public IAsyncEnumerable<StorageItem> ListAsync(
        StorageUri prefix,
        bool recursive = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return ListAsyncCore(prefix, recursive, cancellationToken);
    }

    /// <summary>
    ///     Retrieves metadata for the GCS object at the specified URI.
    /// </summary>
    /// <param name="uri">The storage URI pointing to the GCS object.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing <see cref="StorageMetadata" /> if the object exists; otherwise null.</returns>
    public async Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (bucket, objectName) = GetBucketAndObjectName(uri, requireObjectName: true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            var obj = await _retryPolicy.ExecuteAsync(
                token => client.GetObjectAsync(bucket, objectName, cancellationToken: token),
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

            var metadata = new StorageMetadata
            {
                Size = (long)(obj.Size ?? 0),
                LastModified = NormalizeDateTimeOffset(obj.UpdatedDateTimeOffset),
                ContentType = obj.ContentType,
                ETag = obj.ETag,
                CustomMetadata = customMetadata,
                IsDirectory = false,
            };

            return metadata;
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
        catch (Google.GoogleApiException ex)
        {
            throw TranslateGcsException(ex, bucket, objectName, "metadata");
        }
    }

    /// <summary>
    ///     Gets metadata describing this storage provider's capabilities.
    /// </summary>
    /// <returns>A <see cref="StorageProviderMetadata" /> object containing information about the provider's supported features.</returns>
    public StorageProviderMetadata GetMetadata()
    {
        return new StorageProviderMetadata
        {
            Name = "Google Cloud Storage",
            SupportedSchemes = ["gs"],
            SupportsRead = true,
            SupportsWrite = true,
            SupportsDelete = false,
            SupportsListing = true,
            SupportsMetadata = true,
            SupportsHierarchy = false, // GCS is flat
            Capabilities = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                ["uploadChunkSizeBytes"] = _options.UploadChunkSizeBytes,
                ["uploadBufferThresholdBytes"] = _options.UploadBufferThresholdBytes,
                ["supportsServiceUrl"] = true,
                ["supportsAccessToken"] = true,
                ["supportsCredentialsPath"] = true,
            },
        };
    }

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
        var (bucket, prefixPath) = GetBucketAndObjectName(prefix, requireObjectName: false);
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
                response = await _retryPolicy.ExecuteAsync(
                    token => request.ExecuteAsync(token),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
            {
                yield break;
            }
            catch (Google.GoogleApiException ex)
            {
                throw TranslateGcsException(ex, bucket, prefixPath, "list");
            }

            foreach (var obj in response.Items ?? [])
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (obj is null || string.IsNullOrWhiteSpace(obj.Name))
                    continue;

                var objectKey = obj.Name;
                var itemUri = StorageUri.Parse($"gs://{bucket}/{objectKey}");
                var size = (long)(obj.Size ?? 0);

                yield return new StorageItem
                {
                    Uri = itemUri,
                    Size = size,
                    LastModified = NormalizeDateTimeOffset(obj.UpdatedDateTimeOffset),
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

                    var directoryPath = commonPrefix.TrimEnd('/');
                    var itemUri = StorageUri.Parse($"gs://{bucket}/{directoryPath}");

                    yield return new StorageItem
                    {
                        Uri = itemUri,
                        Size = 0,
                        LastModified = DateTimeOffset.UtcNow,
                        IsDirectory = true,
                    };
                }
            }

            pageToken = response.NextPageToken;
        } while (!string.IsNullOrWhiteSpace(pageToken));
    }

    private static Exception TranslateGcsException(
        Google.GoogleApiException ex,
        string bucket,
        string objectName,
        string operation)
    {
        return ex.HttpStatusCode switch
        {
            HttpStatusCode.Unauthorized
                => new UnauthorizedAccessException(
                    $"Access denied to GCS bucket '{bucket}' and object '{objectName}'. {ex.Message}", ex),
            HttpStatusCode.Forbidden
                => new UnauthorizedAccessException(
                    $"Permission denied for GCS bucket '{bucket}' and object '{objectName}'. {ex.Message}", ex),
            HttpStatusCode.NotFound
                => new FileNotFoundException(
                    $"GCS bucket '{bucket}' or object '{objectName}' not found.", ex),
            HttpStatusCode.BadRequest
                => new ArgumentException(
                    $"Invalid request for GCS bucket '{bucket}' and object '{objectName}'. {ex.Message}", ex),
            HttpStatusCode.Conflict
                => new IOException(
                    $"Conflict occurred for GCS bucket '{bucket}' and object '{objectName}'. {ex.Message}", ex),
            _
                => new GcsStorageException(
                    $"Failed to {operation} on GCS bucket '{bucket}' and object '{objectName}'. {ex.Message}",
                    bucket,
                    objectName,
                    operation,
                    ex),
        };
    }

    private static DateTimeOffset NormalizeDateTimeOffset(DateTimeOffset? value)
    {
        return value ?? DateTimeOffset.UtcNow;
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

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _inner.Read(buffer, offset, count);
        }

        public override int Read(Span<byte> buffer)
        {
            return _inner.Read(buffer);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return _inner.ReadAsync(buffer, offset, count, cancellationToken);
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            return _inner.ReadAsync(buffer, cancellationToken);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _inner.Seek(offset, origin);
        }

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

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

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

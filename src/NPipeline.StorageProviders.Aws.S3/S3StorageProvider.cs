using System.Runtime.CompilerServices;
using Amazon.S3;
using Amazon.S3.Model;
using NPipeline.Connectors;
using NPipeline.Connectors.Abstractions;

namespace NPipeline.StorageProviders.Aws.S3;

/// <summary>
/// Storage provider for AWS S3 that implements the <see cref="IStorageProvider"/> interface.
/// Handles "s3" scheme URIs and supports reading, writing, listing, and metadata operations.
/// </summary>
/// <remarks>
/// - Async-first API design
/// - Stream-based I/O for scalability
/// - Proper error handling and exception translation
/// - Cancellation token support throughout
/// - Thread-safe implementation
/// - Consistent with existing FileSystemStorageProvider patterns
/// </remarks>
public sealed class S3StorageProvider : IStorageProvider, IStorageProviderMetadataProvider
{
    private readonly S3ClientFactory _clientFactory;
    private readonly S3StorageProviderOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="S3StorageProvider"/> class.
    /// </summary>
    /// <param name="clientFactory">The S3 client factory.</param>
    /// <param name="options">The S3 storage provider options.</param>
    public S3StorageProvider(S3ClientFactory clientFactory, S3StorageProviderOptions options)
    {
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Gets the storage scheme supported by this provider.
    /// </summary>
    public StorageScheme Scheme => StorageScheme.S3;

    /// <summary>
    /// Determines whether this provider can handle the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI to check.</param>
    /// <returns>True if the URI scheme matches "s3"; otherwise false.</returns>
    public bool CanHandle(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return Scheme.Equals(uri.Scheme);
    }

    /// <summary>
    /// Opens a readable stream for the specified S3 object.
    /// </summary>
    /// <param name="uri">The storage URI pointing to the S3 object.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a readable stream for the S3 object.</returns>
    public async Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (bucket, key) = GetBucketAndKey(uri);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            var request = new GetObjectRequest
            {
                BucketName = bucket,
                Key = key
            };

            var response = await client.GetObjectAsync(request, cancellationToken).ConfigureAwait(false);
            return new S3ResponseStream(response);
        }
        catch (AmazonS3Exception ex)
        {
            throw TranslateS3Exception(ex, bucket, key);
        }
    }

    /// <summary>
    /// Opens a writable stream for the specified S3 object.
    /// </summary>
    /// <param name="uri">The storage URI pointing to the S3 object.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a writable stream for the S3 object.</returns>
    public async Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (bucket, key) = GetBucketAndKey(uri);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        var contentType = uri.Parameters.TryGetValue("contentType", out var ct) && !string.IsNullOrEmpty(ct)
            ? ct
            : null;

        return new S3WriteStream(client, bucket, key, contentType, _options.MultipartUploadThresholdBytes);
    }

    /// <summary>
    /// Checks whether an S3 object exists at the specified URI.
    /// </summary>
    /// <param name="uri">The storage URI to check.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>True if the S3 object exists; otherwise false.</returns>
    public async Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (bucket, key) = GetBucketAndKey(uri);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            var request = new GetObjectMetadataRequest
            {
                BucketName = bucket,
                Key = key
            };

            _ = await client.GetObjectMetadataAsync(request, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
        catch (AmazonS3Exception ex)
        {
            throw TranslateS3Exception(ex, bucket, key);
        }
    }

    /// <summary>
    /// Deletes the S3 object at the specified URI.
    /// Delete operations are not supported by this provider.
    /// </summary>
    /// <param name="uri">The storage URI of the S3 object to delete.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <exception cref="NotSupportedException">Delete operation is not supported.</exception>
    public Task DeleteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Delete operation is not supported by the S3 storage provider.");
    }

    /// <summary>
    /// Lists S3 objects at the specified prefix.
    /// </summary>
    /// <param name="prefix">The URI prefix to list.</param>
    /// <param name="recursive">If true, recursively lists all objects; if false, lists only objects in the specified prefix.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>An async enumerable of <see cref="StorageItem"/> representing S3 objects.</returns>
    public IAsyncEnumerable<StorageItem> ListAsync(
        StorageUri prefix,
        bool recursive = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return ListAsyncCore(prefix, recursive, cancellationToken);
    }

    /// <summary>
    /// Retrieves metadata for the S3 object at the specified URI.
    /// </summary>
    /// <param name="uri">The storage URI pointing to the S3 object.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing <see cref="StorageMetadata"/> if the object exists; otherwise null.</returns>
    public async Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var (bucket, key) = GetBucketAndKey(uri);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            var request = new GetObjectMetadataRequest
            {
                BucketName = bucket,
                Key = key
            };

            var response = await client.GetObjectMetadataAsync(request, cancellationToken).ConfigureAwait(false);

            var customMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Add S3-specific metadata
            foreach (var metadataKey in response.Metadata.Keys)
            {
                customMetadata[metadataKey] = response.Metadata[metadataKey];
            }

            var metadata = new StorageMetadata
            {
                Size = response.ContentLength,
                LastModified = NormalizeDateTime(response.LastModified),
                ContentType = response.Headers.ContentType,
                ETag = response.ETag,
                CustomMetadata = customMetadata,
                IsDirectory = false
            };

            return metadata;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
        catch (AmazonS3Exception ex)
        {
            throw TranslateS3Exception(ex, bucket, key);
        }
    }

    /// <summary>
    /// Gets metadata describing this storage provider's capabilities.
    /// </summary>
    /// <returns>A <see cref="StorageProviderMetadata"/> object containing information about the provider's supported features.</returns>
    public StorageProviderMetadata GetMetadata()
    {
        return new StorageProviderMetadata
        {
            Name = "AWS S3",
            SupportedSchemes = ["s3"],
            SupportsRead = true,
            SupportsWrite = true,
            SupportsDelete = false,
            SupportsListing = true,
            SupportsMetadata = true,
            SupportsHierarchy = false, // S3 is flat
            Capabilities = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                ["multipartUploadThresholdBytes"] = _options.MultipartUploadThresholdBytes,
                ["supportsPathStyle"] = true,
                ["supportsServiceUrl"] = true
            }
        };
    }

    private static (string bucket, string key) GetBucketAndKey(StorageUri uri)
    {
        var bucket = uri.Host;
        if (string.IsNullOrEmpty(bucket))
        {
            throw new ArgumentException("S3 URI must specify a bucket name in the host component.", nameof(uri));
        }

        var key = uri.Path.TrimStart('/');
        return (bucket, key);
    }

    private async IAsyncEnumerable<StorageItem> ListAsyncCore(
        StorageUri prefix,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (bucket, key) = GetBucketAndKey(prefix);
        var client = await _clientFactory.GetClientAsync(prefix, cancellationToken).ConfigureAwait(false);

        var request = new ListObjectsV2Request
        {
            BucketName = bucket,
            Prefix = key,
            Delimiter = recursive ? string.Empty : "/"
        };

        string? continuationToken = null;

        do
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (continuationToken != null)
            {
                request.ContinuationToken = continuationToken;
            }

            ListObjectsV2Response response;
            try
            {
                response = await client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                // Bucket doesn't exist, return empty
                yield break;
            }
            catch (AmazonS3Exception ex)
            {
                throw TranslateS3Exception(ex, bucket, key);
            }

            // Yield objects
            foreach (var s3Object in response.S3Objects)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var objectKey = s3Object.Key;
                var itemUri = StorageUri.Parse($"s3://{bucket}/{objectKey}");

                yield return new StorageItem
                {
                    Uri = itemUri,
                    Size = s3Object.Size,
                    LastModified = NormalizeDateTime(s3Object.LastModified),
                    IsDirectory = false
                };
            }

            // Yield common prefixes (directories) for non-recursive listing
            if (!recursive)
            {
                foreach (var commonPrefix in response.CommonPrefixes)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var prefixKey = commonPrefix.TrimEnd('/');
                    var itemUri = StorageUri.Parse($"s3://{bucket}/{prefixKey}");

                    yield return new StorageItem
                    {
                        Uri = itemUri,
                        Size = 0,
                        LastModified = DateTimeOffset.UtcNow,
                        IsDirectory = true
                    };
                }
            }

            continuationToken = response.NextContinuationToken;
        }
        while (!string.IsNullOrEmpty(continuationToken));
    }

    private static Exception TranslateS3Exception(AmazonS3Exception ex, string bucket, string key)
    {
        return ex.ErrorCode switch
        {
            "AccessDenied" or "InvalidAccessKeyId" or "SignatureDoesNotMatch"
                => new UnauthorizedAccessException(
                    $"Access denied to S3 bucket '{bucket}' and key '{key}'. {ex.Message}", ex),
            "InvalidBucketName" or "InvalidKey"
                => new ArgumentException(
                    $"Invalid S3 bucket '{bucket}' or key '{key}'. {ex.Message}", ex),
            "NoSuchBucket" or "NotFound"
                => new FileNotFoundException(
                    $"S3 bucket '{bucket}' or key '{key}' not found.", ex),
            _
                => new IOException(
                    $"Failed to access S3 bucket '{bucket}' and key '{key}'. {ex.Message}", ex)
        };
    }

    private static DateTimeOffset NormalizeDateTime(DateTime? value)
    {
        var actual = value ?? DateTime.UtcNow;
        var utc = actual.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(actual, DateTimeKind.Utc)
            : actual.ToUniversalTime();

        return new DateTimeOffset(utc);
    }

    private sealed class S3ResponseStream : Stream
    {
        private readonly GetObjectResponse _response;
        private readonly Stream _inner;

        public S3ResponseStream(GetObjectResponse response)
        {
            _response = response ?? throw new ArgumentNullException(nameof(response));
            _inner = response.ResponseStream ?? throw new InvalidOperationException("S3 response stream was null.");
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => _inner.Length;

        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public override void Flush() => _inner.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

        public override int Read(Span<byte> buffer) => _inner.Read(buffer);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => _inner.ReadAsync(buffer, offset, count, cancellationToken);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => _inner.ReadAsync(buffer, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

        public override void SetLength(long value) => _inner.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

        public override void Write(ReadOnlySpan<byte> buffer) => _inner.Write(buffer);

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => _inner.WriteAsync(buffer, offset, count, cancellationToken);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => _inner.WriteAsync(buffer, cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _response.Dispose();
            }

            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await _inner.DisposeAsync().ConfigureAwait(false);
            _response.Dispose();
            await base.DisposeAsync().ConfigureAwait(false);
        }
    }
}

using System.Net;
using System.Runtime.CompilerServices;
using Amazon.S3;
using Amazon.S3.Model;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.S3;

/// <summary>
///     Core S3 storage provider implementation that handles common S3 operations.
///     Subclasses provide the name, the schemes and the environment-specific client factory.
/// </summary>
/// <remarks>
///     Declares <see cref="StorageCapabilities.Read" />, <see cref="StorageCapabilities.Write" />,
///     <see cref="StorageCapabilities.List" />, <see cref="StorageCapabilities.Delete" /> and
///     <see cref="StorageCapabilities.Move" />. A move is a copy followed by a delete, so it is not atomic.
/// </remarks>
public abstract class S3CoreStorageProvider : StorageProvider, IAsyncDisposable
{
    /// <summary>The largest object <c>CopyObject</c> can copy in one request (5 GiB).</summary>
    internal const long MaxSingleCopyBytes = 5L * 1024 * 1024 * 1024;

    private const long CopyPartBytes = 512L * 1024 * 1024;

    private readonly S3ClientFactoryBase _clientFactory;

    /// <summary>
    ///     Initializes a new instance of the <see cref="S3CoreStorageProvider" /> class.
    /// </summary>
    /// <param name="clientFactory">The S3 client factory.</param>
    /// <param name="options">The S3 storage provider options.</param>
    protected S3CoreStorageProvider(S3ClientFactoryBase clientFactory, S3CoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(clientFactory);
        ArgumentNullException.ThrowIfNull(options);

        _clientFactory = clientFactory;
        Options = options;
    }

    /// <summary>
    ///     Gets the options for this provider.
    /// </summary>
    protected S3CoreOptions Options { get; }

    /// <inheritdoc />
    public override StorageCapabilities Capabilities =>
        StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete | StorageCapabilities.Move;

    /// <summary>
    ///     Disposes the clients the provider's client factory created.
    /// </summary>
    public virtual ValueTask DisposeAsync()
    {
        _clientFactory.Dispose();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    protected override async Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (bucket, key) = GetBucketAndKey(uri);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            var request = new GetObjectRequest
            {
                BucketName = bucket,
                Key = key,
            };

            var response = await client.GetObjectAsync(request, cancellationToken).ConfigureAwait(false);
            return new S3ResponseStream(response);
        }
        catch (AmazonS3Exception ex)
        {
            throw S3Errors.Translate(ex, bucket, key);
        }
    }

    /// <inheritdoc />
    protected override async Task<StorageWriteStream> OpenWriteCoreAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken)
    {
        var (bucket, key) = GetBucketAndKey(uri);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        var contentType = !string.IsNullOrEmpty(options?.ContentType)
            ? options.ContentType
            : uri.Parameters.TryGetValue("contentType", out var ct) && !string.IsNullOrEmpty(ct)
                ? ct
                : null;

        return new S3WriteStream(client, bucket, key, contentType, Options.MultipartUploadThresholdBytes, ifMatch: options?.IfMatch, overwrite: options?.Overwrite ?? true);
    }

    /// <inheritdoc />
    protected override async Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (bucket, key) = GetBucketAndKey(uri);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            var request = new GetObjectMetadataRequest
            {
                BucketName = bucket,
                Key = key,
            };

            var response = await client.GetObjectMetadataAsync(request, cancellationToken).ConfigureAwait(false);

            var customMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var metadataKey in response.Metadata.Keys)
            {
                customMetadata[metadataKey] = response.Metadata[metadataKey];
            }

            return new StorageMetadata
            {
                Size = response.ContentLength,
                LastModified = ToOffset(response.LastModified),
                ContentType = response.Headers.ContentType,
                ETag = response.ETag,
                CustomMetadata = customMetadata,
                IsDirectory = false,
            };
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
        catch (AmazonS3Exception ex)
        {
            throw S3Errors.Translate(ex, bucket, key);
        }
    }

    /// <inheritdoc />
    protected override async Task DeleteCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (bucket, key) = GetBucketAndKey(uri);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            // DeleteObject succeeds for a missing key; a missing bucket is also treated as "already gone".
            _ = await client.DeleteObjectAsync(new DeleteObjectRequest { BucketName = bucket, Key = key }, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // Already gone.
        }
        catch (AmazonS3Exception ex)
        {
            throw S3Errors.Translate(ex, bucket, key);
        }
    }

    /// <inheritdoc />
    protected override async Task MoveCoreAsync(StorageUri source, StorageUri destination, CancellationToken cancellationToken)
    {
        var (sourceBucket, sourceKey) = GetBucketAndKey(source);
        var (destinationBucket, destinationKey) = GetBucketAndKey(destination);
        var client = await _clientFactory.GetClientAsync(source, cancellationToken).ConfigureAwait(false);

        try
        {
            GetObjectMetadataResponse head;

            try
            {
                head = await client.GetObjectMetadataAsync(
                    new GetObjectMetadataRequest { BucketName = sourceBucket, Key = sourceKey }, cancellationToken).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                throw new FileNotFoundException($"S3 object '{sourceBucket}/{sourceKey}' was not found.", ex);
            }

            // Copying an object onto itself would fail, and the delete that follows would destroy it.
            if (string.Equals(sourceBucket, destinationBucket, StringComparison.Ordinal) && string.Equals(sourceKey, destinationKey, StringComparison.Ordinal))
                return;

            if (head.ContentLength <= MaxSingleCopyBytes)
            {
                _ = await client.CopyObjectAsync(
                    new CopyObjectRequest
                    {
                        SourceBucket = sourceBucket,
                        SourceKey = sourceKey,
                        DestinationBucket = destinationBucket,
                        DestinationKey = destinationKey,
                    }, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await CopyMultipartAsync(client, head, sourceBucket, sourceKey, destinationBucket, destinationKey, cancellationToken).ConfigureAwait(false);
            }

            _ = await client.DeleteObjectAsync(new DeleteObjectRequest { BucketName = sourceBucket, Key = sourceKey }, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex)
        {
            throw S3Errors.Translate(ex, sourceBucket, sourceKey);
        }
    }

    /// <inheritdoc />
    protected override async IAsyncEnumerable<StorageItem> ListCoreAsync(
        StorageUri directory,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (bucket, prefix) = GetBucketAndKey(directory);
        var client = await _clientFactory.GetClientAsync(directory, cancellationToken).ConfigureAwait(false);

        var request = new ListObjectsV2Request
        {
            BucketName = bucket,
            Prefix = prefix,
            Delimiter = recursive
                ? null
                : "/",
        };

        do
        {
            cancellationToken.ThrowIfCancellationRequested();

            ListObjectsV2Response response;

            try
            {
                response = await client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // The bucket does not exist.
                yield break;
            }
            catch (AmazonS3Exception ex)
            {
                throw S3Errors.Translate(ex, bucket, prefix);
            }

            foreach (var s3Object in response.S3Objects ?? [])
            {
                // Zero-byte "folder marker" objects (keys ending in '/') are not files.
                if (s3Object.Key.EndsWith('/'))
                    continue;

                yield return new StorageItem
                {
                    Uri = directory.WithPath("/" + s3Object.Key),
                    Size = s3Object.Size,
                    LastModified = ToOffset(s3Object.LastModified),
                    IsDirectory = false,
                };
            }

            if (!recursive)
            {
                foreach (var commonPrefix in response.CommonPrefixes ?? [])
                {
                    yield return new StorageItem
                    {
                        Uri = directory.WithPath("/" + commonPrefix),
                        IsDirectory = true,
                    };
                }
            }

            request.ContinuationToken = response.NextContinuationToken;
        } while (!string.IsNullOrEmpty(request.ContinuationToken));
    }

    /// <summary>
    ///     Extracts the bucket and key from a storage URI.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>A tuple containing the bucket name and object key.</returns>
    protected static (string bucket, string key) GetBucketAndKey(StorageUri uri)
    {
        var bucket = uri.Host;

        if (string.IsNullOrEmpty(bucket))
            throw new ArgumentException("S3 URI must specify a bucket name in the host component.", nameof(uri));

        var key = uri.Path.TrimStart('/');
        return (bucket, key);
    }

    private static DateTimeOffset? ToOffset(DateTime? value)
    {
        if (value is not { } actual)
            return null;

        var utc = actual.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(actual, DateTimeKind.Utc)
            : actual.ToUniversalTime();

        return new DateTimeOffset(utc);
    }

    private static async Task CopyMultipartAsync(
        IAmazonS3 client,
        GetObjectMetadataResponse head,
        string sourceBucket,
        string sourceKey,
        string destinationBucket,
        string destinationKey,
        CancellationToken cancellationToken)
    {
        var initiate = new InitiateMultipartUploadRequest
        {
            BucketName = destinationBucket,
            Key = destinationKey,
            ContentType = head.Headers.ContentType,
        };

        foreach (var metadataKey in head.Metadata.Keys)
        {
            initiate.Metadata[metadataKey] = head.Metadata[metadataKey];
        }

        var upload = await client.InitiateMultipartUploadAsync(initiate, cancellationToken).ConfigureAwait(false);

        try
        {
            var parts = new List<PartETag>();
            var partNumber = 1;

            for (long offset = 0; offset < head.ContentLength; offset += CopyPartBytes, partNumber++)
            {
                var last = Math.Min(offset + CopyPartBytes, head.ContentLength) - 1;

                var response = await client.CopyPartAsync(
                    new CopyPartRequest
                    {
                        SourceBucket = sourceBucket,
                        SourceKey = sourceKey,
                        DestinationBucket = destinationBucket,
                        DestinationKey = destinationKey,
                        UploadId = upload.UploadId,
                        PartNumber = partNumber,
                        FirstByte = offset,
                        LastByte = last,
                    }, cancellationToken).ConfigureAwait(false);

                parts.Add(new PartETag(partNumber, response.ETag));
            }

            _ = await client.CompleteMultipartUploadAsync(
                new CompleteMultipartUploadRequest
                {
                    BucketName = destinationBucket,
                    Key = destinationKey,
                    UploadId = upload.UploadId,
                    PartETags = parts,
                }, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            try
            {
                _ = await client.AbortMultipartUploadAsync(
                    new AbortMultipartUploadRequest { BucketName = destinationBucket, Key = destinationKey, UploadId = upload.UploadId },
                    CancellationToken.None).ConfigureAwait(false);
            }
            catch (AmazonS3Exception)
            {
                // Best effort; the original failure is the one to report.
            }

            throw;
        }
    }

    private sealed class S3ResponseStream : Stream
    {
        private readonly Stream _inner;
        private readonly GetObjectResponse _response;

        public S3ResponseStream(GetObjectResponse response)
        {
            _response = response ?? throw new ArgumentNullException(nameof(response));
            _inner = response.ResponseStream ?? throw new InvalidOperationException("S3 response stream was null.");
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => _response.ContentLength;

        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public override void Flush()
        {
            _inner.Flush();
        }

        public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

        public override int Read(Span<byte> buffer) => _inner.Read(buffer);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            _inner.ReadAsync(buffer, offset, count, cancellationToken);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            _inner.ReadAsync(buffer, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

        public override void SetLength(long value)
        {
            _inner.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _inner.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            _inner.Write(buffer);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            _inner.WriteAsync(buffer, offset, count, cancellationToken);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
            _inner.WriteAsync(buffer, cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _response.Dispose();

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

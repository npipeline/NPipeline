using Amazon.S3;
using Amazon.S3.Model;

namespace NPipeline.StorageProviders.S3;

/// <summary>
///     A stream that buffers writes and uploads to S3 on disposal or flush.
///     Supports multipart upload for files larger than the configured threshold.
/// </summary>
public sealed class S3WriteStream : Stream
{
    private const int DefaultPartSize = 8 * 1024 * 1024; // 8 MB parts
    private const int MaxConcurrentUploads = 4;

    private readonly string _bucket;
    private readonly string? _contentType;
    private readonly string _key;
    private readonly long _multipartUploadThreshold;
    private readonly int _partSize;
    private readonly object _readLock = new();
    private readonly IAmazonS3 _s3Client;
    private readonly string _tempFilePath;
    private bool _disposed;
    private FileStream? _tempFileStream;
    private bool _uploaded;

    /// <summary>
    ///     Initializes a new instance of the <see cref="S3WriteStream" /> class.
    /// </summary>
    /// <param name="s3Client">The Amazon S3 client.</param>
    /// <param name="bucket">The S3 bucket name.</param>
    /// <param name="key">The S3 object key.</param>
    /// <param name="contentType">Optional content type for the upload.</param>
    /// <param name="multipartUploadThreshold">Threshold in bytes for using multipart upload. Default is 64 MB.</param>
    /// <param name="partSize">Size of each part for multipart upload. Default is 8 MB.</param>
    public S3WriteStream(
        IAmazonS3 s3Client,
        string bucket,
        string key,
        string? contentType = null,
        long multipartUploadThreshold = 64 * 1024 * 1024,
        int partSize = DefaultPartSize)
    {
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _key = key ?? throw new ArgumentNullException(nameof(key));
        _contentType = contentType;
        _multipartUploadThreshold = multipartUploadThreshold;
        _partSize = Math.Max(partSize, 5 * 1024 * 1024); // Minimum 5 MB per S3 requirements
        _tempFilePath = Path.Combine(Path.GetTempPath(), $"s3-upload-{Guid.NewGuid()}.tmp");

        _tempFileStream = new FileStream(
            _tempFilePath,
            FileMode.Create,
            FileAccess.ReadWrite,
            FileShare.None,
            81920,
            FileOptions.DeleteOnClose | FileOptions.Asynchronous);
    }

    /// <inheritdoc />
    public override bool CanRead => false;

    /// <inheritdoc />
    public override bool CanSeek => false;

    /// <inheritdoc />
    public override bool CanWrite => true;

    /// <inheritdoc />
    public override long Length
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _tempFileStream?.Length ?? 0;
        }
    }

    /// <inheritdoc />
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override void Flush()
    {
        // Flush is a no-op - upload happens on disposal
    }

    /// <inheritdoc />
    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        // Flush is a no-op - upload happens on disposal
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _tempFileStream?.Write(buffer, offset, count);
    }

    /// <inheritdoc />
    public override async Task WriteAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_tempFileStream is null)
            ObjectDisposedException.ThrowIf(true, this);

        await _tempFileStream.WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override async ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_tempFileStream is null)
            ObjectDisposedException.ThrowIf(true, this);

        await _tempFileStream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        _disposed = true;

        if (disposing)
        {
            try
            {
                if (!_uploaded && _tempFileStream is not null)
                {
                    // Flush the temp file stream to ensure all data is written
                    _tempFileStream.Flush();

                    // Reset position to beginning for upload
                    _tempFileStream.Position = 0;

                    // Upload to S3
                    UploadAsync(CancellationToken.None).GetAwaiter().GetResult();
                }
            }
            finally
            {
                _tempFileStream?.Dispose();
                _tempFileStream = null;
            }
        }

        base.Dispose(disposing);
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        try
        {
            if (!_uploaded && _tempFileStream is not null)
            {
                // Flush the temp file stream to ensure all data is written
                await _tempFileStream.FlushAsync(CancellationToken.None).ConfigureAwait(false);

                // Reset position to beginning for upload
                _tempFileStream.Position = 0;

                // Upload to S3
                await UploadAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
        finally
        {
            if (_tempFileStream is not null)
            {
                await _tempFileStream.DisposeAsync().ConfigureAwait(false);
                _tempFileStream = null;
            }
        }

        await base.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     Uploads the buffered data to S3, using multipart upload for large files.
    /// </summary>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    private async Task UploadAsync(CancellationToken cancellationToken)
    {
        if (_tempFileStream is null || _uploaded)
            return;

        _uploaded = true;

        var contentLength = _tempFileStream.Length;

        try
        {
            if (contentLength > 0 && contentLength >= _multipartUploadThreshold)
                await UploadMultipartAsync(contentLength, cancellationToken).ConfigureAwait(false);
            else
                await UploadSingleAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex)
        {
            throw TranslateS3Exception(ex, _bucket, _key);
        }
    }

    /// <summary>
    ///     Uploads the content using a single PutObject request.
    /// </summary>
    private async Task UploadSingleAsync(CancellationToken cancellationToken)
    {
        var request = new PutObjectRequest
        {
            BucketName = _bucket,
            Key = _key,
            InputStream = _tempFileStream,
            AutoCloseStream = false,
            UseChunkEncoding = false,
        };

        if (!string.IsNullOrEmpty(_contentType))
            request.ContentType = _contentType;

        _ = await _s3Client.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Uploads the content using S3 multipart upload.
    /// </summary>
    /// <param name="contentLength">The total length of the content to upload.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    private async Task UploadMultipartAsync(long contentLength, CancellationToken cancellationToken)
    {
        // Initiate multipart upload
        var initiateRequest = new InitiateMultipartUploadRequest
        {
            BucketName = _bucket,
            Key = _key,
            ContentType = _contentType,
        };

        var initiateResponse = await _s3Client.InitiateMultipartUploadAsync(initiateRequest, cancellationToken).ConfigureAwait(false);
        var uploadId = initiateResponse.UploadId;
        var parts = new List<PartETag>();

        try
        {
            // Calculate part boundaries
            var partCount = (int)Math.Ceiling((double)contentLength / _partSize);

            // Upload parts, optionally in parallel
            using var semaphore = new SemaphoreSlim(MaxConcurrentUploads);
            var uploadTasks = new List<Task<PartETag>>();

            for (var partNumber = 1; partNumber <= partCount; partNumber++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var currentPartNumber = partNumber;
                var task = UploadPartAsync(currentPartNumber, contentLength, uploadId, semaphore, cancellationToken);
                uploadTasks.Add(task);

                // If we've reached max concurrent uploads, wait for at least one to complete
                if (uploadTasks.Count >= MaxConcurrentUploads)
                {
                    var completedTask = await Task.WhenAny(uploadTasks).ConfigureAwait(false);
                    _ = uploadTasks.Remove(completedTask);
                    parts.Add(await completedTask.ConfigureAwait(false));
                }
            }

            // Wait for remaining uploads to complete
            foreach (var task in uploadTasks)
            {
                parts.Add(await task.ConfigureAwait(false));
            }

            // Sort parts by part number to ensure correct order
            var orderedParts = parts.OrderBy(p => p.PartNumber).ToList();

            // Complete multipart upload
            var completeRequest = new CompleteMultipartUploadRequest
            {
                BucketName = _bucket,
                Key = _key,
                UploadId = uploadId,
                PartETags = orderedParts,
            };

            _ = await _s3Client.CompleteMultipartUploadAsync(completeRequest, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Abort multipart upload on failure
            try
            {
                var abortRequest = new AbortMultipartUploadRequest
                {
                    BucketName = _bucket,
                    Key = _key,
                    UploadId = uploadId,
                };

                _ = await _s3Client.AbortMultipartUploadAsync(abortRequest, CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Ignore abort failures - the upload will eventually expire
            }

            throw;
        }
    }

    /// <summary>
    ///     Uploads a single part of a multipart upload.
    /// </summary>
    private async Task<PartETag> UploadPartAsync(
        int partNumber,
        long contentLength,
        string uploadId,
        SemaphoreSlim semaphore,
        CancellationToken cancellationToken)
    {
        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            // Calculate offset and size for this part
            var offset = (long)(partNumber - 1) * _partSize;
            var partSize = (int)Math.Min(_partSize, contentLength - offset);

            // Allocate buffer per part to avoid race condition with parallel uploads
            var buffer = new byte[partSize];

            // Read the part data - null check already done in UploadAsync
            ObjectDisposedException.ThrowIf(_tempFileStream is null, typeof(S3WriteStream));

            int bytesRead;

            lock (_readLock)
            {
                _tempFileStream.Position = offset;
                bytesRead = _tempFileStream.Read(buffer, 0, partSize);
            }

            if (bytesRead != partSize)
                throw new IOException($"Expected to read {partSize} bytes for part {partNumber}, but only read {bytesRead} bytes.");

            // Upload the part
            using var partStream = new MemoryStream(buffer, 0, bytesRead, false);

            var uploadRequest = new UploadPartRequest
            {
                BucketName = _bucket,
                Key = _key,
                UploadId = uploadId,
                PartNumber = partNumber,
                PartSize = bytesRead,
                InputStream = partStream,
            };

            var response = await _s3Client.UploadPartAsync(uploadRequest, cancellationToken).ConfigureAwait(false);

            return new PartETag(partNumber, response.ETag);
        }
        finally
        {
            _ = semaphore.Release();
        }
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
                    $"Failed to upload to S3 bucket '{bucket}' and key '{key}'. {ex.Message}", ex),
        };
    }
}

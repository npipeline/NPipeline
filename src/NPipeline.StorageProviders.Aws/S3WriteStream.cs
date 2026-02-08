using Amazon.S3;
using Amazon.S3.Model;

namespace NPipeline.StorageProviders.Aws;

/// <summary>
///     A stream that buffers writes and uploads to S3 on disposal or flush.
/// </summary>
public sealed class S3WriteStream : Stream
{
    private readonly string _bucket;
    private readonly string? _contentType;
    private readonly string _key;
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
    /// <param name="multipartUploadThreshold">Reserved for future multipart upload support.</param>
    public S3WriteStream(
        IAmazonS3 s3Client,
        string bucket,
        string key,
        string? contentType = null,
        long multipartUploadThreshold = 64 * 1024 * 1024)
    {
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _key = key ?? throw new ArgumentNullException(nameof(key));
        _contentType = contentType;
        _ = multipartUploadThreshold; // Reserved for future multipart upload support
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
    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        // Flush is a no-op - upload happens on disposal
        await Task.CompletedTask;
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

        await base.DisposeAsync();
    }

    /// <summary>
    ///     Uploads the buffered data to S3.
    /// </summary>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    private async Task UploadAsync(CancellationToken cancellationToken)
    {
        if (_tempFileStream is null || _uploaded)
            return;

        _uploaded = true;

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

        try
        {
            _ = await _s3Client.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex)
        {
            throw TranslateS3Exception(ex, _bucket, _key);
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

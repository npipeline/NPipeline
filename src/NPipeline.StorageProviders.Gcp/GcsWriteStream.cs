using System.Net;
using System.Runtime.ExceptionServices;
using Google;
using Google.Cloud.Storage.V1;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace NPipeline.StorageProviders.Gcp;

/// <summary>
///     A write-only stream that buffers data to a local temp file and uploads to Google Cloud Storage on disposal.
///     Implements the upload-on-dispose pattern consistent with S3/Azure write streams.
/// </summary>
public sealed class GcsWriteStream : Stream
{
    private static readonly TimeSpan UploadDisposeTimeout = TimeSpan.FromMinutes(5);
    private readonly string _bucket;
    private readonly int _chunkSizeBytes;
    private readonly string? _contentType;
    private readonly CancellationToken _disposeCancellationToken;
    private readonly string _objectName;
    private readonly GcsRetryPolicy _retryPolicy;
    private readonly StorageClient _storageClient;
    private int _disposeState; // 0 = not disposed, 1 = disposing, 2 = disposed
    private FileStream? _tempFileStream;
    private bool _uploaded;

    /// <summary>
    ///     Initializes a new instance of the <see cref="GcsWriteStream" /> class.
    /// </summary>
    /// <param name="storageClient">The Google Cloud Storage client.</param>
    /// <param name="bucket">The GCS bucket name.</param>
    /// <param name="objectName">The GCS object name (key).</param>
    /// <param name="contentType">Optional content type for the upload.</param>
    /// <param name="chunkSizeBytes">Chunk size for resumable uploads. Default is 16 MB.</param>
    /// <param name="disposeCancellationToken">Cancellation token to observe while disposing the stream.</param>
    public GcsWriteStream(
        StorageClient storageClient,
        string bucket,
        string objectName,
        string? contentType = null,
        int chunkSizeBytes = 16 * 1024 * 1024,
        CancellationToken disposeCancellationToken = default)
        : this(storageClient, bucket, objectName, contentType, chunkSizeBytes, new GcsRetryPolicy(null), disposeCancellationToken)
    {
    }

    internal GcsWriteStream(
        StorageClient storageClient,
        string bucket,
        string objectName,
        string? contentType,
        int chunkSizeBytes,
        GcsRetryPolicy retryPolicy,
        CancellationToken disposeCancellationToken = default)
    {
        _storageClient = storageClient ?? throw new ArgumentNullException(nameof(storageClient));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _objectName = objectName ?? throw new ArgumentNullException(nameof(objectName));
        _contentType = contentType;
        _retryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));
        _disposeCancellationToken = disposeCancellationToken;

        if (chunkSizeBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(chunkSizeBytes), "Chunk size must be a positive number of bytes.");

        _chunkSizeBytes = chunkSizeBytes;

        var tempFilePath = Path.Combine(Path.GetTempPath(), $"gcs-upload-{Guid.NewGuid():N}.tmp");

        _tempFileStream = new FileStream(
            tempFilePath,
            FileMode.Create,
            FileAccess.ReadWrite,
            FileShare.None,
            81920,
            FileOptions.DeleteOnClose | FileOptions.Asynchronous);
    }

    private bool IsDisposed => Volatile.Read(ref _disposeState) != 0;

    /// <inheritdoc />
    public override bool CanRead => false;

    /// <inheritdoc />
    public override bool CanSeek => false;

    /// <inheritdoc />
    public override bool CanWrite => !IsDisposed;

    /// <inheritdoc />
    public override long Length
    {
        get
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);
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
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        _tempFileStream?.Write(buffer, offset, count);
    }

    /// <inheritdoc />
    public override async Task WriteAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);

        if (_tempFileStream is null)
            ObjectDisposedException.ThrowIf(true, this);

        await _tempFileStream
            .WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override async ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);

        if (_tempFileStream is null)
            ObjectDisposedException.ThrowIf(true, this);

        await _tempFileStream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (Interlocked.CompareExchange(ref _disposeState, 1, 0) != 0)
            return;

        ExceptionDispatchInfo? capturedException = null;

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

                    // Upload to GCS synchronously
                    using var cts = CreateLinkedUploadCts();
                    UploadAsync(cts.Token).GetAwaiter().GetResult();
                }
            }
            catch (OperationCanceledException)
            {
                // Rethrow cancellation immediately, but still call base.Dispose
                _tempFileStream?.Dispose();
                _tempFileStream = null;
                Interlocked.Exchange(ref _disposeState, 2);
                base.Dispose(disposing);
                throw;
            }
            catch (Exception ex)
            {
                capturedException = ExceptionDispatchInfo.Capture(ex);
            }
            finally
            {
                _tempFileStream?.Dispose();
                _tempFileStream = null;
            }
        }

        Interlocked.Exchange(ref _disposeState, 2);

        base.Dispose(disposing);

        capturedException?.Throw();
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _disposeState, 1, 0) != 0)
            return;

        ExceptionDispatchInfo? capturedException = null;

        try
        {
            if (!_uploaded && _tempFileStream is not null)
            {
                // Flush the temp file stream to ensure all data is written
                await _tempFileStream.FlushAsync(CancellationToken.None).ConfigureAwait(false);

                // Reset position to beginning for upload
                _tempFileStream.Position = 0;

                // Upload to GCS
                using var cts = CreateLinkedUploadCts();
                await UploadAsync(cts.Token).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            capturedException = ExceptionDispatchInfo.Capture(ex);
        }
        finally
        {
            if (_tempFileStream is not null)
            {
                await _tempFileStream.DisposeAsync().ConfigureAwait(false);
                _tempFileStream = null;
            }

            Interlocked.Exchange(ref _disposeState, 2);
        }

        await base.DisposeAsync().ConfigureAwait(false);

        capturedException?.Throw();
    }

    /// <summary>
    ///     Uploads the buffered data to Google Cloud Storage.
    /// </summary>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    private async Task UploadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_tempFileStream is null || _uploaded)
            return;

        try
        {
            // Use resumable upload for proper handling of large files
            var uploadOptions = new UploadObjectOptions
            {
                ChunkSize = _chunkSizeBytes,
            };

            var obj = new Object
            {
                Bucket = _bucket,
                Name = _objectName,
            };

            if (!string.IsNullOrEmpty(_contentType))
                obj.ContentType = _contentType;

            await _retryPolicy.ExecuteAsync(
                token => _storageClient.UploadObjectAsync(
                    obj,
                    _tempFileStream,
                    uploadOptions,
                    token),
                cancellationToken).ConfigureAwait(false);

            _uploaded = true;
        }
        catch (GoogleApiException ex)
        {
            throw TranslateGcsException(ex, _bucket, _objectName, "upload");
        }
    }

    private CancellationTokenSource CreateLinkedUploadCts()
    {
        var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(_disposeCancellationToken);
        linkedSource.CancelAfter(UploadDisposeTimeout);
        return linkedSource;
    }

    /// <summary>
    ///     Translates a Google API exception to an appropriate .NET exception.
    /// </summary>
    private static Exception TranslateGcsException(
        GoogleApiException ex,
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
}

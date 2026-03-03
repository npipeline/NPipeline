using System.Runtime.ExceptionServices;
using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Files.DataLake;

namespace NPipeline.StorageProviders.Adls;

/// <summary>
///     A stream that buffers writes and uploads to ADLS Gen2 on disposal.
/// </summary>
public sealed class AdlsGen2WriteStream : Stream
{
    private static readonly TimeSpan UploadDisposeTimeout = TimeSpan.FromMinutes(5);
    private readonly string _path;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly long _uploadThreshold;
    private readonly string _filesystem;
    private readonly string? _contentType;
    private readonly CancellationToken _disposeCancellationToken;
    private readonly int? _maximumConcurrency;
    private readonly int? _maximumTransferSizeBytes;
    private readonly string _tempFilePath;
    private int _disposeState; // 0 = not disposed, 1 = disposing, 2 = disposed
    private FileStream? _tempFileStream;
    private bool _uploaded;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AdlsGen2WriteStream" /> class.
    /// </summary>
    /// <param name="blobServiceClient">The Azure Blob Service client (used for uploads via the Blob API, which is compatible with Azurite and all ADLS Gen2 configurations).</param>
    /// <param name="filesystem">The ADLS filesystem name (maps to a Blob container).</param>
    /// <param name="path">The ADLS path.</param>
    /// <param name="contentType">Optional content type for the upload.</param>
    /// <param name="uploadThreshold">Threshold in bytes for using upload.</param>
    /// <param name="maximumConcurrency">Maximum concurrent upload requests for large files.</param>
    /// <param name="maximumTransferSizeBytes">Maximum transfer size in bytes for each upload chunk.</param>
    /// <param name="disposeCancellationToken">Cancellation token to observe while disposing the stream.</param>
    public AdlsGen2WriteStream(
        BlobServiceClient blobServiceClient,
        string filesystem,
        string path,
        string? contentType = null,
        long uploadThreshold = 64 * 1024 * 1024,
        int? maximumConcurrency = null,
        int? maximumTransferSizeBytes = null,
        CancellationToken disposeCancellationToken = default)
    {
        _blobServiceClient = blobServiceClient ?? throw new ArgumentNullException(nameof(blobServiceClient));
        _filesystem = filesystem ?? throw new ArgumentNullException(nameof(filesystem));
        _path = path ?? throw new ArgumentNullException(nameof(path));
        _contentType = contentType;
        _uploadThreshold = uploadThreshold;
        _maximumConcurrency = maximumConcurrency;
        _maximumTransferSizeBytes = maximumTransferSizeBytes;
        _tempFilePath = Path.Combine(Path.GetTempPath(), $"adls-upload-{Guid.NewGuid()}.tmp");
        _disposeCancellationToken = disposeCancellationToken;

        _tempFileStream = new FileStream(
            _tempFilePath,
            FileMode.Create,
            FileAccess.ReadWrite,
            FileShare.None,
            81920,
            FileOptions.Asynchronous | FileOptions.DeleteOnClose);
    }

    private bool IsDisposed => Volatile.Read(ref _disposeState) != 0;

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

        await _tempFileStream.WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken).ConfigureAwait(false);
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
        // Ensure only one disposing path executes
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

                    // Upload to ADLS Gen2
                    using var cts = CreateLinkedUploadCts();
                    UploadAsync(cts.Token).GetAwaiter().GetResult();
                }
            }
            catch (Exception ex)
            {
                if (ex is OperationCanceledException)
                    throw;

                capturedException = ExceptionDispatchInfo.Capture(ex);
            }
            finally
            {
                _tempFileStream?.Dispose();
                _tempFileStream = null;

                TryDeleteTempFileOnSuccess();
            }
        }

        Interlocked.Exchange(ref _disposeState, 2);

        capturedException?.Throw();

        base.Dispose(disposing);
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        // Ensure only one async disposing path executes
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

                // Upload to ADLS Gen2
                using var cts = CreateLinkedUploadCts();
                await UploadAsync(cts.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
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

            TryDeleteTempFileOnSuccess();

            Interlocked.Exchange(ref _disposeState, 2);
        }

        await base.DisposeAsync();

        capturedException?.Throw();
    }

    /// <summary>
    ///     Uploads the buffered data to ADLS Gen2.
    /// </summary>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    private async Task UploadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_tempFileStream is null || _uploaded)
            return;

        var blobClient = _blobServiceClient.GetBlobContainerClient(_filesystem).GetBlobClient(_path);

        try
        {
            // Determine if we should use chunked upload based on file size
            var fileSize = _tempFileStream.Length;

            if (fileSize >= _uploadThreshold)
            {
                // Use chunked upload for large files
                await UploadChunkedAsync(blobClient, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                // Use simple upload for smaller files
                await UploadSimpleAsync(blobClient, cancellationToken).ConfigureAwait(false);
            }

            _uploaded = true;
        }
        catch (RequestFailedException ex)
        {
            throw TranslateAdlsException(ex, _filesystem, _path);
        }
    }

    private CancellationTokenSource CreateLinkedUploadCts()
    {
        var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(_disposeCancellationToken);
        linkedSource.CancelAfter(UploadDisposeTimeout);
        return linkedSource;
    }

    private void TryDeleteTempFileOnSuccess()
    {
        if (!_uploaded)
            return;

        try
        {
            if (File.Exists(_tempFilePath))
                File.Delete(_tempFilePath);
        }
        catch
        {
            // Best-effort cleanup; ignore failures to delete the temporary file.
        }
    }

    /// <summary>
    ///     Uploads the file using simple upload for smaller files via Blob API.
    /// </summary>
    private async Task UploadSimpleAsync(Azure.Storage.Blobs.BlobClient blobClient, CancellationToken cancellationToken)
    {
        var options = new BlobUploadOptions();

        if (!string.IsNullOrEmpty(_contentType))
            options.HttpHeaders = new BlobHttpHeaders { ContentType = _contentType };

        // Ensure container (filesystem) exists before uploading
        var containerClient = _blobServiceClient.GetBlobContainerClient(_filesystem);
        _ = await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        _ = await blobClient.UploadAsync(_tempFileStream, options, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Uploads the file using chunked upload for large files via Blob API.
    /// </summary>
    private async Task UploadChunkedAsync(Azure.Storage.Blobs.BlobClient blobClient, CancellationToken cancellationToken)
    {
        // Ensure container (filesystem) exists before uploading
        var containerClient = _blobServiceClient.GetBlobContainerClient(_filesystem);
        _ = await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        var options = new BlobUploadOptions();

        if (!string.IsNullOrEmpty(_contentType))
            options.HttpHeaders = new BlobHttpHeaders { ContentType = _contentType };

        if (_maximumConcurrency.HasValue || _maximumTransferSizeBytes.HasValue)
        {
            var transferOptions = new StorageTransferOptions();

            if (_maximumConcurrency.HasValue)
                transferOptions.MaximumConcurrency = _maximumConcurrency.Value;

            if (_maximumTransferSizeBytes.HasValue)
                transferOptions.MaximumTransferSize = _maximumTransferSizeBytes.Value;

            options.TransferOptions = transferOptions;
        }

        _ = await blobClient.UploadAsync(_tempFileStream, options, cancellationToken).ConfigureAwait(false);
    }

    private static Exception TranslateAdlsException(RequestFailedException ex, string filesystem, string path)
    {
        var code = ex.ErrorCode ?? string.Empty;
        var status = ex.Status;

        return code switch
        {
            "AuthenticationFailed" or "AuthorizationFailed" or "AuthorizationFailure"
                => new UnauthorizedAccessException(
                    $"Access denied to ADLS filesystem '{filesystem}' and path '{path}'. Status={status}, Code={code}. {ex.Message}", ex),
            "InvalidQueryParameterValue" or "InvalidResourceName"
                => new ArgumentException(
                    $"Invalid ADLS filesystem '{filesystem}' or path '{path}'. Status={status}, Code={code}. {ex.Message}", ex),
            "FilesystemNotFound" or "PathNotFound"
                => new FileNotFoundException(
                    $"ADLS filesystem '{filesystem}' or path '{path}' not found. Status={status}, Code={code}.", ex),
            _ when status is 401 or 403
                => new UnauthorizedAccessException(
                    $"Access denied to ADLS filesystem '{filesystem}' and path '{path}'. Status={status}, Code={code}. {ex.Message}", ex),
            _ when status == 400
                => new ArgumentException(
                    $"Invalid ADLS filesystem '{filesystem}' or path '{path}'. Status={status}, Code={code}. {ex.Message}", ex),
            _ when status == 404
                => new FileNotFoundException(
                    $"ADLS filesystem '{filesystem}' or path '{path}' not found. Status={status}, Code={code}.", ex),
            _
                => new IOException(
                    $"Failed to upload to ADLS filesystem '{filesystem}' and path '{path}'. Status={status}, Code={code}. {ex.Message}", ex),
        };
    }
}

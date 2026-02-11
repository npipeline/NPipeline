using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Runtime.ExceptionServices;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     A stream that buffers writes and uploads to Azure Blob Storage on disposal.
/// </summary>
public sealed class AzureBlobWriteStream : Stream
{
    private readonly string _blob;
    private readonly long _blockBlobUploadThreshold;
    private readonly string _container;
    private readonly string? _contentType;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly CancellationToken _disposeCancellationToken;
    private readonly int? _maximumConcurrency;
    private readonly int? _maximumTransferSizeBytes;
    private readonly string _tempFilePath;
    private static readonly TimeSpan UploadDisposeTimeout = TimeSpan.FromMinutes(5);
    private bool _disposed;
    private FileStream? _tempFileStream;
    private bool _uploaded;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AzureBlobWriteStream" /> class.
    /// </summary>
    /// <param name="blobServiceClient">The Azure Blob Service client.</param>
    /// <param name="container">The Azure container name.</param>
    /// <param name="blob">The Azure blob name.</param>
    /// <param name="contentType">Optional content type for the upload.</param>
    /// <param name="blockBlobUploadThreshold">Threshold in bytes for using block blob upload.</param>
    /// <param name="maximumConcurrency">Maximum concurrent upload requests for large blobs.</param>
    /// <param name="maximumTransferSizeBytes">Maximum transfer size in bytes for each upload chunk.</param>
    /// <param name="disposeCancellationToken">Cancellation token to observe while disposing the stream.</param>
    public AzureBlobWriteStream(
        BlobServiceClient blobServiceClient,
        string container,
        string blob,
        string? contentType = null,
        long blockBlobUploadThreshold = 64 * 1024 * 1024,
        int? maximumConcurrency = null,
        int? maximumTransferSizeBytes = null,
        CancellationToken disposeCancellationToken = default)
    {
        _blobServiceClient = blobServiceClient ?? throw new ArgumentNullException(nameof(blobServiceClient));
        _container = container ?? throw new ArgumentNullException(nameof(container));
        _blob = blob ?? throw new ArgumentNullException(nameof(blob));
        _contentType = contentType;
        _blockBlobUploadThreshold = blockBlobUploadThreshold;
        _maximumConcurrency = maximumConcurrency;
        _maximumTransferSizeBytes = maximumTransferSizeBytes;
        _tempFilePath = Path.Combine(Path.GetTempPath(), $"azure-upload-{Guid.NewGuid()}.tmp");
        _disposeCancellationToken = disposeCancellationToken;

        _tempFileStream = new FileStream(
            _tempFilePath,
            FileMode.Create,
            FileAccess.ReadWrite,
            FileShare.None,
            81920,
            FileOptions.Asynchronous);
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
        {
            ObjectDisposedException.ThrowIf(true, this);
        }

        await _tempFileStream.WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override async ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_tempFileStream is null)
        {
            ObjectDisposedException.ThrowIf(true, this);
        }

        await _tempFileStream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (disposing)
        {
            ExceptionDispatchInfo? capturedException = null;

            try
            {
                if (!_uploaded && _tempFileStream is not null)
                {
                    // Flush the temp file stream to ensure all data is written
                    _tempFileStream.Flush();

                    // Reset position to beginning for upload
                    _tempFileStream.Position = 0;

                    // Upload to Azure Blob Storage
                    using var cts = CreateLinkedUploadCts();
                    UploadAsync(cts.Token).GetAwaiter().GetResult();
                }
            }
            catch (Exception ex)
            {
                if (ex is OperationCanceledException)
                {
                    throw;
                }

                capturedException = ExceptionDispatchInfo.Capture(ex);
            }
            finally
            {
                _tempFileStream?.Dispose();
                _tempFileStream = null;

                TryDeleteTempFileOnSuccess();
            }

            capturedException?.Throw();
        }

        base.Dispose(disposing);
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        ExceptionDispatchInfo? capturedException = null;

        try
        {
            if (!_uploaded && _tempFileStream is not null)
            {
                // Flush the temp file stream to ensure all data is written
                await _tempFileStream.FlushAsync(CancellationToken.None).ConfigureAwait(false);

                // Reset position to beginning for upload
                _tempFileStream.Position = 0;

                // Upload to Azure Blob Storage
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
        }

        await base.DisposeAsync();

        capturedException?.Throw();
    }

    /// <summary>
    ///     Uploads the buffered data to Azure Blob Storage.
    /// </summary>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    private async Task UploadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_tempFileStream is null || _uploaded)
        {
            return;
        }

        var blobClient = _blobServiceClient.GetBlobContainerClient(_container).GetBlobClient(_blob);

        try
        {
            // Determine if we should use block blob upload based on file size
            var fileSize = _tempFileStream.Length;

            if (fileSize >= _blockBlobUploadThreshold)
            {
                // Use block blob upload for large files
                await UploadBlockBlobAsync(blobClient, cancellationToken).ConfigureAwait(false);
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
            throw TranslateAzureException(ex, _container, _blob);
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
        {
            return;
        }

        try
        {
            if (File.Exists(_tempFilePath))
            {
                File.Delete(_tempFilePath);
            }
        }
        catch
        {
            // Best-effort cleanup; ignore failures to delete the temporary file.
        }
    }

    /// <summary>
    ///     Uploads the blob using simple upload for smaller files.
    /// </summary>
    private async Task UploadSimpleAsync(BlobClient blobClient, CancellationToken cancellationToken)
    {
        var options = new BlobUploadOptions();

        if (!string.IsNullOrEmpty(_contentType))
        {
            options.HttpHeaders = new BlobHttpHeaders
            {
                ContentType = _contentType,
            };
        }

        // Ensure container exists before uploading
        var containerClient = _blobServiceClient.GetBlobContainerClient(_container);
        _ = await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        _ = await blobClient.UploadAsync(_tempFileStream, options, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Uploads the blob using block blob upload for large files.
    /// </summary>
    private async Task UploadBlockBlobAsync(BlobClient blobClient, CancellationToken cancellationToken)
    {
        // Ensure container exists before uploading
        var containerClient = _blobServiceClient.GetBlobContainerClient(_container);
        _ = await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        var options = new BlobUploadOptions();

        if (!string.IsNullOrEmpty(_contentType))
        {
            options.HttpHeaders = new BlobHttpHeaders
            {
                ContentType = _contentType,
            };
        }

        if (_maximumConcurrency.HasValue || _maximumTransferSizeBytes.HasValue)
        {
            var transferOptions = new StorageTransferOptions();

            if (_maximumConcurrency.HasValue)
            {
                transferOptions.MaximumConcurrency = _maximumConcurrency.Value;
            }

            if (_maximumTransferSizeBytes.HasValue)
            {
                transferOptions.MaximumTransferSize = _maximumTransferSizeBytes.Value;
            }

            options.TransferOptions = transferOptions;
        }

        _ = await blobClient.UploadAsync(_tempFileStream, options, cancellationToken).ConfigureAwait(false);
    }

    private static Exception TranslateAzureException(RequestFailedException ex, string container, string blob)
    {
        var code = ex.ErrorCode ?? string.Empty;
        var status = ex.Status;

        return code switch
        {
            "AuthenticationFailed" or "AuthorizationFailed" or "AuthorizationFailure"
                => new UnauthorizedAccessException(
                    $"Access denied to Azure container '{container}' and blob '{blob}'. Status={status}, Code={code}. {ex.Message}", ex),
            "InvalidQueryParameterValue" or "InvalidResourceName"
                => new ArgumentException(
                    $"Invalid Azure container '{container}' or blob '{blob}'. Status={status}, Code={code}. {ex.Message}", ex),
            "ContainerNotFound" or "BlobNotFound"
                => new FileNotFoundException(
                    $"Azure container '{container}' or blob '{blob}' not found. Status={status}, Code={code}.", ex),
            _ when status is 401 or 403
                => new UnauthorizedAccessException(
                    $"Access denied to Azure container '{container}' and blob '{blob}'. Status={status}, Code={code}. {ex.Message}", ex),
            _ when status == 400
                => new ArgumentException(
                    $"Invalid Azure container '{container}' or blob '{blob}'. Status={status}, Code={code}. {ex.Message}", ex),
            _ when status == 404
                => new FileNotFoundException(
                    $"Azure container '{container}' or blob '{blob}' not found. Status={status}, Code={code}.", ex),
            _
                => new IOException(
                    $"Failed to upload to Azure container '{container}' and blob '{blob}'. Status={status}, Code={code}. {ex.Message}", ex),
        };
    }
}

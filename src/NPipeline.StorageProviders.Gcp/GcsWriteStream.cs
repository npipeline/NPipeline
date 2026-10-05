using System.Buffers;
using System.IO.Pipelines;
using Google;
using Google.Cloud.Storage.V1;
using NPipeline.StorageProviders.Abstractions;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace NPipeline.StorageProviders.Gcp;

/// <summary>
///     A write-only stream that uploads to Google Cloud Storage while the caller writes. Writes go into a
///     <see cref="Pipe" /> whose reader feeds <see cref="StorageClient.UploadObjectAsync(Object, Stream, UploadObjectOptions, CancellationToken, IProgress{Google.Apis.Upload.IUploadProgress})" />,
///     a resumable upload that runs concurrently. Nothing is spooled to disk, and memory is bounded by about two chunks:
///     one the SDK is sending and one the pipe holds before it pauses the writer.
/// </summary>
/// <remarks>
///     <para>
///         Commit and abort: the object appears only when the final chunk of the resumable session is sent.
///         <see cref="CommitAsync" /> completes the pipe, which lets the SDK send that chunk, and awaits the upload. Disposing
///         without committing cancels the upload and completes the pipe with an error rather than an end-of-stream, so the
///         SDK cannot mistake the abandoned data for a finished object. The final chunk is never sent, so no object
///         appears at the target; the session expires on the server.
///     </para>
///     <para>
///         Retries: a stream cannot be replayed, so <see cref="GcsStorageProviderOptions.Resilience" /> does not wrap the
///         upload. Transient chunk failures are retried by the SDK inside the resumable session, which asks the server
///         how much it received and re-sends from there. A failure the SDK gives up on faults the stream: the next
///         write, or <see cref="CommitAsync" />, throws it, and the caller can write the object again from the start.
///     </para>
/// </remarks>
public sealed class GcsWriteStream : StorageWriteStream
{
    private const int ChunkAlignmentBytes = 256 * 1024;

    private readonly string _bucket;
    private readonly int _chunkSizeBytes;
    private readonly string? _contentType;
    private readonly CancellationTokenSource _cancellation = new();
    private readonly string _objectName;
    private readonly Pipe _pipe;
    private readonly StorageClient _storageClient;

    private bool _committed;
    private int _disposed;
    private long _length;
    private Task<Object?>? _upload;

    /// <summary>
    ///     Initializes a new instance of the <see cref="GcsWriteStream" /> class.
    /// </summary>
    /// <param name="storageClient">The Google Cloud Storage client.</param>
    /// <param name="bucket">The GCS bucket name.</param>
    /// <param name="objectName">The GCS object name (key).</param>
    /// <param name="contentType">Optional content type for the upload.</param>
    /// <param name="chunkSizeBytes">Chunk size for the resumable upload, a positive multiple of 256 KiB. Default is 16 MB.</param>
    public GcsWriteStream(
        StorageClient storageClient,
        string bucket,
        string objectName,
        string? contentType = null,
        int chunkSizeBytes = 16 * 1024 * 1024)
    {
        _storageClient = storageClient ?? throw new ArgumentNullException(nameof(storageClient));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _objectName = objectName ?? throw new ArgumentNullException(nameof(objectName));
        _contentType = contentType;

        if (chunkSizeBytes <= 0 || chunkSizeBytes % ChunkAlignmentBytes != 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(chunkSizeBytes),
                chunkSizeBytes,
                "Chunk size must be a positive multiple of 256 KiB (262,144 bytes).");
        }

        _chunkSizeBytes = chunkSizeBytes;

        // The writer pauses once a chunk's worth of data is waiting for the SDK, which is the back-pressure.
        _pipe = new Pipe(new PipeOptions(
            pauseWriterThreshold: chunkSizeBytes,
            resumeWriterThreshold: chunkSizeBytes / 2,
            minimumSegmentSize: 64 * 1024,
            useSynchronizationContext: false));
    }

    /// <inheritdoc />
    public override bool CanRead => false;

    /// <inheritdoc />
    public override bool CanSeek => false;

    /// <inheritdoc />
    public override bool CanWrite => _disposed == 0 && !_committed;

    /// <inheritdoc />
    public override long Length
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            return _length;
        }
    }

    /// <inheritdoc />
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        if (_committed)
            throw new InvalidOperationException("The stream has already been committed.");

        cancellationToken.ThrowIfCancellationRequested();

        var upload = EnsureUploadStarted();

        // The upload runs under the stream's own token, so the commit's token cancels it through this registration.
        using var registration = cancellationToken.Register(static state => ((CancellationTokenSource)state!).Cancel(), _cancellation);

        // End of stream: the SDK sends the final chunk, which is what makes the object appear.
        await _pipe.Writer.CompleteAsync().ConfigureAwait(false);

        var uploaded = await upload.ConfigureAwait(false);
        var etag = uploaded?.ETag;
        ETag = string.IsNullOrEmpty(etag) ? null : etag;
        _committed = true;
    }

    /// <inheritdoc />
    public override void Flush()
    {
        // Nothing becomes visible until CommitAsync.
    }

    /// <inheritdoc />
    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    /// <inheritdoc />
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    /// <inheritdoc />
    public override void SetLength(long value) => throw new NotSupportedException();

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count) =>
        Write(new ReadOnlySpan<byte>(buffer, offset, count));

    /// <inheritdoc />
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        ThrowIfNotWritable();
        _ = EnsureUploadStarted();

        // Blocks while the pipe is full, which is the back-pressure on a synchronous writer.
        _pipe.Writer.Write(buffer);
        var result = _pipe.Writer.FlushAsync().AsTask().GetAwaiter().GetResult();
        _length += buffer.Length;

        if (result.IsCompleted)
            ThrowUploadEnded().GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    /// <inheritdoc />
    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ThrowIfNotWritable();
        _ = EnsureUploadStarted();

        // Waits while the pipe is full. If the upload failed, the pipe's reader was completed with the failure, so the
        // write throws it (or reports the pipe as completed, which ThrowUploadEnded turns into the same exception).
        var result = await _pipe.Writer.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
        _length += buffer.Length;

        if (result.IsCompleted)
            await ThrowUploadEnded().ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing && Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            var upload = Abort();

            if (upload is null)
                _cancellation.Dispose();
            else
            {
                // Do not block a synchronous Dispose on the network; observe the outcome and release the token.
                _ = upload.ContinueWith(
                    static (task, state) =>
                    {
                        _ = task.Exception;
                        ((CancellationTokenSource)state!).Dispose();
                    },
                    _cancellation,
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
        }

        base.Dispose(disposing);
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            var upload = Abort();

            if (upload is not null)
            {
                try
                {
                    _ = await upload.ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Discarding an upload: its cancellation, or a failure the caller already saw, is not news.
                }
            }

            _cancellation.Dispose();
        }

        await base.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    ///     Stops an upload that was not committed. The token is cancelled first and the pipe is completed with an error,
    ///     never normally, so the SDK sees a failure and not the end of the data, and cannot send the final chunk.
    /// </summary>
    private Task<Object?>? Abort()
    {
        if (_committed)
            return null;

        _cancellation.Cancel();

        var abandoned = new OperationCanceledException("The GCS write was disposed without being committed.");
        _pipe.Writer.Complete(abandoned);
        _pipe.Reader.Complete(abandoned);

        return _upload;
    }

    private Task<Object?> EnsureUploadStarted() => _upload ??= Task.Run(UploadAsync);

    private async Task<Object?> UploadAsync()
    {
        var reader = _pipe.Reader;

        try
        {
            var options = new UploadObjectOptions { ChunkSize = _chunkSizeBytes };
            var obj = new Object { Bucket = _bucket, Name = _objectName };

            if (!string.IsNullOrEmpty(_contentType))
                obj.ContentType = _contentType;

            using var source = reader.AsStream(true);

            var uploaded = await _storageClient.UploadObjectAsync(obj, source, options, _cancellation.Token).ConfigureAwait(false);
            await reader.CompleteAsync().ConfigureAwait(false);

            return uploaded;
        }
        catch (GoogleApiException ex)
        {
            var translated = GcsErrors.Translate(ex, _bucket, _objectName, "upload");

            // Wakes a writer that is waiting on a full pipe, and fails its next write with the reason.
            await reader.CompleteAsync(translated).ConfigureAwait(false);
            throw translated;
        }
        catch (Exception ex)
        {
            await reader.CompleteAsync(ex).ConfigureAwait(false);
            throw;
        }
    }

    private async Task ThrowUploadEnded()
    {
        if (_upload is { } upload)
        {
            // Throws the upload's failure.
            _ = await upload.ConfigureAwait(false);
        }

        throw new IOException("The GCS upload ended before the stream was committed.");
    }

    private void ThrowIfNotWritable()
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        if (_committed)
            throw new InvalidOperationException("The stream has been committed and accepts no more writes.");
    }
}

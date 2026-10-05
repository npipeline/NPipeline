using System.Buffers;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;

namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     A <see cref="StorageWriteStream" /> that uploads while the caller writes. Bytes accumulate in a pooled buffer; each
///     full buffer is handed to <see cref="UploadPartAsync" /> and the stream keeps accepting writes, with at most
///     <c>maxConcurrency</c> parts in flight. Memory is bounded by <c>partSize × (maxConcurrency + 1)</c> and nothing goes
///     to local disk. An object that fits in one part is sent with <see cref="UploadSingleAsync" />; a larger one is
///     assembled by <see cref="CompleteAsync" /> in <see cref="CommitAsync" />. Disposing without committing cancels
///     in-flight parts and calls <see cref="AbortAsync" />.
/// </summary>
public abstract class ChunkedUploadStream : StorageWriteStream
{
    private readonly CancellationTokenSource _cancellation = new();
    private readonly List<Task> _inFlight = [];
    private readonly SemaphoreSlim _slots;

    private bool _aborted;
    private byte[]? _buffer;
    private int _capacity;
    private bool _committed;
    private int _disposed;
    private ExceptionDispatchInfo? _fault;
    private int _filled;
    private long _offset;
    private int _partCount;
    private bool _started;

    /// <summary>Creates the stream.</summary>
    /// <param name="partSizeBytes">The size of each part, except that growth is allowed by <see cref="PartSizeFor" />.</param>
    /// <param name="maxConcurrency">The most parts that may upload at the same time.</param>
    protected ChunkedUploadStream(int partSizeBytes, int maxConcurrency)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partSizeBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxConcurrency);

        PartSizeBytes = partSizeBytes;
        _slots = new SemaphoreSlim(maxConcurrency, maxConcurrency);
    }

    /// <summary>Gets the configured part size in bytes.</summary>
    protected int PartSizeBytes { get; }

    /// <inheritdoc />
    public override bool CanRead => false;

    /// <inheritdoc />
    public override bool CanSeek => false;

    /// <inheritdoc />
    public override bool CanWrite => _disposed == 0 && !_committed;

    /// <inheritdoc />
    public override long Length => _offset + _filled;

    /// <inheritdoc />
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    /// <summary>The size of the part with the given 1-based number. Override to grow parts for very large objects.</summary>
    /// <param name="partNumber">The 1-based part number.</param>
    /// <returns>The part size in bytes.</returns>
    protected virtual int PartSizeFor(int partNumber) => PartSizeBytes;

    /// <summary>Called once, before the first part is uploaded. Start the multi-part session here.</summary>
    /// <param name="cancellationToken">Cancelled when the stream is aborted or an earlier step failed.</param>
    protected virtual Task BeginAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>Uploads one part. Calls for different parts run concurrently, and may complete in any order.</summary>
    /// <param name="partNumber">The 1-based part number.</param>
    /// <param name="offset">The offset of the part's first byte within the object.</param>
    /// <param name="data">The part's bytes. Valid only until the returned task completes.</param>
    /// <param name="cancellationToken">Cancelled when the stream is aborted or another part failed.</param>
    protected abstract Task UploadPartAsync(int partNumber, long offset, ReadOnlyMemory<byte> data, CancellationToken cancellationToken);

    /// <summary>Uploads an object that fits in one part, in one request.</summary>
    /// <param name="data">The whole object. Valid only until the returned task completes.</param>
    /// <param name="cancellationToken">The token of the commit.</param>
    /// <returns>The committed object's ETag or generation, or <see langword="null" /> when the store reports none.</returns>
    protected abstract Task<string?> UploadSingleAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken);

    /// <summary>Makes the uploaded parts into the object. Called after every part has uploaded.</summary>
    /// <param name="partCount">The number of parts.</param>
    /// <param name="totalLength">The object's length in bytes.</param>
    /// <param name="cancellationToken">The token of the commit.</param>
    /// <returns>The committed object's ETag or generation, or <see langword="null" /> when the store reports none.</returns>
    protected abstract Task<string?> CompleteAsync(int partCount, long totalLength, CancellationToken cancellationToken);

    /// <summary>Discards the multi-part session. Called when the stream is disposed without a successful commit, if <see cref="BeginAsync" /> ran.</summary>
    /// <returns>A task that completes when the session is discarded. Failures are ignored.</returns>
    protected virtual Task AbortAsync() => Task.CompletedTask;

    /// <summary>Wraps a part's bytes in a read-only, seekable stream without copying.</summary>
    /// <param name="data">The bytes, which must be array-backed.</param>
    /// <returns>A stream over <paramref name="data" />.</returns>
    protected static MemoryStream AsStream(ReadOnlyMemory<byte> data) =>
        MemoryMarshal.TryGetArray(data, out var segment)
            ? new MemoryStream(segment.Array!, segment.Offset, segment.Count, false)
            : new MemoryStream(data.ToArray(), false);

    /// <inheritdoc />
    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        if (_committed)
            throw new InvalidOperationException("The stream has already been committed.");

        cancellationToken.ThrowIfCancellationRequested();
        _fault?.Throw();

        // A cancelled commit also cancels the parts still uploading.
        using var registration = cancellationToken.Register(static state => ((CancellationTokenSource)state!).Cancel(), _cancellation);

        try
        {
            if (_partCount == 0)
            {
                ETag = await UploadSingleAsync(_buffer.AsMemory(0, _filled), cancellationToken).ConfigureAwait(false);
            }
            else
            {
                if (_filled > 0)
                    await DispatchAsync(cancellationToken).ConfigureAwait(false);

                await Task.WhenAll(_inFlight).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
                _fault?.Throw();
                ETag = await CompleteAsync(_partCount, _offset, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception) when (_fault is not null && !cancellationToken.IsCancellationRequested)
        {
            // A part failed; report that failure rather than the cancellation it caused in its siblings.
            _fault.Throw();
            throw;
        }

        _committed = true;
        ReleaseBuffer();
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
        // Upload requests are asynchronous, so the synchronous path waits on the asynchronous one. Prefer WriteAsync.
        WriteAsync(buffer.AsMemory(offset, count), CancellationToken.None).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc />
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        var copy = buffer.ToArray();
        WriteAsync(copy, CancellationToken.None).AsTask().GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    /// <inheritdoc />
    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        if (_committed)
            throw new InvalidOperationException("The stream has been committed and accepts no more writes.");

        _fault?.Throw();

        while (!buffer.IsEmpty)
        {
            // A full buffer is sent only when more data arrives, so an object of exactly one part stays a single upload.
            if (_buffer is not null && _filled == _capacity)
                await DispatchAsync(cancellationToken).ConfigureAwait(false);

            if (_buffer is null)
            {
                _capacity = PartSizeFor(_partCount + 1);
                _buffer = ArrayPool<byte>.Shared.Rent(_capacity);
            }

            var count = Math.Min(buffer.Length, _capacity - _filled);
            buffer.Slice(0, count).CopyTo(_buffer.AsMemory(_filled));
            _filled += count;
            buffer = buffer.Slice(count);
        }
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing && Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            // Dispose does no I/O on the caller's thread; the abort runs in the background.
            _ = Task.Run(CleanUpAsync);
        }

        base.Dispose(disposing);
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
            await CleanUpAsync().ConfigureAwait(false);

        await base.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private async Task DispatchAsync(CancellationToken cancellationToken)
    {
        // Wait for a free slot first: this is the back-pressure on the writer.
        await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);

        var slotHeld = true;

        try
        {
            _fault?.Throw();

            var partNumber = _partCount + 1;

            if (!_started)
            {
                _started = true;
                await BeginAsync(_cancellation.Token).ConfigureAwait(false);
            }

            var buffer = _buffer!;
            var length = _filled;
            var offset = _offset;

            _partCount = partNumber;
            _offset += length;
            _buffer = null;
            _filled = 0;
            slotHeld = false;
            _inFlight.Add(RunPartAsync(partNumber, offset, buffer, length));
        }
        finally
        {
            if (slotHeld)
                _ = _slots.Release();
        }
    }

    private async Task RunPartAsync(int partNumber, long offset, byte[] buffer, int length)
    {
        try
        {
            await UploadPartAsync(partNumber, offset, buffer.AsMemory(0, length), _cancellation.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Keep the first failure; the rest are usually cancellations it caused.
            _ = Interlocked.CompareExchange(ref _fault, ExceptionDispatchInfo.Capture(ex), null);
            await _cancellation.CancelAsync().ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            _ = _slots.Release();
        }
    }

    private async Task CleanUpAsync()
    {
        if (!_committed)
        {
            try
            {
                await _cancellation.CancelAsync().ConfigureAwait(false);
                await Task.WhenAll(_inFlight).ConfigureAwait(false);
            }
            catch
            {
                // Parts report their failures through _fault; the stream is being discarded.
            }

            if (_started && !_aborted)
            {
                _aborted = true;

                try
                {
                    await AbortAsync().ConfigureAwait(false);
                }
                catch
                {
                    // Best effort: an abandoned session expires on the service.
                }
            }
        }

        ReleaseBuffer();
        _cancellation.Dispose();
    }

    private void ReleaseBuffer()
    {
        var buffer = Interlocked.Exchange(ref _buffer, null);

        if (buffer is not null)
            ArrayPool<byte>.Shared.Return(buffer);

        _filled = 0;
    }
}

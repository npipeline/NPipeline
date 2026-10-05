namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     A <see cref="StorageWriteStream" /> for stores that take an object in one request or one multi-part sequence. Writes
///     go to a local temporary file, <see cref="CommitAsync" /> uploads it, and disposing without committing deletes it, so
///     nothing reaches the target unless the writer finished.
/// </summary>
public abstract class SpooledWriteStream : StorageWriteStream
{
    private readonly FileStream _spool;
    private bool _committed;
    private bool _disposed;

    /// <summary>Creates the temporary file.</summary>
    /// <param name="tempFilePrefix">A prefix for the temporary file's name, such as <c>s3-upload</c>.</param>
    protected SpooledWriteStream(string tempFilePrefix)
    {
        ArgumentException.ThrowIfNullOrEmpty(tempFilePrefix);

        _spool = new FileStream(
            Path.Combine(Path.GetTempPath(), $"{tempFilePrefix}-{Guid.NewGuid():N}.tmp"),
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
    public override bool CanWrite => !_disposed && !_committed;

    /// <inheritdoc />
    public override long Length
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _spool.Length;
        }
    }

    /// <inheritdoc />
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    /// <summary>Uploads the content to the target. The upload runs under the token passed to <see cref="CommitAsync" />.</summary>
    /// <param name="content">The written bytes, positioned at the start. It is owned by the stream; do not dispose it.</param>
    /// <param name="cancellationToken">The token of the commit.</param>
    /// <returns>The committed object's ETag or generation, or <see langword="null" /> when the store reports none.</returns>
    protected abstract Task<string?> UploadAsync(Stream content, CancellationToken cancellationToken);

    /// <inheritdoc />
    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_committed)
            throw new InvalidOperationException("The stream has already been committed.");

        cancellationToken.ThrowIfCancellationRequested();
        await _spool.FlushAsync(cancellationToken).ConfigureAwait(false);
        _spool.Position = 0;
        ETag = await UploadAsync(_spool, cancellationToken).ConfigureAwait(false);
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
    public override void Write(byte[] buffer, int offset, int count)
    {
        ThrowIfNotWritable();
        _spool.Write(buffer, offset, count);
    }

    /// <inheritdoc />
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        ThrowIfNotWritable();
        _spool.Write(buffer);
    }

    /// <inheritdoc />
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    /// <inheritdoc />
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ThrowIfNotWritable();
        return _spool.WriteAsync(buffer, cancellationToken);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing && !_disposed)
        {
            _disposed = true;
            _spool.Dispose();
        }

        base.Dispose(disposing);
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await _spool.DisposeAsync().ConfigureAwait(false);
        }

        await base.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private void ThrowIfNotWritable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_committed)
            throw new InvalidOperationException("The stream has been committed and accepts no more writes.");
    }
}

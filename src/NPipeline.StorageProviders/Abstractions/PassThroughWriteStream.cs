namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     Adapts a stream whose data is published when it is disposed to <see cref="StorageWriteStream" />.
///     <see cref="CommitAsync" /> flushes and does nothing else: the underlying stream still commits on dispose.
/// </summary>
/// <param name="inner">The stream to write through to. It is disposed with this stream.</param>
public sealed class PassThroughWriteStream(Stream inner) : StorageWriteStream
{
    /// <inheritdoc />
    public override bool CanRead => false;

    /// <inheritdoc />
    public override bool CanSeek => inner.CanSeek;

    /// <inheritdoc />
    public override bool CanWrite => inner.CanWrite;

    /// <inheritdoc />
    public override long Length => inner.Length;

    /// <inheritdoc />
    public override long Position
    {
        get => inner.Position;
        set => inner.Position = value;
    }

    /// <inheritdoc />
    public override Task CommitAsync(CancellationToken cancellationToken = default) => inner.FlushAsync(cancellationToken);

    /// <inheritdoc />
    public override void Flush() => inner.Flush();

    /// <inheritdoc />
    public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

    /// <inheritdoc />
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    /// <inheritdoc />
    public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

    /// <inheritdoc />
    public override void SetLength(long value) => inner.SetLength(value);

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);

    /// <inheritdoc />
    public override void Write(ReadOnlySpan<byte> buffer) => inner.Write(buffer);

    /// <inheritdoc />
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        inner.WriteAsync(buffer, offset, count, cancellationToken);

    /// <inheritdoc />
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
        inner.WriteAsync(buffer, cancellationToken);

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
            inner.Dispose();

        base.Dispose(disposing);
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await inner.DisposeAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }
}

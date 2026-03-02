namespace NPipeline.StorageProviders.Sftp;

/// <summary>
///     A read stream backed by SSH.NET's native <c>SftpClient.OpenRead()</c>.
///     Holds a connection lease from <see cref="SftpClientPool" /> for its lifetime;
///     the lease is returned to the pool when the stream is disposed.
/// </summary>
public sealed class SftpReadStream : Stream
{
    private readonly IPooledConnection _lease;
    private readonly Stream _sftpStream;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SftpReadStream" /> class.
    /// </summary>
    /// <param name="lease">The pooled connection lease.</param>
    /// <param name="remotePath">The remote file path.</param>
    internal SftpReadStream(IPooledConnection lease, string remotePath)
    {
        _lease = lease ?? throw new ArgumentNullException(nameof(lease));

        if (string.IsNullOrWhiteSpace(remotePath))
            throw new ArgumentException("Remote path cannot be null or whitespace.", nameof(remotePath));

        _sftpStream = lease.Client.OpenRead(remotePath);
    }

    /// <inheritdoc />
    public override bool CanRead => !_disposed && _sftpStream.CanRead;

    /// <inheritdoc />
    public override bool CanSeek => !_disposed && _sftpStream.CanSeek;

    /// <inheritdoc />
    public override bool CanWrite => false;

    /// <inheritdoc />
    public override long Length
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _sftpStream.Length;
        }
    }

    /// <inheritdoc />
    public override long Position
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _sftpStream.Position;
        }
        set
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _sftpStream.Position = value;
        }
    }

    /// <inheritdoc />
    public override int Read(byte[] buffer, int offset, int count)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _sftpStream.Read(buffer, offset, count);
    }

    /// <inheritdoc />
    public override int Read(Span<byte> buffer)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _sftpStream.Read(buffer);
    }

    /// <inheritdoc />
    public override Task<int> ReadAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _sftpStream.ReadAsync(buffer, offset, count, cancellationToken);
    }

    /// <inheritdoc />
    public override ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _sftpStream.ReadAsync(buffer, cancellationToken);
    }

    /// <inheritdoc />
    public override long Seek(long offset, SeekOrigin origin)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _sftpStream.Seek(offset, origin);
    }

    /// <inheritdoc />
    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override Task WriteAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override void Flush()
    {
        // No-op for read stream
    }

    /// <inheritdoc />
    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        _disposed = true;

        if (disposing)
        {
            _sftpStream.Dispose();
            _lease.Dispose();
        }

        base.Dispose(disposing);
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        await _sftpStream.DisposeAsync().ConfigureAwait(false);
        await _lease.DisposeAsync().ConfigureAwait(false);

        await base.DisposeAsync().ConfigureAwait(false);
    }
}

using Renci.SshNet;
using Renci.SshNet.Common;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.Sftp;

/// <summary>
///     A write stream backed by an SSH.NET <c>SftpFileStream</c>. It writes to a hidden sibling of the target and renames it
///     into place in <see cref="CommitAsync" />; disposing without committing deletes the sibling. Holds a connection lease
///     from <see cref="SftpClientPool" /> for its lifetime; the lease is returned to the pool when the stream is disposed.
/// </summary>
/// <remarks>
///     Do NOT buffer the entire payload in a <see cref="MemoryStream" /> - that would cause
///     OOM on large files. SSH.NET streams data over the wire as each Write/WriteAsync call is made.
/// </remarks>
public sealed class SftpWriteStream : StorageWriteStream
{
    private readonly IPooledConnection _lease;
    private readonly Stream _sftpStream;
    private readonly string _targetPath;
    private readonly string _tempPath;
    private bool _committed;
    private bool _disposed;

    private SftpWriteStream(IPooledConnection lease, Stream sftpStream, string targetPath, string tempPath)
    {
        _lease = lease ?? throw new ArgumentNullException(nameof(lease));
        _sftpStream = sftpStream ?? throw new ArgumentNullException(nameof(sftpStream));
        _targetPath = targetPath;
        _tempPath = tempPath;
    }

    /// <summary>
    ///     Opens a temporary sibling of <paramref name="remotePath" /> for writing. The returned stream owns <paramref name="lease" />;
    ///     if opening fails, the caller keeps it.
    /// </summary>
    /// <param name="lease">The pooled connection lease.</param>
    /// <param name="remotePath">The remote file path the content is published to on commit.</param>
    /// <param name="createDirectory">Whether to create the parent directory if it doesn't exist.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    internal static async Task<SftpWriteStream> OpenAsync(
        IPooledConnection lease,
        string remotePath,
        bool createDirectory,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);

        if (string.IsNullOrWhiteSpace(remotePath))
            throw new ArgumentException("Remote path cannot be null or whitespace.", nameof(remotePath));

        if (createDirectory)
            await EnsureParentDirectoryExistsAsync(lease.Client, remotePath, cancellationToken).ConfigureAwait(false);

        var slash = remotePath.LastIndexOf('/');
        var tempPath = $"{remotePath[..(slash + 1)]}.{remotePath[(slash + 1)..]}.{Guid.NewGuid():N}.tmp";

        // CreateNew: the name is unique, and a leftover must never be appended to or truncated by another writer.
        var stream = await lease.Client.OpenAsync(tempPath, FileMode.CreateNew, FileAccess.Write, cancellationToken).ConfigureAwait(false);

        return new SftpWriteStream(lease, stream, remotePath, tempPath);
    }

    /// <inheritdoc />
    public override bool CanRead => false;

    /// <inheritdoc />
    public override bool CanSeek => false;

    /// <inheritdoc />
    public override bool CanWrite => !_disposed && !_committed && _sftpStream.CanWrite;

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
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_committed)
            throw new InvalidOperationException("The stream has already been committed.");

        cancellationToken.ThrowIfCancellationRequested();
        await _sftpStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        await _sftpStream.DisposeAsync().ConfigureAwait(false);

        var client = _lease.Client;

        // A rename does not overwrite, so the destination goes first.
        try
        {
            await client.DeleteFileAsync(_targetPath, cancellationToken).ConfigureAwait(false);
        }
        catch (SftpPathNotFoundException)
        {
            // Nothing to overwrite.
        }

        await client.RenameFileAsync(_tempPath, _targetPath, cancellationToken).ConfigureAwait(false);
        _committed = true;
    }

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count)
    {
        ThrowIfNotWritable();
        _sftpStream.Write(buffer, offset, count);
    }

    /// <inheritdoc />
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        ThrowIfNotWritable();
        _sftpStream.Write(buffer);
    }

    /// <inheritdoc />
    public override Task WriteAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        ThrowIfNotWritable();
        return _sftpStream.WriteAsync(buffer, offset, count, cancellationToken);
    }

    /// <inheritdoc />
    public override ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        ThrowIfNotWritable();
        return _sftpStream.WriteAsync(buffer, cancellationToken);
    }

    /// <inheritdoc />
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    /// <inheritdoc />
    public override int Read(Span<byte> buffer) => throw new NotSupportedException();

    /// <inheritdoc />
    public override Task<int> ReadAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public override ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    /// <inheritdoc />
    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override void Flush()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_committed)
            _sftpStream.Flush();
    }

    /// <inheritdoc />
    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _committed ? Task.CompletedTask : _sftpStream.FlushAsync(cancellationToken);
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
                _sftpStream.Dispose();

                if (!_committed)
                    TryDeleteTemp();
            }
            finally
            {
                _lease.Dispose();
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
            await _sftpStream.DisposeAsync().ConfigureAwait(false);

            if (!_committed)
                await TryDeleteTempAsync().ConfigureAwait(false);
        }
        finally
        {
            await _lease.DisposeAsync().ConfigureAwait(false);
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

    private void TryDeleteTemp()
    {
        try
        {
            _lease.Client.DeleteFile(_tempPath);
        }
        catch (Exception ex) when (ex is SshException or InvalidOperationException or ObjectDisposedException)
        {
            // Best effort: the temporary file is hidden and the target was never touched.
        }
    }

    private async Task TryDeleteTempAsync()
    {
        try
        {
            await _lease.Client.DeleteFileAsync(_tempPath, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is SshException or InvalidOperationException or ObjectDisposedException)
        {
            // Best effort: the temporary file is hidden and the target was never touched.
        }
    }

    /// <summary>Creates the parent directory of <paramref name="remotePath" /> and any missing ancestors.</summary>
    internal static async Task EnsureParentDirectoryExistsAsync(SftpClient client, string remotePath, CancellationToken cancellationToken)
    {
        var parentPath = GetParentPath(remotePath);

        if (string.IsNullOrEmpty(parentPath))
            return;

        if (!await client.ExistsAsync(parentPath, cancellationToken).ConfigureAwait(false))
            await CreateDirectoryRecursiveAsync(client, parentPath, cancellationToken).ConfigureAwait(false);
    }

    private static string? GetParentPath(string path)
    {
        if (string.IsNullOrEmpty(path))
            return null;

        // Normalize path separators
        var normalizedPath = path.Replace('\\', '/');

        // Remove trailing slashes
        normalizedPath = normalizedPath.TrimEnd('/');

        var lastSlashIndex = normalizedPath.LastIndexOf('/');

        if (lastSlashIndex <= 0)
            return null;

        return normalizedPath[..lastSlashIndex];
    }

    private static async Task CreateDirectoryRecursiveAsync(SftpClient client, string path, CancellationToken cancellationToken)
    {
        var parts = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var currentPath = string.Empty;

        foreach (var part in parts)
        {
            currentPath = string.IsNullOrEmpty(currentPath)
                ? $"/{part}"
                : $"{currentPath}/{part}";

            try
            {
                if (!await client.ExistsAsync(currentPath, cancellationToken).ConfigureAwait(false))
                    await client.CreateDirectoryAsync(currentPath, cancellationToken).ConfigureAwait(false);
            }
            catch (SshException ex)
            {
                if (ex is SftpPermissionDeniedException ||
                    ex.Message.Contains("permission", StringComparison.OrdinalIgnoreCase) ||
                    ex.Message.Contains("denied", StringComparison.OrdinalIgnoreCase))
                {
                    throw new UnauthorizedAccessException(
                        $"Permission denied creating SFTP directory '{currentPath}': {ex.Message}",
                        ex);
                }

                // Any other failure is tolerated: the directory may exist from a race. The Open that follows fails
                // with a clear error if the directory truly does not exist.
            }
        }
    }
}

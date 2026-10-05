using Renci.SshNet;
using Renci.SshNet.Common;

namespace NPipeline.StorageProviders.Sftp;

/// <summary>
///     A write stream backed by an SSH.NET <c>SftpFileStream</c>, opened with <see cref="FileMode.Create" /> so an existing file is truncated.
///     Holds a connection lease from <see cref="SftpClientPool" /> for its lifetime;
///     the lease is returned to the pool when the stream is disposed.
/// </summary>
/// <remarks>
///     Do NOT buffer the entire payload in a <see cref="MemoryStream" /> - that would cause
///     OOM on large files. SSH.NET streams data over the wire as each Write/WriteAsync call is made.
/// </remarks>
public sealed class SftpWriteStream : Stream
{
    private readonly IPooledConnection _lease;
    private readonly Stream _sftpStream;
    private bool _disposed;

    private SftpWriteStream(IPooledConnection lease, Stream sftpStream)
    {
        _lease = lease ?? throw new ArgumentNullException(nameof(lease));
        _sftpStream = sftpStream ?? throw new ArgumentNullException(nameof(sftpStream));
    }

    /// <summary>
    ///     Opens the remote file for writing, truncating an existing file. The returned stream owns <paramref name="lease" />;
    ///     if opening fails, the caller keeps it.
    /// </summary>
    /// <param name="lease">The pooled connection lease.</param>
    /// <param name="remotePath">The remote file path.</param>
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

        // FileMode.Create truncates; SftpClient.OpenWrite opens with OpenOrCreate, which leaves the old file's tail after shorter content.
        var stream = await lease.Client.OpenAsync(remotePath, FileMode.Create, FileAccess.Write, cancellationToken).ConfigureAwait(false);

        return new SftpWriteStream(lease, stream);
    }

    /// <inheritdoc />
    public override bool CanRead => false;

    /// <inheritdoc />
    public override bool CanSeek => false;

    /// <inheritdoc />
    public override bool CanWrite => !_disposed && _sftpStream.CanWrite;

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
    public override void Write(byte[] buffer, int offset, int count)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _sftpStream.Write(buffer, offset, count);
    }

    /// <inheritdoc />
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _sftpStream.Write(buffer);
    }

    /// <inheritdoc />
    public override Task WriteAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _sftpStream.WriteAsync(buffer, offset, count, cancellationToken);
    }

    /// <inheritdoc />
    public override ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
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
        _sftpStream.Flush();
    }

    /// <inheritdoc />
    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _sftpStream.FlushAsync(cancellationToken);
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

using Renci.SshNet;
using Renci.SshNet.Common;

namespace NPipeline.StorageProviders.Sftp;

/// <summary>
///     A write stream backed by SSH.NET's native <c>SftpClient.OpenWrite()</c>.
///     Holds a connection lease from <see cref="SftpClientPool" /> for its lifetime;
///     the lease is returned to the pool when the stream is disposed.
/// </summary>
/// <remarks>
///     Do NOT buffer the entire payload in a <see cref="MemoryStream" /> — that would cause
///     OOM on large files. SSH.NET streams data over the wire as each Write/WriteAsync call is made.
/// </remarks>
public sealed class SftpWriteStream : Stream
{
    private readonly IPooledConnection _lease;
    private readonly Stream _sftpStream;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SftpWriteStream" /> class.
    /// </summary>
    /// <param name="lease">The pooled connection lease.</param>
    /// <param name="remotePath">The remote file path.</param>
    /// <param name="createDirectory">Whether to create the parent directory if it doesn't exist.</param>
    internal SftpWriteStream(
        IPooledConnection lease,
        string remotePath,
        bool createDirectory = true)
    {
        _lease = lease ?? throw new ArgumentNullException(nameof(lease));

        if (string.IsNullOrWhiteSpace(remotePath))
            throw new ArgumentException("Remote path cannot be null or whitespace.", nameof(remotePath));

        // Ensure parent directory exists
        if (createDirectory)
            EnsureParentDirectoryExists(lease.Client, remotePath);

        _sftpStream = lease.Client.OpenWrite(remotePath);
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
    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override int Read(Span<byte> buffer)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override Task<int> ReadAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
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

    private static void EnsureParentDirectoryExists(SftpClient client, string remotePath)
    {
        var parentPath = GetParentPath(remotePath);

        if (string.IsNullOrEmpty(parentPath))
            return;

        // Check if directory exists
        if (!client.Exists(parentPath))
        {
            // Create directory recursively
            CreateDirectoryRecursive(client, parentPath);
        }
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

    private static void CreateDirectoryRecursive(SftpClient client, string path)
    {
        var parts = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var currentPath = string.Empty;
        List<string>? failedPaths = null;

        foreach (var part in parts)
        {
            currentPath = string.IsNullOrEmpty(currentPath)
                ? $"/{part}"
                : $"{currentPath}/{part}";

            try
            {
                if (!client.Exists(currentPath))
                    client.CreateDirectory(currentPath);
            }
            catch (SshException ex)
            {
                // Track failures but continue - directory might exist from race condition
                // or we might not have permission. The subsequent OpenWrite will fail with
                // a clear error if the directory truly doesn't exist.
                failedPaths ??= [];
                failedPaths.Add(currentPath);

                // If this is a permission error, we want to know about it
                if (ex.Message.Contains("permission", StringComparison.OrdinalIgnoreCase) ||
                    ex.Message.Contains("denied", StringComparison.OrdinalIgnoreCase))
                {
                    throw new UnauthorizedAccessException(
                        $"Permission denied creating SFTP directory '{currentPath}': {ex.Message}",
                        ex);
                }
            }
        }
    }
}

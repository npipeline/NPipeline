using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders;

/// <summary>
///     Writes to a hidden sibling of the target and moves it into place in <see cref="CommitAsync" />, so readers see
///     the old file or the whole new file. Disposing without committing deletes the temporary file.
/// </summary>
internal sealed class FileSystemWriteStream : StorageWriteStream
{
    private readonly FileStream _stream;
    private readonly string _targetPath;
    private readonly string _tempPath;
    private readonly bool _overwrite;
    private bool _committed;
    private bool _disposed;

    public FileSystemWriteStream(string targetPath, bool overwrite)
    {
        _targetPath = targetPath;
        _overwrite = overwrite;

        var directory = Path.GetDirectoryName(targetPath);

        if (!string.IsNullOrEmpty(directory))
            _ = Directory.CreateDirectory(directory);

        _tempPath = Path.Combine(directory ?? string.Empty, $".{Path.GetFileName(targetPath)}.{Guid.NewGuid():N}.tmp");

        _stream = new FileStream(
            _tempPath,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            81920,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
    }

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => !_disposed && !_committed;

    public override long Length => _stream.Length;

    public override long Position
    {
        get => _stream.Position;
        set => throw new NotSupportedException();
    }

    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_committed)
            throw new InvalidOperationException("The stream has already been committed.");

        cancellationToken.ThrowIfCancellationRequested();
        await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        await _stream.DisposeAsync().ConfigureAwait(false);

        File.Move(_tempPath, _targetPath, _overwrite);
        _committed = true;
    }

    public override void Flush() => _stream.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => _stream.FlushAsync(cancellationToken);

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => _stream.Write(buffer, offset, count);

    public override void Write(ReadOnlySpan<byte> buffer) => _stream.Write(buffer);

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        _stream.WriteAsync(buffer, offset, count, cancellationToken);

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
        _stream.WriteAsync(buffer, cancellationToken);

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_disposed)
        {
            _disposed = true;
            _stream.Dispose();
            DeleteTempFile();
        }

        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await _stream.DisposeAsync().ConfigureAwait(false);
            DeleteTempFile();
        }

        await base.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private void DeleteTempFile()
    {
        if (_committed)
            return;

        try
        {
            File.Delete(_tempPath);
        }
        catch (IOException)
        {
            // Best effort: the temporary file is hidden and the target was never touched.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}

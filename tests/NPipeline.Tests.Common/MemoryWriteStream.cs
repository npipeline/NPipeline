using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Tests.Common;

/// <summary>
///     A <see cref="StorageWriteStream" /> over memory for test providers: <paramref name="commit" /> receives the bytes
///     when <see cref="CommitAsync" /> runs and returns the new ETag, and disposing without committing discards them.
/// </summary>
/// <param name="commit">Stores the written bytes, and returns the ETag of the stored object.</param>
/// <param name="forwardOnly">Reports <c>CanSeek = false</c> and refuses <c>Length</c>, like an object-store upload stream.</param>
public sealed class MemoryWriteStream(Func<byte[], string?> commit, bool forwardOnly = false) : StorageWriteStream
{
    private readonly MemoryStream _buffer = new();
    private bool _committed;

    public override bool CanRead => false;

    public override bool CanSeek => !forwardOnly;

    public override bool CanWrite => !_committed;

    public override long Length => forwardOnly ? throw new NotSupportedException() : _buffer.Length;

    public override long Position
    {
        get => forwardOnly ? throw new NotSupportedException() : _buffer.Position;
        set => throw new NotSupportedException();
    }

    public override Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (_committed)
            throw new InvalidOperationException("The stream has already been committed.");

        cancellationToken.ThrowIfCancellationRequested();
        ETag = commit(_buffer.ToArray());
        _committed = true;
        return Task.CompletedTask;
    }

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => _buffer.Write(buffer, offset, count);

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _buffer.Dispose();

        base.Dispose(disposing);
    }
}

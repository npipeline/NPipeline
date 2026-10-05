using System.Net.Http;
using NResilience;

namespace NPipeline.StorageProviders.Gcp;

/// <summary>
///     A forward-only read stream over a GCS object that survives a connection failure mid-read. When a read fails with
///     a transient error it reopens the object at the current offset (<c>Range: bytes={position}-</c>), pinned to the
///     generation of the first response so the bytes it appends come from the same object version. The number of
///     consecutive failed resumes without progress is bounded by the provider's <see cref="Resilience" />.
/// </summary>
internal sealed class GcsResumingReadStream : Stream
{
    private readonly Func<long, long?, CancellationToken, Task<Opened>> _open;
    private readonly Resilience _resilience;

    private Opened _current;
    private long? _end;
    private bool _eof;
    private bool _isDisposed;
    private long _position;

    /// <summary>Creates the stream over an already opened first response.</summary>
    /// <param name="first">The first response, positioned at offset 0.</param>
    /// <param name="open">Reopens the object at a position, pinned to a generation. It applies the retry policy itself.</param>
    /// <param name="resilience">Supplies the transient/permanent classification and the resume budget.</param>
    public GcsResumingReadStream(Opened first, Func<long, long?, CancellationToken, Task<Opened>> open, Resilience resilience)
    {
        _current = first;
        _open = open;
        _resilience = resilience;
        _end = first.Length;
    }

    public override bool CanRead => !_isDisposed;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        var scratch = new byte[buffer.Length];
        var read = ReadAsync(scratch.AsMemory()).AsTask().GetAwaiter().GetResult();
        scratch.AsSpan(0, read).CopyTo(buffer);
        return read;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);

        if (_eof || buffer.IsEmpty)
            return 0;

        // A resume that makes no progress counts against the budget; a read that returns data resets it.
        var failures = 0;
        var maxResumes = Math.Max(_resilience.Attempts - 1, 0);

        while (true)
        {
            try
            {
                var read = await _current.Content.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);

                if (read == 0)
                {
                    // The server closed the body early: the connection dropped without an error.
                    if (_end is { } end && _position < end)
                        throw new IOException($"The GCS response ended after {_position} of {end} bytes.");

                    _eof = true;
                    return 0;
                }

                _position += read;
                return read;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested && failures < maxResumes && IsTransient(ex))
            {
                failures++;
                await ReopenAsync(cancellationToken).ConfigureAwait(false);

                if (_eof)
                    return 0;
            }
        }
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_isDisposed)
        {
            _isDisposed = true;
            _current.Dispose();
        }

        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        Dispose(true);
        await base.DisposeAsync().ConfigureAwait(false);
    }

    private bool IsTransient(Exception exception) =>
        _resilience.Classifier.ClassifyException(exception).Kind is VerdictKind.Transient or VerdictKind.Throttled;

    private async Task ReopenAsync(CancellationToken cancellationToken)
    {
        _current.Dispose();

        if (_end is { } end && _position >= end)
        {
            // Every byte arrived before the failure; there is nothing to request.
            _eof = true;
            return;
        }

        var reopened = await _open(_position, _current.Generation, cancellationToken).ConfigureAwait(false);
        _current = reopened;
        _end = reopened.Length is { } length ? _position + length : null;
    }

    /// <summary>One open response: its body, the object generation, and how many bytes it is expected to carry.</summary>
    internal readonly record struct Opened(Stream Content, HttpResponseMessage? Response, long? Generation, long? Length)
    {
        public void Dispose()
        {
            Content.Dispose();
            Response?.Dispose();
        }
    }
}

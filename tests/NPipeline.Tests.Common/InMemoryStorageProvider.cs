using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Tests.Common;

/// <summary>
///     An in-memory <see cref="IStorageProvider" /> for connector tests and benchmarks. Objects live in a dictionary
///     keyed by host and path, and a written object becomes visible when its stream is disposed, like an object-store
///     PUT. Streams can be made non-seekable to reproduce S3, Azure Blob and HTTP response streams.
/// </summary>
public sealed class InMemoryStorageProvider : StorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> SupportedSchemes = [new StorageScheme("mem")];

    private readonly ConcurrentDictionary<string, byte[]> _objects = new(StringComparer.Ordinal);

    public override string Name => "In-memory";

    public override IReadOnlyList<StorageScheme> Schemes => SupportedSchemes;

    public override StorageCapabilities Capabilities =>
        StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete;

    /// <summary>Read streams report <c>CanSeek = false</c>, like an object-store download stream.</summary>
    public bool NonSeekableReads { get; init; }

    /// <summary>Write streams are write-only and report <c>CanSeek = false</c>, like an object-store upload stream.</summary>
    public bool NonSeekableWrites { get; init; }

    /// <summary>Creates a <c>mem://</c> URI for <paramref name="path" />.</summary>
    public static StorageUri Uri(string path) => StorageUri.Parse($"mem://test/{path.TrimStart('/')}");

    protected override Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        if (!_objects.TryGetValue(Key(uri), out var bytes))
            throw new FileNotFoundException($"No in-memory object at '{uri}'.");

        Stream stream = new MemoryStream(bytes, false);

        if (NonSeekableReads)
            stream = new ForwardOnlyStream(stream, false);

        return Task.FromResult(stream);
    }

    protected override Task<StorageWriteStream> OpenWriteCoreAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken)
    {
        WriteRequests.Enqueue(uri);
        Stream stream = new CommittingStream(bytes => _objects[Key(uri)] = bytes);

        if (NonSeekableWrites)
            stream = new ForwardOnlyStream(stream, true);

        return Task.FromResult<StorageWriteStream>(new PassThroughWriteStream(stream));
    }

    protected override Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken cancellationToken) =>
        Task.FromResult<StorageMetadata?>(
            _objects.TryGetValue(Key(uri), out var bytes)
                ? new StorageMetadata { Size = bytes.LongLength }
                : null);

    protected override Task DeleteCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        _ = _objects.TryRemove(Key(uri), out _);
        return Task.CompletedTask;
    }

    protected override async IAsyncEnumerable<StorageItem> ListCoreAsync(
        StorageUri directory,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var prefixKey = Key(directory).TrimEnd('/') + "/";

        foreach (var (key, bytes) in _objects.OrderBy(o => o.Key, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!key.StartsWith(prefixKey, StringComparison.Ordinal))
                continue;

            if (!recursive && key.AsSpan(prefixKey.Length).Contains('/'))
                continue;

            yield return new StorageItem
            {
                Uri = directory.WithPath(key[key.IndexOf('/')..]),
                Size = bytes.LongLength,
                LastModified = DateTimeOffset.UnixEpoch,
            };
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <summary>Every URI passed to <see cref="StorageProvider.OpenWriteAsync" />, in order, including parameters the key ignores.</summary>
    public ConcurrentQueue<StorageUri> WriteRequests { get; } = new();

    /// <summary>Stores <paramref name="bytes" /> at <paramref name="uri" />, as if written by another tool.</summary>
    public void Put(StorageUri uri, byte[] bytes) => _objects[Key(uri)] = bytes;

    /// <summary>Returns the bytes stored at <paramref name="uri" />.</summary>
    public byte[] Get(StorageUri uri) => _objects.TryGetValue(Key(uri), out var bytes)
        ? bytes
        : throw new FileNotFoundException($"No in-memory object at '{uri}'.");

    /// <summary>The keys of every stored object, for asserting that no temporary files were left behind.</summary>
    public IReadOnlyCollection<string> Keys => [.. _objects.Keys];

    private static string Key(StorageUri uri) => $"{uri.Host}{uri.Path}";

    private sealed class CommittingStream(Action<byte[]> commit) : MemoryStream
    {
        private bool _committed;

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_committed)
            {
                _committed = true;
                commit(ToArray());
            }

            base.Dispose(disposing);
        }
    }

    /// <summary>Hides seeking (and reading, for writes) from the caller while delegating to an inner stream.</summary>
    private sealed class ForwardOnlyStream(Stream inner, bool writeOnly) : Stream
    {
        public override bool CanRead => !writeOnly && inner.CanRead;

        public override bool CanSeek => false;

        public override bool CanWrite => writeOnly && inner.CanWrite;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() => inner.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

        public override int Read(byte[] buffer, int offset, int count) =>
            CanRead
                ? inner.Read(buffer, offset, count)
                : throw new NotSupportedException();

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            CanRead
                ? inner.ReadAsync(buffer, cancellationToken)
                : throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (!CanWrite)
                throw new NotSupportedException();

            inner.Write(buffer, offset, count);
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
            CanWrite
                ? inner.WriteAsync(buffer, cancellationToken)
                : throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();

            base.Dispose(disposing);
        }
    }
}

using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Files;

/// <summary>The plumbing shared by <see cref="FileSourceNode{T}" /> and <see cref="FileSinkNode{T}" />.</summary>
internal static class FileNodeSupport
{
    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => StorageProviderFactory.CreateResolver(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    public static IStorageProvider ResolveProvider(FileNodeOptions options, bool write)
    {
        var provider = options.Provider ?? StorageProviderFactory.GetProviderOrThrow(options.Resolver ?? DefaultResolver.Value, options.Uri);

        if (provider is IStorageProviderMetadataProvider metadataProvider)
        {
            var metadata = metadataProvider.GetMetadata();

            if (write ? !metadata.SupportsWrite : !metadata.SupportsRead)
                throw new UnsupportedStorageCapabilityException(options.Uri, write ? "write" : "read", metadata.Name);
        }

        return provider;
    }

    public static string StreamName(Type nodeType, Type itemType)
    {
        var name = nodeType.Name;
        var tick = name.IndexOf('`', StringComparison.Ordinal);
        return $"{(tick < 0 ? name : name[..tick])}<{itemType.Name}>";
    }

    /// <summary>A file, the files in a directory (a path ending in <c>/</c>), or the files matching a glob (<c>*</c>, <c>**</c>), in ordinal path order.</summary>
    public static async Task<IReadOnlyList<StorageUri>> ExpandAsync(
        IStorageProvider provider,
        StorageUri uri,
        bool recursive,
        IReadOnlyList<string> directoryExtensions,
        CancellationToken cancellationToken)
    {
        var path = uri.Path;
        var wildcard = path.IndexOf('*', StringComparison.Ordinal);

        if (wildcard < 0 && !path.EndsWith('/'))
            return [uri];

        Regex? pattern = null;
        var listUri = uri;

        if (wildcard >= 0)
        {
            var prefixEnd = path.LastIndexOf('/', wildcard);
            listUri = uri.WithPath(path[..(prefixEnd + 1)]);
            recursive = path.AsSpan(wildcard).Contains('/') || path.Contains("**", StringComparison.Ordinal);
            pattern = GlobToRegex(path);
        }

        var files = new List<StorageUri>();

        try
        {
            await foreach (var item in provider.ListAsync(listUri, recursive, cancellationToken).ConfigureAwait(false))
            {
                if (item.IsDirectory)
                    continue;

                var itemPath = item.Uri.Path;
                var matches = pattern?.IsMatch(itemPath)
                              ?? (directoryExtensions.Count == 0 || directoryExtensions.Any(e => itemPath.EndsWith(e, StringComparison.OrdinalIgnoreCase)));

                // The listed URI can lack the original's parameters (credentials, region), so keep everything but the path.
                if (matches)
                    files.Add(uri.WithPath(itemPath));
            }
        }
        catch (NotSupportedException ex)
        {
            throw new NotSupportedException(
                $"'{uri}' is a directory or glob, but {provider.GetType().Name} cannot list objects.", ex);
        }

        files.Sort(static (a, b) => string.CompareOrdinal(a.Path, b.Path));
        return files;
    }

    /// <summary>
    ///     Translates a glob into an anchored regular expression: <c>**</c> spans directories and <c>*</c> does not. There is
    ///     no <c>?</c> wildcard, because <c>?</c> starts a URI's query string.
    /// </summary>
    public static Regex GlobToRegex(string glob)
    {
        var pattern = new StringBuilder("^");

        for (var i = 0; i < glob.Length; i++)
        {
            var c = glob[i];

            if (c == '*' && i + 1 < glob.Length && glob[i + 1] == '*')
            {
                var slashFollows = i + 2 < glob.Length && glob[i + 2] == '/';
                _ = pattern.Append(slashFollows ? "(?:.*/)?" : ".*");
                i += slashFollows ? 2 : 1;
            }
            else if (c == '*')
                _ = pattern.Append("[^/]*");
            else
                _ = pattern.Append(Regex.Escape(c.ToString()));
        }

        return new Regex(pattern.Append('$').ToString(), RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
    }

    public static FileCompression ResolveCompression(FileCompression compression, string path, bool supported, string connector)
    {
        if (compression == FileCompression.Auto)
        {
            compression = path switch
            {
                _ when path.EndsWith(".gz", StringComparison.OrdinalIgnoreCase) => FileCompression.Gzip,
                _ when path.EndsWith(".br", StringComparison.OrdinalIgnoreCase) => FileCompression.Brotli,
                _ when path.EndsWith(".zz", StringComparison.OrdinalIgnoreCase) || path.EndsWith(".zlib", StringComparison.OrdinalIgnoreCase) => FileCompression.ZLib,
                _ when path.EndsWith(".deflate", StringComparison.OrdinalIgnoreCase) => FileCompression.Deflate,
                _ => FileCompression.None,
            };

            // A format with its own compression (Parquet, XLSX) is never wrapped by suffix.
            if (!supported)
                return FileCompression.None;
        }

        if (compression != FileCompression.None && !supported)
            throw new NotSupportedException($"The {connector} connector compresses its own files and does not support {compression} stream compression.");

        return compression;
    }

    public static Stream Decompress(Stream stream, FileCompression compression) =>
        compression switch
        {
            FileCompression.Gzip => new GZipStream(stream, CompressionMode.Decompress, true),
            FileCompression.Brotli => new BrotliStream(stream, CompressionMode.Decompress, true),
            FileCompression.ZLib => new ZLibStream(stream, CompressionMode.Decompress, true),
            FileCompression.Deflate => new DeflateStream(stream, CompressionMode.Decompress, true),
            _ => stream,
        };

    public static Stream Compress(Stream stream, FileCompression compression) =>
        compression switch
        {
            FileCompression.Gzip => new GZipStream(stream, CompressionLevel.Optimal, true),
            FileCompression.Brotli => new BrotliStream(stream, CompressionLevel.Optimal, true),
            FileCompression.ZLib => new ZLibStream(stream, CompressionLevel.Optimal, true),
            FileCompression.Deflate => new DeflateStream(stream, CompressionLevel.Optimal, true),
            _ => stream,
        };

    /// <summary>A read-write temporary file that deletes itself when disposed, for formats that need a seekable stream.</summary>
    public static FileStream CreateSpoolFile(int bufferSize) =>
        new(
            Path.Combine(Path.GetTempPath(), $"npipeline-{Guid.NewGuid():N}.spool"),
            FileMode.CreateNew,
            FileAccess.ReadWrite,
            FileShare.None,
            bufferSize,
            FileOptions.DeleteOnClose | FileOptions.Asynchronous);

    public static StorageUri TemporaryUri(StorageUri target) =>
        // Everything but the path is kept: parameters, port and user info select the account, region or endpoint.
        target.WithPath($"{target.Path}.tmp-{Guid.NewGuid():N}");

    public static async Task TryDeleteAsync(IStorageProvider provider, StorageUri uri)
    {
        if (provider is not IDeletableStorageProvider deletable)
            return;

        try
        {
            // CancellationToken.None: cleanup runs because the operation failed, which is often a cancellation.
            await deletable.DeleteAsync(uri, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Best effort: the original failure is the one to report.
        }
    }
}

/// <summary>Disposes the streams it holds in reverse order, so wrappers flush into the streams they wrap.</summary>
internal sealed class StreamChain : IAsyncDisposable
{
    private readonly Stack<Stream> _streams = new();

    public T Push<T>(T stream)
        where T : Stream
    {
        _streams.Push(stream);
        return stream;
    }

    public async ValueTask DisposeAsync()
    {
        List<Exception>? failures = null;

        while (_streams.TryPop(out var stream))
        {
            try
            {
                await stream.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                (failures ??= []).Add(ex);
            }
        }

        if (failures is not null)
            throw failures.Count == 1 ? failures[0] : new AggregateException(failures);
    }
}

/// <summary>Counts the bytes read from or written to the stream it wraps, and leaves that stream open.</summary>
internal sealed class CountingStream(Stream inner) : Stream
{
    public long Bytes { get; private set; }

    public override bool CanRead => inner.CanRead;

    public override bool CanSeek => inner.CanSeek;

    public override bool CanWrite => inner.CanWrite;

    public override long Length => inner.Length;

    public override long Position
    {
        get => inner.Position;
        set => inner.Position = value;
    }

    public override void Flush() => inner.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

    public override int Read(byte[] buffer, int offset, int count) => Count(inner.Read(buffer, offset, count));

    public override int Read(Span<byte> buffer) => Count(inner.Read(buffer));

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        Count(await inner.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false));

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        Count(await inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false));

    public override void Write(byte[] buffer, int offset, int count)
    {
        inner.Write(buffer, offset, count);
        Bytes += count;
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        inner.Write(buffer);
        Bytes += buffer.Length;
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await inner.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
        Bytes += buffer.Length;
    }

    public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

    public override void SetLength(long value) => inner.SetLength(value);

    private int Count(int read)
    {
        Bytes += read;
        return read;
    }
}

/// <summary>Counts the items passing through, and applies a sink's null-item policy.</summary>
internal sealed class ItemCounter
{
    public long Count { get; private set; }

    public async IAsyncEnumerable<T> Pass<T>(
        IAsyncEnumerable<T> items,
        NullItemHandling nullItems,
        string nodeName,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (item is null)
            {
                switch (nullItems)
                {
                    case NullItemHandling.Skip:
                        continue;
                    case NullItemHandling.Throw:
                        throw new InvalidOperationException(
                            $"{nodeName} received a null item at position {Count + 1}. Set NullItems to Skip to drop null items.");
                }
            }

            Count++;
            yield return item;
        }
    }
}

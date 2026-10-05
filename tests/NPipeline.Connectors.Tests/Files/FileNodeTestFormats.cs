using System.Runtime.CompilerServices;
using System.Text;
using NPipeline.Connectors.Files;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.Tests.Files;

public sealed record LineSourceOptions : FileSourceOptions;

public sealed record LineSinkOptions : FileSinkOptions;

/// <summary>A minimal text format: one record per line. A line reading "bad" fails to map.</summary>
public sealed class LineSource(LineSourceOptions options) : FileSourceNode<string>(options)
{
    public bool NeedsSeekableStream { get; init; }

    public bool Compressible { get; init; } = true;

    public List<bool> SeekableStreamsSeen { get; } = [];

    protected override string ConnectorName => "lines";

    protected override IReadOnlyList<string> DirectoryFileExtensions => [".txt"];

    protected override bool RequiresSeekableStream => NeedsSeekableStream;

    protected override bool SupportsCompression => Compressible;

    protected override async IAsyncEnumerable<string> ReadAsync(Stream stream, FileReadContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        SeekableStreamsSeen.Add(stream.CanSeek);
        using var reader = new StreamReader(stream, Encoding.UTF8, false, context.BufferSize, true);
        long recordNumber = 0;

        while (await reader.ReadLineAsync(cancellationToken) is { } line)
        {
            recordNumber++;

            if (line == "bad")
            {
                await context.HandleRowErrorAsync(recordNumber, new FormatException("bad line"), line, cancellationToken);
                continue;
            }

            yield return line;
        }
    }
}

/// <summary>Writes one line per record. Throws on "explode", to test cleanup.</summary>
public sealed class LineSink(LineSinkOptions options) : FileSinkNode<string?>(options)
{
    public bool NeedsSeekableStream { get; init; }

    public bool Compressible { get; init; } = true;

    public bool CanWriteNulls { get; init; }

    public bool? SawSeekableStream { get; private set; }

    protected override string ConnectorName => "lines";

    protected override bool RequiresSeekableStream => NeedsSeekableStream;

    protected override bool SupportsCompression => Compressible;

    protected override bool SupportsNullItems => CanWriteNulls;

    protected override async Task WriteAsync(Stream stream, IAsyncEnumerable<string?> items, FileWriteContext context, CancellationToken cancellationToken)
    {
        SawSeekableStream = stream.CanSeek && stream.CanRead;
        var writer = new StreamWriter(stream, new UTF8Encoding(false), context.BufferSize, true);

        await using (writer)
        {
            await foreach (var item in items.WithCancellation(cancellationToken))
            {
                if (item == "explode")
                    throw new InvalidOperationException("boom");

                await writer.WriteLineAsync(item ?? "<null>");
            }
        }
    }
}

/// <summary>An in-memory provider that can move objects (like the file system) and records every URI it opens.</summary>
public sealed class MoveableProvider(InMemoryStorageProvider inner) : StorageProvider
{
    public List<StorageUri> Reads { get; } = [];

    public List<(StorageUri From, StorageUri To)> Moves { get; } = [];

    public override string Name => "Moveable in-memory";

    public override IReadOnlyList<StorageScheme> Schemes => inner.Schemes;

    public override StorageCapabilities Capabilities => inner.Capabilities | StorageCapabilities.Move | StorageCapabilities.AtomicMove;

    protected override Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        Reads.Add(uri);
        return inner.OpenReadAsync(uri, cancellationToken);
    }

    protected override Task<StorageWriteStream> OpenWriteCoreAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken) =>
        inner.OpenWriteAsync(uri, options, cancellationToken);

    protected override Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken cancellationToken) =>
        inner.GetMetadataAsync(uri, cancellationToken);

    protected override IAsyncEnumerable<StorageItem> ListCoreAsync(StorageUri directory, bool recursive, CancellationToken cancellationToken) =>
        inner.ListAsync(directory, recursive, cancellationToken);

    protected override Task DeleteCoreAsync(StorageUri uri, CancellationToken cancellationToken) => inner.DeleteAsync(uri, cancellationToken);

    protected override async Task MoveCoreAsync(StorageUri source, StorageUri destination, CancellationToken cancellationToken)
    {
        Moves.Add((source, destination));
        inner.Put(destination, inner.Get(source));
        await inner.DeleteAsync(source, cancellationToken);
    }
}

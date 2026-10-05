using System.Diagnostics;
using NPipeline.Connectors.Diagnostics;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Files;

/// <summary>What a <see cref="FileSinkNode{T}" /> gives the format for the file it writes.</summary>
public sealed class FileWriteContext
{
    internal FileWriteContext(StorageUri uri, int bufferSize)
    {
        Uri = uri;
        BufferSize = bufferSize;
    }

    /// <summary>The file being written (the final target, even when writing through a temporary object).</summary>
    public StorageUri Uri { get; }

    /// <summary>The buffer size for the format's writer.</summary>
    public int BufferSize { get; }
}

/// <summary>
///     A sink that writes records to a file through a storage provider. The base resolves the provider, chooses between a
///     direct and an atomic write, compresses, gives formats that need one a seekable stream, applies the null-item
///     policy, cleans up after failures and reports metrics; the format implements <see cref="WriteAsync" />.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public abstract class FileSinkNode<T> : SinkNode<T>
{
    /// <summary>Creates the sink and validates <paramref name="options" />.</summary>
    protected FileSinkNode(FileSinkOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        Options = options;
    }

    /// <summary>The sink's options.</summary>
    protected FileSinkOptions Options { get; }

    /// <summary>The connector's name in metrics and traces, such as <c>csv</c>.</summary>
    protected abstract string ConnectorName { get; }

    /// <summary>Whether the format needs a seekable, readable stream (a zip). The format then writes to a temporary file, which is copied to storage.</summary>
    protected virtual bool RequiresSeekableStream => false;

    /// <summary>Whether the format's files can be stream-compressed. Formats with their own compression return <c>false</c>.</summary>
    protected virtual bool SupportsCompression => true;

    /// <summary>Whether the format can write a <c>null</c> item, which <see cref="NullItemHandling.Write" /> requires.</summary>
    protected virtual bool SupportsNullItems => false;

    /// <inheritdoc />
    public sealed override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);

        if (Options.NullItems == NullItemHandling.Write && !SupportsNullItems)
            throw new NotSupportedException($"The {ConnectorName} connector cannot write null items; use NullItemHandling.Throw or Skip.");

        var provider = FileNodeSupport.ResolveProvider(Options, true);
        var target = Options.Uri;
        var compression = FileNodeSupport.ResolveCompression(Options.Compression, target.Path, SupportsCompression, ConnectorName);

        var viaTemporary = Options.AtomicWrite switch
        {
            AtomicWrite.Always => true,
            AtomicWrite.Never => false,
            _ => provider.Capabilities.HasFlag(StorageCapabilities.AtomicMove),
        };

        var writeUri = viaTemporary ? FileNodeSupport.TemporaryUri(target) : target;
        using var activity = ConnectorDiagnostics.StartFileActivity("connector.file.write", ConnectorName, target);
        var counter = new ItemCounter();
        long bytes;

        try
        {
            bytes = await WriteFileAsync(provider, writeUri, compression, counter.Pass(input, Options.NullItems, GetType().Name, cancellationToken), cancellationToken)
                .ConfigureAwait(false);

            if (viaTemporary)
                await PublishAsync(provider, writeUri, target, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (viaTemporary || Options.DeletePartialOnFailure)
                await FileNodeSupport.TryDeleteAsync(provider, writeUri).ConfigureAwait(false);

            _ = activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }

        ConnectorDiagnostics.RecordFileWritten(ConnectorName, target, counter.Count, bytes);
        _ = activity?.SetTag("npipeline.connector.rows", counter.Count);
    }

    /// <summary>Writes every record to <paramref name="stream" />. Flush what the format buffers; the base disposes the stream.</summary>
    /// <param name="stream">Where to write: compressed as configured, and seekable when <see cref="RequiresSeekableStream" /> is set.</param>
    /// <param name="items">The records, after the null-item policy.</param>
    /// <param name="context">The file being written.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    protected abstract Task WriteAsync(Stream stream, IAsyncEnumerable<T> items, FileWriteContext context, CancellationToken cancellationToken);

    private async Task<long> WriteFileAsync(
        IStorageProvider provider,
        StorageUri writeUri,
        FileCompression compression,
        IAsyncEnumerable<T> items,
        CancellationToken cancellationToken)
    {
        var context = new FileWriteContext(Options.Uri, Options.BufferSize);
        CountingStream counted;

        // Disposed in reverse: the spool, then the compressor (writing its trailer), then the provider's stream (committing it).
        var chain = new StreamChain();

        await using (chain.ConfigureAwait(false))
        {
            var raw = chain.Push(await provider.OpenWriteAsync(writeUri, null, cancellationToken).ConfigureAwait(false));
            counted = chain.Push(new CountingStream(raw));
            var output = chain.Push(FileNodeSupport.Compress(counted, compression));

            if (RequiresSeekableStream && !(output.CanSeek && output.CanRead))
            {
                var spool = chain.Push(FileNodeSupport.CreateSpoolFile(Options.BufferSize));
                await WriteAsync(spool, items, context, cancellationToken).ConfigureAwait(false);
                await spool.FlushAsync(cancellationToken).ConfigureAwait(false);
                spool.Position = 0;
                await spool.CopyToAsync(output, Options.BufferSize, cancellationToken).ConfigureAwait(false);
            }
            else
                await WriteAsync(output, items, context, cancellationToken).ConfigureAwait(false);

            await output.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        return counted.Bytes;
    }

    private async Task PublishAsync(IStorageProvider provider, StorageUri temporary, StorageUri target, CancellationToken cancellationToken)
    {
        if (provider.Capabilities.HasFlag(StorageCapabilities.Move))
        {
            await provider.MoveAsync(temporary, target, cancellationToken).ConfigureAwait(false);
            return;
        }

        // AtomicWrite.Always on a provider that cannot move: copy into place, then remove the temporary object.
        var source = await provider.OpenReadAsync(temporary, cancellationToken).ConfigureAwait(false);

        await using (source.ConfigureAwait(false))
        {
            var destination = await provider.OpenWriteAsync(target, null, cancellationToken).ConfigureAwait(false);

            await using (destination.ConfigureAwait(false))
            {
                await source.CopyToAsync(destination, Options.BufferSize, cancellationToken).ConfigureAwait(false);
            }
        }

        await FileNodeSupport.TryDeleteAsync(provider, temporary).ConfigureAwait(false);
    }
}

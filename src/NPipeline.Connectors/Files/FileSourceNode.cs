using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Errors;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Files;

/// <summary>What a <see cref="FileSourceNode{T}" /> gives the format for one file.</summary>
public sealed class FileReadContext
{
    private readonly string _connector;
    private readonly DeadLetterChannel _deadLetters;
    private readonly FileSourceOptions _options;

    internal FileReadContext(StorageUri uri, FileSourceOptions options, string connector, DeadLetterChannel deadLetters)
    {
        Uri = uri;
        Source = uri.ToString();
        _options = options;
        _connector = connector;
        _deadLetters = deadLetters;
    }

    /// <summary>The file being read.</summary>
    public StorageUri Uri { get; }

    /// <summary>The file as text without its query string, as it appears in errors.</summary>
    public string Source { get; }

    /// <summary>The buffer size for the format's reader.</summary>
    public int BufferSize => _options.BufferSize;

    /// <summary>
    ///     Applies the configured <see cref="RowErrorHandler" /> to a record that failed to map. Returns when the record is to
    ///     be skipped (dropped or dead-lettered); throws <see cref="RecordMappingException" /> when the read is to fail.
    /// </summary>
    /// <param name="recordNumber">The record's 1-based position in the file.</param>
    /// <param name="exception">Why it failed. A <see cref="FieldMappingException" /> supplies the field.</param>
    /// <param name="rawRecord">The raw record or value, if the format has one; it is truncated to the configured excerpt length.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    public ValueTask HandleRowErrorAsync(long recordNumber, Exception exception, string? rawRecord = null, CancellationToken cancellationToken = default) =>
        RowErrorDispatch.HandleAsync(
            _options.RowErrorHandler,
            _options.RawExcerptLength,
            _deadLetters,
            _connector,
            Uri.Scheme.Value,
            Source,
            recordNumber,
            exception,
            rawRecord,
            cancellationToken);
}

/// <summary>
///     A source that reads records from one or more files through a storage provider. The base resolves the provider,
///     expands directories and globs, decompresses, makes streams seekable for formats that need it, and reports metrics;
///     the format implements <see cref="ReadAsync" /> for one file.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public abstract class FileSourceNode<T> : SourceNode<T>
{
    /// <summary>Creates the source and validates <paramref name="options" />.</summary>
    protected FileSourceNode(FileSourceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        Options = options;
    }

    /// <summary>The source's options.</summary>
    protected FileSourceOptions Options { get; }

    /// <summary>The connector's name in metrics and traces, such as <c>csv</c>.</summary>
    protected abstract string ConnectorName { get; }

    /// <summary>How many records each file read ahead of the current one may buffer. Defaults to 1,024.</summary>
    protected virtual int ReadAheadBuffer => 1024;

    /// <summary>When <see cref="FileNodeOptions.Uri" /> is a directory, the file suffixes to read (for example <c>.parquet</c>); empty reads every file.</summary>
    protected virtual IReadOnlyList<string> DirectoryFileExtensions => [];

    /// <summary>Whether the format needs a seekable stream (a zip or a footer-first format). Non-seekable streams are spooled to a temporary file first.</summary>
    protected virtual bool RequiresSeekableStream => false;

    /// <summary>Whether the format's files can be stream-compressed (gzip and similar). Formats with their own compression return <c>false</c>.</summary>
    protected virtual bool SupportsCompression => true;

    /// <inheritdoc />
    public sealed override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        var provider = FileNodeSupport.ResolveProvider(Options, false);
        var deadLetters = OpenDeadLetterChannel(context);
        return new DataStream<T>(ReadFilesAsync(provider, deadLetters, cancellationToken), FileNodeSupport.StreamName(GetType(), typeof(T)));
    }

    /// <summary>Reads the records of one file. Report records that fail to map through <see cref="FileReadContext.HandleRowErrorAsync" />.</summary>
    /// <param name="stream">The file's content, decompressed, and seekable when <see cref="RequiresSeekableStream" /> is set. The base disposes it.</param>
    /// <param name="context">The file and the error handling for its records.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    protected abstract IAsyncEnumerable<T> ReadAsync(Stream stream, FileReadContext context, CancellationToken cancellationToken);

    private async IAsyncEnumerable<T> ReadFilesAsync(
        IStorageProvider provider,
        DeadLetterChannel deadLetters,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var files = await FileNodeSupport.ExpandAsync(provider, Options.Uri, Options.Recursive, DirectoryFileExtensions, cancellationToken)
            .ConfigureAwait(false);

        if (Options.FileReadParallelism == 1 || files.Count < 2)
        {
            foreach (var file in files)
            {
                await foreach (var item in ReadFileAsync(provider, file, deadLetters, cancellationToken).ConfigureAwait(false))
                {
                    yield return item;
                }
            }

            yield break;
        }

        await foreach (var item in ReadFilesAheadAsync(provider, files, deadLetters, cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    /// <summary>
    ///     Reads up to <see cref="FileSourceOptions.FileReadParallelism" /> files at once, yielding their records in file
    ///     order. It is a sliding window: file <c>i + p</c> starts only once file <c>i</c> has been drained, so the file being
    ///     consumed is always running and the bounded buffers cannot deadlock.
    /// </summary>
    private async IAsyncEnumerable<T> ReadFilesAheadAsync(
        IStorageProvider provider,
        IReadOnlyList<StorageUri> files,
        DeadLetterChannel deadLetters,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var window = Math.Min(Options.FileReadParallelism, files.Count);
        using var workers = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var running = new Queue<(Channel<T> Channel, Task Worker)>(window);
        var next = 0;

        try
        {
            while (running.Count < window)
            {
                running.Enqueue(StartReadAhead(provider, files[next++], deadLetters, workers.Token));
            }

            while (running.TryPeek(out var current))
            {
                await foreach (var item in current.Channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                {
                    yield return item;
                }

                await current.Worker.ConfigureAwait(false);
                _ = running.Dequeue();

                if (next < files.Count)
                    running.Enqueue(StartReadAhead(provider, files[next++], deadLetters, workers.Token));
            }
        }
        finally
        {
            // The consumer stopped early or a file failed: stop the remaining readers and wait for them, so none is left
            // blocked on a full buffer holding an open stream.
            await workers.CancelAsync().ConfigureAwait(false);

            foreach (var (_, worker) in running)
            {
                await worker.ConfigureAwait(false);
            }
        }
    }

    private (Channel<T> Channel, Task Worker) StartReadAhead(IStorageProvider provider, StorageUri file, DeadLetterChannel deadLetters, CancellationToken cancellationToken)
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(ReadAheadBuffer)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true,
        });

        // The worker never throws: every outcome, including cancellation, completes the channel, so the reader either
        // drains it or observes the error.
        var worker = Task.Run(async () =>
        {
            try
            {
                await foreach (var item in ReadFileAsync(provider, file, deadLetters, cancellationToken).ConfigureAwait(false))
                {
                    await channel.Writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
                }

                _ = channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                _ = channel.Writer.TryComplete(ex);
            }
        }, CancellationToken.None);

        return (channel, worker);
    }

    private async IAsyncEnumerable<T> ReadFileAsync(
        IStorageProvider provider,
        StorageUri file,
        DeadLetterChannel deadLetters,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var compression = FileNodeSupport.ResolveCompression(Options.Compression, file.Path, SupportsCompression, ConnectorName);
        using var activity = ConnectorDiagnostics.StartFileActivity("connector.file.read", ConnectorName, file);
        var chain = new StreamChain();
        await using var chainScope = chain.ConfigureAwait(false);

        var raw = chain.Push(await provider.OpenReadAsync(file, cancellationToken).ConfigureAwait(false));
        var counted = chain.Push(new CountingStream(raw));
        Stream stream = chain.Push(FileNodeSupport.Decompress(counted, compression));

        if (RequiresSeekableStream && !stream.CanSeek)
        {
            var spool = chain.Push(FileNodeSupport.CreateSpoolFile(Options.BufferSize));
            await stream.CopyToAsync(spool, Options.BufferSize, cancellationToken).ConfigureAwait(false);
            spool.Position = 0;
            stream = spool;
        }

        var context = new FileReadContext(file, Options, ConnectorName, deadLetters);
        long rows = 0;

        try
        {
            await foreach (var item in ReadAsync(stream, context, cancellationToken).ConfigureAwait(false))
            {
                rows++;
                yield return item;
            }
        }
        finally
        {
            ConnectorDiagnostics.RecordFileRead(ConnectorName, file, rows, counted.Bytes);
            _ = activity?.SetTag("npipeline.connector.rows", rows);
        }
    }
}

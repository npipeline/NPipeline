using NPipeline.Connectors.Errors;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Files;

/// <summary>How a file node compresses or decompresses its stream.</summary>
public enum FileCompression
{
    /// <summary>By the file's suffix: <c>.gz</c> is gzip, <c>.br</c> Brotli, <c>.zz</c> or <c>.zlib</c> zlib, <c>.deflate</c> raw deflate; anything else is uncompressed.</summary>
    Auto,

    /// <summary>Uncompressed.</summary>
    None,

    /// <summary>gzip.</summary>
    Gzip,

    /// <summary>Brotli.</summary>
    Brotli,

    /// <summary>zlib.</summary>
    ZLib,

    /// <summary>Raw deflate.</summary>
    Deflate,
}

/// <summary>Whether a file sink writes to a temporary object and moves it into place.</summary>
public enum AtomicWrite
{
    /// <summary>
    ///     Only when the provider can rename atomically (<see cref="StorageCapabilities.AtomicMove" />, such as the file
    ///     system, ADLS and SFTP). Object stores write directly: their uploads already become visible all at once, and copying a temporary
    ///     object would double the I/O.
    /// </summary>
    Auto,

    /// <summary>Always; on a provider that cannot move objects (<see cref="StorageCapabilities.Move" />), the temporary object is copied into place and deleted.</summary>
    Always,

    /// <summary>Never; write the target directly.</summary>
    Never,
}

/// <summary>What a file sink does with a <c>null</c> item.</summary>
public enum NullItemHandling
{
    /// <summary>Fail the write.</summary>
    Throw,

    /// <summary>Drop the item.</summary>
    Skip,

    /// <summary>Pass it to the format, which writes its own null (a JSON <c>null</c>, for example). Only formats that support it accept this value.</summary>
    Write,
}

/// <summary>Options shared by file sources and sinks. Connectors derive their own options records from the two below.</summary>
public abstract record FileNodeOptions
{
    /// <summary>The default I/O buffer size, 64 KB.</summary>
    public const int DefaultBufferSize = 64 * 1024;

    /// <summary>
    ///     The file to read or write. A source also accepts a directory (a path ending in <c>/</c>) or a glob: <c>*</c>
    ///     matches within a path segment and <c>**</c> across segments. There is no <c>?</c> wildcard, because <c>?</c>
    ///     starts the URI's query string.
    /// </summary>
    public required StorageUri Uri { get; init; }

    /// <summary>The storage provider. When <c>null</c>, it is resolved from <see cref="Resolver" /> or the default resolver.</summary>
    public IStorageProvider? Provider { get; init; }

    /// <summary>The resolver used when <see cref="Provider" /> is <c>null</c>. When both are <c>null</c>, the default resolver is used.</summary>
    public IStorageResolver? Resolver { get; init; }

    /// <summary>The stream compression. Defaults to <see cref="FileCompression.Auto" />.</summary>
    public FileCompression Compression { get; init; } = FileCompression.Auto;

    /// <summary>The buffer size for the format's reader or writer, in bytes. Defaults to 64 KB.</summary>
    public int BufferSize { get; init; } = DefaultBufferSize;

    /// <summary>Checks the options. Nodes call it from their constructors.</summary>
    /// <exception cref="ArgumentException">An option is invalid.</exception>
    public virtual void Validate()
    {
        ArgumentNullException.ThrowIfNull(Uri, nameof(Uri));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(BufferSize, nameof(BufferSize));
    }
}

/// <summary>Options for a <see cref="FileSourceNode{T}" />.</summary>
public abstract record FileSourceOptions : FileNodeOptions
{
    /// <summary>The default length of <see cref="RowError.RawExcerpt" />, 256 characters.</summary>
    public const int DefaultRawExcerptLength = 256;

    /// <summary>Whether a directory <see cref="FileNodeOptions.Uri" /> includes its subdirectories. Globs with <c>**</c> always recurse.</summary>
    public bool Recursive { get; init; }

    /// <summary>What to do with a record that fails to map. Without a handler, the read fails.</summary>
    public RowErrorHandler? RowErrorHandler { get; init; }

    /// <summary>The longest raw excerpt a <see cref="RowError" /> carries, in characters; <c>0</c> omits it (for sensitive data).</summary>
    public int RawExcerptLength { get; init; } = DefaultRawExcerptLength;

    /// <summary>
    ///     How many files to read at once when <see cref="FileNodeOptions.Uri" /> names several. Records still arrive in file
    ///     order: later files are read ahead into a bounded buffer while the current one is consumed. Defaults to 1.
    /// </summary>
    public int FileReadParallelism { get; init; } = 1;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentOutOfRangeException.ThrowIfNegative(RawExcerptLength, nameof(RawExcerptLength));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(FileReadParallelism, nameof(FileReadParallelism));
    }
}

/// <summary>Options for a <see cref="FileSinkNode{T}" />.</summary>
public abstract record FileSinkOptions : FileNodeOptions
{
    /// <summary>Whether to write through a temporary object. Defaults to <see cref="Files.AtomicWrite.Auto" />.</summary>
    public AtomicWrite AtomicWrite { get; init; } = AtomicWrite.Auto;

    /// <summary>What to do with a <c>null</c> item. Defaults to <see cref="NullItemHandling.Throw" />.</summary>
    public NullItemHandling NullItems { get; init; } = NullItemHandling.Throw;

    /// <summary>
    ///     Whether a failed write deletes what it wrote, when the provider can delete: the temporary object, or the target
    ///     when writing directly. Defaults to <c>true</c>, so a failure never leaves a truncated file behind.
    /// </summary>
    public bool DeletePartialOnFailure { get; init; } = true;
}

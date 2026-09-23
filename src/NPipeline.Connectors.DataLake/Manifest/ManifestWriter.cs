using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using NPipeline.Connectors.DataLake.Reliability;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.Manifest;

/// <summary>
///     Appends manifest entries to the table's manifest file.
///     Manifest is stored at <c>_manifest/manifest.ndjson</c> relative to the table base path.
///     Retries the main-manifest append on transient storage errors through
///     <see cref="DataLakeConnectorResilience.ManifestWrite" />.
/// </summary>
/// <remarks>
///     <para>
///         Each flush writes two files: the per-snapshot manifest <c>_manifest/snapshots/{snapshotId}.ndjson</c>, which
///         holds every entry this writer has flushed and is written only by this writer, and then the main manifest,
///         which it appends to by reading the file, adding the new entries, and replacing it (by an atomic rename when the
///         provider implements <see cref="IMoveableStorageProvider" />, otherwise by overwriting it in place).
///     </para>
///     <para>
///         The main manifest is last-writer-wins: there is no conditional write, so when two writers append at the same
///         time, one writer's entries can be missing from it. <see cref="ManifestReader" /> recovers them by merging every
///         per-snapshot manifest into what it reads from the main manifest, so readers see all flushed entries. Tools that
///         read <c>manifest.ndjson</c> directly, without the snapshot files, can miss entries. Use a distinct snapshot ID
///         per writer (<see cref="GenerateSnapshotId" />): two writers sharing one overwrite each other's snapshot file.
///     </para>
/// </remarks>
public sealed class ManifestWriter : IAsyncDisposable
{
    private const string ManifestDirectoryName = "_manifest";
    private const string ManifestFileName = "manifest.ndjson";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = false,
    };

    private readonly StorageUri _manifestUri;
    // Entries already written by earlier flushes; the snapshot file is rewritten with these plus the pending ones
    private readonly List<ManifestEntry> _flushedEntries = [];
    private readonly List<ManifestEntry> _pendingEntries = [];

    private readonly IStorageProvider _provider;
    private readonly NResilience.Resilience _resilience;
    private readonly StorageUri _snapshotManifestUri;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ManifestWriter" /> class.
    /// </summary>
    /// <param name="provider">The storage provider to use for writing.</param>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="snapshotId">The snapshot ID for this write session.</param>
    /// <param name="resilience">
    ///     The policy for appending to the main manifest. Defaults to <see cref="DataLakeConnectorResilience.ManifestWrite" />.
    /// </param>
    public ManifestWriter(
        IStorageProvider provider,
        StorageUri tableBasePath,
        string snapshotId,
        NResilience.Resilience? resilience = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(tableBasePath);
        ArgumentNullException.ThrowIfNull(snapshotId);

        _provider = provider;
        SnapshotId = snapshotId;
        _resilience = resilience ?? DataLakeConnectorResilience.ManifestWrite;
        _resilience.Validate();

        // Build manifest URIs
        var manifestPath = BuildManifestPath(tableBasePath);
        _manifestUri = StorageUri.Parse($"{tableBasePath.Scheme}://{tableBasePath.Host}{manifestPath}");

        var snapshotManifestPath = BuildSnapshotManifestPath(tableBasePath, snapshotId);

        _snapshotManifestUri =
            StorageUri.Parse($"{tableBasePath.Scheme}://{tableBasePath.Host}{snapshotManifestPath}");
    }

    /// <summary>
    ///     Gets the snapshot ID for this writer.
    /// </summary>
    public string SnapshotId { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        try
        {
            if (_pendingEntries.Count > 0)
                await FlushAsync().ConfigureAwait(false);
        }
        catch
        {
            // Swallow exceptions during disposal
        }

        _writeLock.Dispose();
        _disposed = true;
    }

    /// <summary>
    ///     Generates a new snapshot ID using the format: yyyyMMddHHmmssfff-xxxxxxxx.
    /// </summary>
    /// <returns>A new snapshot ID string.</returns>
    public static string GenerateSnapshotId()
    {
        var timestamp = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmssfff", CultureInfo.InvariantCulture);
        var randomSuffix = RandomNumberGenerator.GetHexString(8, true);
        return $"{timestamp}-{randomSuffix}";
    }

    /// <summary>
    ///     Appends a manifest entry to the pending list.
    ///     Call <see cref="FlushAsync" /> to write pending entries to storage.
    /// </summary>
    /// <param name="entry">The manifest entry to append.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="entry" /> is <c>null</c>.</exception>
    public void Append(ManifestEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Ensure the entry has the correct snapshot ID
        var entryWithSnapshot = entry.SnapshotId == SnapshotId
            ? entry
            : entry with { SnapshotId = SnapshotId };

        _pendingEntries.Add(entryWithSnapshot);
    }

    /// <summary>
    ///     Appends multiple manifest entries to the pending list.
    /// </summary>
    /// <param name="entries">The manifest entries to append.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="entries" /> is <c>null</c>.</exception>
    public void AppendRange(IEnumerable<ManifestEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ObjectDisposedException.ThrowIf(_disposed, this);

        foreach (var entry in entries)
        {
            Append(entry);
        }
    }

    /// <summary>
    ///     Flushes all pending entries to storage.
    ///     Writes to both the per-snapshot manifest and appends to the main manifest.
    ///     Retries the main-manifest append on transient storage errors.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_pendingEntries.Count == 0)
            return;

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            // Write to per-snapshot manifest
            await WriteSnapshotManifestAsync(cancellationToken).ConfigureAwait(false);

            // Append to main manifest, retrying transient storage errors
            await AppendToMainManifestWithRetryAsync(cancellationToken).ConfigureAwait(false);

            _flushedEntries.AddRange(_pendingEntries);
            _pendingEntries.Clear();
        }
        finally
        {
            _ = _writeLock.Release();
        }
    }

    private async Task WriteSnapshotManifestAsync(CancellationToken cancellationToken)
    {
        // Rewrite this writer's snapshot file with everything it has flushed, so it stays complete across flushes: it is
        // what readers use to recover entries a concurrent writer overwrote in the main manifest
        var content = BuildNdJsonContent([.. _flushedEntries, .. _pendingEntries]);

        var stream = await _provider.OpenWriteAsync(_snapshotManifestUri, cancellationToken)
            .ConfigureAwait(false);
        await using var streamScope = stream.ConfigureAwait(false);

        var writer = new StreamWriter(stream, Encoding.UTF8, leaveOpen: false);
        await using var writerScope = writer.ConfigureAwait(false);
        await writer.WriteAsync(content).ConfigureAwait(false);
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task AppendToMainManifestWithRetryAsync(CancellationToken cancellationToken)
    {
        var newContent = BuildNdJsonContent(_pendingEntries);

        await _resilience.RunAsync(
                (Func<CancellationToken, Task>)(ct => AppendToMainManifestAtomicAsync(newContent, ct)),
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task AppendToMainManifestAtomicAsync(string newContent, CancellationToken cancellationToken)
    {
        // Check if main manifest exists
        bool manifestExists;

        // Providers return null for a missing file. Any other failure must propagate: treating it as "missing" would
        // overwrite the manifest with only the new entries.
        try
        {
            var metadata = await _provider.GetMetadataAsync(_manifestUri, cancellationToken).ConfigureAwait(false);
            manifestExists = metadata is not null;
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            manifestExists = false;
        }

        if (manifestExists)
        {
            // For atomic appends, we use a temp file pattern when the provider supports it
            if (_provider is IMoveableStorageProvider moveableProvider)
            {
                await AppendWithAtomicRenameAsync(moveableProvider, newContent, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                // Fallback: read-modify-write (less safe for concurrent writes)
                await AppendWithReadModifyWriteAsync(newContent, cancellationToken).ConfigureAwait(false);
            }
        }
        else
        {
            // Create new manifest
            var writeStream = await _provider.OpenWriteAsync(_manifestUri, cancellationToken)
                .ConfigureAwait(false);
            await using var writeStreamScope = writeStream.ConfigureAwait(false);

            var writer = new StreamWriter(writeStream, Encoding.UTF8, leaveOpen: false);
            await using var writerScope = writer.ConfigureAwait(false);
            await writer.WriteAsync(newContent).ConfigureAwait(false);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task AppendWithAtomicRenameAsync(
        IMoveableStorageProvider moveableProvider,
        string newContent,
        CancellationToken cancellationToken)
    {
        // Read existing content
        string existingContent;

        var readStream = await _provider.OpenReadAsync(_manifestUri, cancellationToken).ConfigureAwait(false);

        await using (readStream.ConfigureAwait(false))
        {
            using var reader = new StreamReader(readStream, Encoding.UTF8);
            existingContent = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
        }

        // An earlier attempt may have committed before it failed; don't append the same entries twice
        if (ContainsEntries(existingContent, newContent))
            return;

        // Build combined content
        var combinedContent = existingContent;

        if (!existingContent.EndsWith('\n') && !string.IsNullOrEmpty(existingContent))
            combinedContent += '\n';

        combinedContent += newContent;

        // Write to temp file
        var tempUri = CreateTempManifestUri();

        var writeStream = await _provider.OpenWriteAsync(tempUri, cancellationToken).ConfigureAwait(false);

        await using (writeStream.ConfigureAwait(false))
        {
            var writer = new StreamWriter(writeStream, Encoding.UTF8, leaveOpen: false);
            await using var writerScope = writer.ConfigureAwait(false);
            await writer.WriteAsync(combinedContent).ConfigureAwait(false);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        // Atomic rename
        await moveableProvider.MoveAsync(tempUri, _manifestUri, cancellationToken).ConfigureAwait(false);
    }

    private async Task AppendWithReadModifyWriteAsync(string newContent, CancellationToken cancellationToken)
    {
        // Read existing content and append
        string existingContent;

        var readStream = await _provider.OpenReadAsync(_manifestUri, cancellationToken).ConfigureAwait(false);

        await using (readStream.ConfigureAwait(false))
        {
            using var reader = new StreamReader(readStream, Encoding.UTF8);
            existingContent = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
        }

        // An earlier attempt may have committed before it failed; don't append the same entries twice
        if (ContainsEntries(existingContent, newContent))
            return;

        var combinedContent = existingContent;

        if (!existingContent.EndsWith('\n') && !string.IsNullOrEmpty(existingContent))
            combinedContent += '\n';

        combinedContent += newContent;

        var writeStream = await _provider.OpenWriteAsync(_manifestUri, cancellationToken)
            .ConfigureAwait(false);
        await using var writeStreamScope = writeStream.ConfigureAwait(false);

        var writer = new StreamWriter(writeStream, Encoding.UTF8, leaveOpen: false);
        await using var writerScope = writer.ConfigureAwait(false);
        await writer.WriteAsync(combinedContent).ConfigureAwait(false);
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static bool ContainsEntries(string existingContent, string newContent)
    {
        // Every entry carries this flush's snapshot ID and write timestamps, so the serialized block is unique to it
        return newContent.Length > 0 && existingContent.Contains(newContent, StringComparison.Ordinal);
    }

    private StorageUri CreateTempManifestUri()
    {
        var tempSuffix = $".tmp-{Guid.NewGuid():N}";
        var manifestPath = _manifestUri.Path ?? string.Empty;
        var tempPath = manifestPath + tempSuffix;
        return StorageUri.Parse($"{_manifestUri.Scheme}://{_manifestUri.Host}{tempPath}");
    }

    private static string BuildNdJsonContent(IReadOnlyList<ManifestEntry> entries)
    {
        var sb = new StringBuilder();

        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            var json = JsonSerializer.Serialize(entry, JsonOptions);
            _ = sb.Append(json);

            if (i < entries.Count - 1)
                _ = sb.Append('\n');
        }

        return sb.ToString();
    }

    private static string BuildManifestPath(StorageUri tableBasePath)
    {
        var basePath = tableBasePath.Path?.TrimStart('/') ?? string.Empty;

        return string.IsNullOrEmpty(basePath)
            ? $"/{ManifestDirectoryName}/{ManifestFileName}"
            : $"/{basePath}/{ManifestDirectoryName}/{ManifestFileName}";
    }

    private static string BuildSnapshotManifestPath(StorageUri tableBasePath, string snapshotId)
    {
        var basePath = tableBasePath.Path?.TrimStart('/') ?? string.Empty;

        return string.IsNullOrEmpty(basePath)
            ? $"/{ManifestDirectoryName}/snapshots/{snapshotId}.ndjson"
            : $"/{basePath}/{ManifestDirectoryName}/snapshots/{snapshotId}.ndjson";
    }
}

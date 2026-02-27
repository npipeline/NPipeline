using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.Manifest
{
    /// <summary>
    ///     Appends manifest entries to the table's manifest file.
    ///     Uses append-only writes to avoid full-file rewrites.
    ///     Manifest is stored at <c>_manifest/manifest.ndjson</c> relative to the table base path.
    ///     Implements retry logic for concurrent write safety.
    /// </summary>
    public sealed class ManifestWriter : IAsyncDisposable
    {
        private const string ManifestDirectoryName = "_manifest";
        private const string ManifestFileName = "manifest.ndjson";
        private const int MaxRetryAttempts = 3;
        private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(100);

        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            WriteIndented = false
        };

        private readonly IStorageProvider _provider;
        private readonly StorageUri _manifestUri;
        private readonly StorageUri _snapshotManifestUri;
        private readonly List<ManifestEntry> _pendingEntries = [];
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private bool _disposed;

        /// <summary>
        ///     Gets the snapshot ID for this writer.
        /// </summary>
        public string SnapshotId { get; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="ManifestWriter" /> class.
        /// </summary>
        /// <param name="provider">The storage provider to use for writing.</param>
        /// <param name="tableBasePath">The base path of the table.</param>
        /// <param name="snapshotId">The snapshot ID for this write session.</param>
        public ManifestWriter(IStorageProvider provider, StorageUri tableBasePath, string snapshotId)
        {
            ArgumentNullException.ThrowIfNull(provider);
            ArgumentNullException.ThrowIfNull(tableBasePath);
            ArgumentNullException.ThrowIfNull(snapshotId);

            _provider = provider;
            SnapshotId = snapshotId;

            // Build manifest URIs
            var manifestPath = BuildManifestPath(tableBasePath);
            _manifestUri = StorageUri.Parse($"{tableBasePath.Scheme}://{tableBasePath.Host}{manifestPath}");

            var snapshotManifestPath = BuildSnapshotManifestPath(tableBasePath, snapshotId);
            _snapshotManifestUri =
                StorageUri.Parse($"{tableBasePath.Scheme}://{tableBasePath.Host}{snapshotManifestPath}");
        }

        /// <summary>
        ///     Generates a new snapshot ID using the format: yyyyMMddHHmmssfff-xxxxxxxx.
        /// </summary>
        /// <returns>A new snapshot ID string.</returns>
        public static string GenerateSnapshotId()
        {
            var timestamp = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmssfff", System.Globalization.CultureInfo.InvariantCulture);
            var randomSuffix = RandomNumberGenerator.GetHexString(8, lowercase: true);
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
        ///     Implements retry logic to handle concurrent write conflicts.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_pendingEntries.Count == 0)
            {
                return;
            }

            await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                // Write to per-snapshot manifest
                await WriteSnapshotManifestAsync(cancellationToken).ConfigureAwait(false);

                // Append to main manifest with retry logic for concurrent write safety
                await AppendToMainManifestWithRetryAsync(cancellationToken).ConfigureAwait(false);

                _pendingEntries.Clear();
            }
            finally
            {
                _ = _writeLock.Release();
            }
        }

        private async Task WriteSnapshotManifestAsync(CancellationToken cancellationToken)
        {
            // Write all pending entries to a dedicated snapshot file
            var content = BuildNdJsonContent(_pendingEntries);

            await using var stream = await _provider.OpenWriteAsync(_snapshotManifestUri, cancellationToken)
                .ConfigureAwait(false);

            await using var writer = new StreamWriter(stream, Encoding.UTF8, leaveOpen: false);
            await writer.WriteAsync(content).ConfigureAwait(false);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task AppendToMainManifestWithRetryAsync(CancellationToken cancellationToken)
        {
            var newContent = BuildNdJsonContent(_pendingEntries);

            for (var attempt = 0; attempt < MaxRetryAttempts; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    await AppendToMainManifestAtomicAsync(newContent, cancellationToken).ConfigureAwait(false);
                    return; // Success
                }
                catch (Exception ex) when (IsRetryableException(ex) && attempt < MaxRetryAttempts - 1)
                {
                    // Concurrent modification or transient error detected, retry after delay
                    await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private static bool IsRetryableException(Exception ex)
        {
            // IOException covers file locking and concurrent access scenarios
            // Include other transient storage errors that may occur with different providers
            return ex is IOException ||
                   ex is UnauthorizedAccessException || // Can occur during brief locking windows
                   (ex is AggregateException ae && ae.InnerExceptions.Any(IsRetryableException));
        }

        private async Task AppendToMainManifestAtomicAsync(string newContent, CancellationToken cancellationToken)
        {
            // Check if main manifest exists
            bool manifestExists;

            try
            {
                var metadata = await _provider.GetMetadataAsync(_manifestUri, cancellationToken).ConfigureAwait(false);
                manifestExists = metadata is not null;
            }
            catch
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
                await using var writeStream = await _provider.OpenWriteAsync(_manifestUri, cancellationToken)
                    .ConfigureAwait(false);
                await using var writer = new StreamWriter(writeStream, Encoding.UTF8, leaveOpen: false);
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
            await using (var readStream = await _provider.OpenReadAsync(_manifestUri, cancellationToken)
                             .ConfigureAwait(false))
            {
                using var reader = new StreamReader(readStream, Encoding.UTF8);
                existingContent = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
            }

            // Build combined content
            var combinedContent = existingContent;
            if (!existingContent.EndsWith('\n') && !string.IsNullOrEmpty(existingContent))
            {
                combinedContent += '\n';
            }
            combinedContent += newContent;

            // Write to temp file
            var tempUri = CreateTempManifestUri();
            await using (var writeStream = await _provider.OpenWriteAsync(tempUri, cancellationToken)
                             .ConfigureAwait(false))
            {
                await using var writer = new StreamWriter(writeStream, Encoding.UTF8, leaveOpen: false);
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
            await using (var readStream = await _provider.OpenReadAsync(_manifestUri, cancellationToken)
                             .ConfigureAwait(false))
            {
                using var reader = new StreamReader(readStream, Encoding.UTF8);
                existingContent = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
            }

            var combinedContent = existingContent;
            if (!existingContent.EndsWith('\n') && !string.IsNullOrEmpty(existingContent))
            {
                combinedContent += '\n';
            }

            combinedContent += newContent;

            await using var writeStream = await _provider.OpenWriteAsync(_manifestUri, cancellationToken)
                .ConfigureAwait(false);
            await using var writer = new StreamWriter(writeStream, Encoding.UTF8, leaveOpen: false);
            await writer.WriteAsync(combinedContent).ConfigureAwait(false);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
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
                {
                    _ = sb.Append('\n');
                }
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

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                if (_pendingEntries.Count > 0)
                {
                    await FlushAsync().ConfigureAwait(false);
                }
            }
            catch
            {
                // Swallow exceptions during disposal
            }

            _writeLock.Dispose();
            _disposed = true;
        }
    }
}

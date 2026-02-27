using System.Text;
using System.Text.Json;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.Manifest
{
    /// <summary>
    ///     Reads manifest entries from the table's manifest file.
    ///     Supports filtering by snapshot ID and time-travel queries.
    /// </summary>
    public sealed class ManifestReader
    {
        private const string ManifestDirectoryName = "_manifest";
        private const string ManifestFileName = "manifest.ndjson";

        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            PropertyNameCaseInsensitive = true
        };

        private readonly IStorageProvider _provider;
        private readonly StorageUri _tableBasePath;

        /// <summary>
        ///     Initializes a new instance of the <see cref="ManifestReader" /> class.
        /// </summary>
        /// <param name="provider">The storage provider to use for reading.</param>
        /// <param name="tableBasePath">The base path of the table.</param>
        public ManifestReader(IStorageProvider provider, StorageUri tableBasePath)
        {
            ArgumentNullException.ThrowIfNull(provider);
            ArgumentNullException.ThrowIfNull(tableBasePath);

            _provider = provider;
            _tableBasePath = tableBasePath;
        }

        /// <summary>
        ///     Reads all manifest entries from the table.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A list of all manifest entries.</returns>
        public async Task<IReadOnlyList<ManifestEntry>> ReadAllAsync(CancellationToken cancellationToken = default)
        {
            var entries = new List<ManifestEntry>();

            // Try to read from the main manifest first
            var mainManifestUri = BuildManifestUri();
            if (await ExistsAsync(mainManifestUri, cancellationToken).ConfigureAwait(false))
            {
                var content = await ReadContentAsync(mainManifestUri, cancellationToken).ConfigureAwait(false);
                entries.AddRange(ParseNdJson(content));
            }

            // Also check for snapshot fragment manifests and merge them
            var snapshotEntries = await ReadSnapshotManifestsAsync(cancellationToken).ConfigureAwait(false);
            entries = MergeEntries(entries, snapshotEntries);

            return entries;
        }

        /// <summary>
        ///     Reads manifest entries for a specific snapshot.
        /// </summary>
        /// <param name="snapshotId">The snapshot ID to filter by.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A list of manifest entries for the specified snapshot.</returns>
        public async Task<IReadOnlyList<ManifestEntry>> ReadBySnapshotAsync(
            string snapshotId,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(snapshotId);

            // First, try to read from the dedicated snapshot manifest
            var snapshotManifestUri = BuildSnapshotManifestUri(snapshotId);
            if (await ExistsAsync(snapshotManifestUri, cancellationToken).ConfigureAwait(false))
            {
                var content = await ReadContentAsync(snapshotManifestUri, cancellationToken).ConfigureAwait(false);
                return ParseNdJson(content);
            }

            // Fallback: filter main manifest by snapshot ID
            var allEntries = await ReadAllAsync(cancellationToken).ConfigureAwait(false);
            return allEntries.Where(e => e.SnapshotId == snapshotId).ToList();
        }

        /// <summary>
        ///     Reads manifest entries written at or before the specified timestamp (time travel).
        /// </summary>
        /// <param name="asOf">The timestamp to filter by.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A list of manifest entries written at or before the specified timestamp.</returns>
        public async Task<IReadOnlyList<ManifestEntry>> ReadAsOfAsync(
            DateTimeOffset asOf,
            CancellationToken cancellationToken = default)
        {
            var allEntries = await ReadAllAsync(cancellationToken).ConfigureAwait(false);

            // Filter by timestamp and deduplicate by path (keep the latest version before asOf)
            // Use UtcTicks for consistent comparison regardless of timezone offsets
            var asOfTicks = asOf.UtcTicks;
            return allEntries
                .Where(e => e.WrittenAt.UtcTicks <= asOfTicks)
                .GroupBy(e => e.Path)
                .Select(g => g.OrderByDescending(e => e.WrittenAt).First())
                .ToList();
        }

        /// <summary>
        ///     Gets the list of available snapshot IDs from the manifest.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A list of unique snapshot IDs.</returns>
        public async Task<IReadOnlyList<string>> GetSnapshotIdsAsync(CancellationToken cancellationToken = default)
        {
            var entries = await ReadAllAsync(cancellationToken).ConfigureAwait(false);
            return entries
                .Select(e => e.SnapshotId)
                .Distinct()
                .OrderByDescending(id => id) // Most recent first
                .ToList();
        }

        /// <summary>
        ///     Gets the latest snapshot ID from the manifest.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The latest snapshot ID, or <c>null</c> if no snapshots exist.</returns>
        public async Task<string?> GetLatestSnapshotIdAsync(CancellationToken cancellationToken = default)
        {
            var snapshotIds = await GetSnapshotIdsAsync(cancellationToken).ConfigureAwait(false);
            return snapshotIds.Count > 0 ? snapshotIds[0] : null;
        }

        /// <summary>
        ///     Gets the total row count across all manifest entries.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The total row count.</returns>
        public async Task<long> GetTotalRowCountAsync(CancellationToken cancellationToken = default)
        {
            var entries = await ReadAllAsync(cancellationToken).ConfigureAwait(false);
            return entries.Sum(e => e.RowCount);
        }

        /// <summary>
        ///     Checks if the manifest exists.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns><c>true</c> if the manifest exists; otherwise, <c>false</c>.</returns>
        public async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            var manifestUri = BuildManifestUri();
            return await ExistsAsync(manifestUri, cancellationToken).ConfigureAwait(false);
        }

        private StorageUri BuildManifestUri()
        {
            var manifestPath = BuildManifestPath();
            return StorageUri.Parse($"{_tableBasePath.Scheme}://{_tableBasePath.Host}{manifestPath}");
        }

        private StorageUri BuildSnapshotManifestUri(string snapshotId)
        {
            var basePath = _tableBasePath.Path?.TrimStart('/') ?? string.Empty;
            var snapshotPath = string.IsNullOrEmpty(basePath)
                ? $"/{ManifestDirectoryName}/snapshots/{snapshotId}.ndjson"
                : $"/{basePath}/{ManifestDirectoryName}/snapshots/{snapshotId}.ndjson";

            return StorageUri.Parse($"{_tableBasePath.Scheme}://{_tableBasePath.Host}{snapshotPath}");
        }

        private string BuildManifestPath()
        {
            var basePath = _tableBasePath.Path?.TrimStart('/') ?? string.Empty;
            return string.IsNullOrEmpty(basePath)
                ? $"/{ManifestDirectoryName}/{ManifestFileName}"
                : $"/{basePath}/{ManifestDirectoryName}/{ManifestFileName}";
        }

        private async Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken)
        {
            try
            {
                var metadata = await _provider.GetMetadataAsync(uri, cancellationToken).ConfigureAwait(false);
                return metadata is not null && !metadata.IsDirectory;
            }
            catch
            {
                return false;
            }
        }

        private async Task<string> ReadContentAsync(StorageUri uri, CancellationToken cancellationToken)
        {
            await using var stream = await _provider.OpenReadAsync(uri, cancellationToken).ConfigureAwait(false);
            using var reader = new StreamReader(stream, Encoding.UTF8);
            return await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task<List<ManifestEntry>> ReadSnapshotManifestsAsync(CancellationToken cancellationToken)
        {
            var entries = new List<ManifestEntry>();

            // List snapshot manifest files
            var snapshotsPath = BuildSnapshotsDirectoryPath();
            var snapshotsUri =
                StorageUri.Parse($"{_tableBasePath.Scheme}://{_tableBasePath.Host}{snapshotsPath}");

            try
            {
                await foreach (var item in _provider.ListAsync(snapshotsUri, recursive: false, cancellationToken)
                                   .ConfigureAwait(false))
                {
                    if (item.IsDirectory || !item.Uri.Path?.EndsWith(".ndjson", StringComparison.OrdinalIgnoreCase) == true)
                    {
                        continue;
                    }

                    try
                    {
                        var content = await ReadContentAsync(item.Uri, cancellationToken).ConfigureAwait(false);
                        entries.AddRange(ParseNdJson(content));
                    }
                    catch
                    {
                        // Skip unreadable snapshot manifests
                    }
                }
            }
            catch
            {
                // Snapshots directory may not exist
            }

            return entries;
        }

        private string BuildSnapshotsDirectoryPath()
        {
            var basePath = _tableBasePath.Path?.TrimStart('/') ?? string.Empty;
            return string.IsNullOrEmpty(basePath)
                ? $"/{ManifestDirectoryName}/snapshots/"
                : $"/{basePath}/{ManifestDirectoryName}/snapshots/";
        }

        private static List<ManifestEntry> ParseNdJson(string content)
        {
            if (string.IsNullOrWhiteSpace(content))
            {
                return [];
            }

            var entries = new List<ManifestEntry>();
            var lines = content.Split(['\n', '\r'], StringSplitOptions.RemoveEmptyEntries);

            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                try
                {
                    var entry = JsonSerializer.Deserialize<ManifestEntry>(line, JsonOptions);
                    if (entry is not null)
                    {
                        entries.Add(entry);
                    }
                }
                catch (JsonException)
                {
                    // Skip malformed entries
                }
            }

            return entries;
        }

        private static List<ManifestEntry> MergeEntries(
            IReadOnlyList<ManifestEntry> mainEntries,
            List<ManifestEntry> snapshotEntries)
        {
            if (snapshotEntries.Count == 0)
            {
                return [.. mainEntries];
            }

            if (mainEntries.Count == 0)
            {
                return snapshotEntries;
            }

            // Create a dictionary keyed by (Path, SnapshotId) for deduplication
            var merged = mainEntries.ToDictionary(e => (e.Path, e.SnapshotId));

            foreach (var entry in snapshotEntries)
            {
                var key = (entry.Path, entry.SnapshotId);
                if (!merged.ContainsKey(key))
                {
                    merged[key] = entry;
                }
            }

            return [.. merged.Values];
        }
    }
}

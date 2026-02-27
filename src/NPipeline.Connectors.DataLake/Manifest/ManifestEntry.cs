using System.Text.Json.Serialization;

namespace NPipeline.Connectors.DataLake.Manifest
{
    /// <summary>
    ///     Represents a single entry in the table manifest, tracking a data file's metadata.
    ///     Serialized as newline-delimited JSON (NDJSON) in the manifest file.
    /// </summary>
    public sealed record ManifestEntry
    {
        /// <summary>
        ///     Gets the relative path to the data file from the table base path.
        /// </summary>
        [JsonPropertyName("path")]
        public required string Path { get; init; }

        /// <summary>
        ///     Gets the number of rows in the data file.
        /// </summary>
        [JsonPropertyName("row_count")]
        public required long RowCount { get; init; }

        /// <summary>
        ///     Gets the timestamp when this file was written.
        /// </summary>
        [JsonPropertyName("written_at")]
        public required DateTimeOffset WrittenAt { get; init; }

        /// <summary>
        ///     Gets the size of the file in bytes.
        /// </summary>
        [JsonPropertyName("file_size_bytes")]
        public required long FileSizeBytes { get; init; }

        /// <summary>
        ///     Gets the partition values for this file, if the table is partitioned.
        ///     Keys are partition column names, values are the string representation of partition values.
        /// </summary>
        [JsonPropertyName("partition_values")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public IReadOnlyDictionary<string, string>? PartitionValues { get; init; }

        /// <summary>
        ///     Gets the snapshot ID that this file belongs to.
        ///     Format: yyyyMMddHHmmssfff-xxxxxxxx (sortable timestamp + random suffix).
        /// </summary>
        [JsonPropertyName("snapshot_id")]
        public required string SnapshotId { get; init; }

        /// <summary>
        ///     Gets an optional content hash (e.g., MD5, SHA256) for integrity verification.
        /// </summary>
        [JsonPropertyName("content_hash")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? ContentHash { get; init; }

        /// <summary>
        ///     Gets the manifest format version. Default is "v1".
        /// </summary>
        [JsonPropertyName("format_version")]
        public string FormatVersion { get; init; } = "v1";

        /// <summary>
        ///     Gets the file format (e.g., "parquet").
        /// </summary>
        [JsonPropertyName("file_format")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? FileFormat { get; init; }

        /// <summary>
        ///     Gets optional compression codec used for the file.
        /// </summary>
        [JsonPropertyName("compression")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? Compression { get; init; }

        /// <summary>
        ///     Creates a deep copy of this manifest entry.
        /// </summary>
        /// <returns>A new <see cref="ManifestEntry" /> with the same values.</returns>
        public ManifestEntry Copy()
        {
            return new ManifestEntry
            {
                Path = Path,
                RowCount = RowCount,
                WrittenAt = WrittenAt,
                FileSizeBytes = FileSizeBytes,
                PartitionValues = PartitionValues is not null
                    ? new Dictionary<string, string>(PartitionValues)
                    : null,
                SnapshotId = SnapshotId,
                ContentHash = ContentHash,
                FormatVersion = FormatVersion,
                FileFormat = FileFormat,
                Compression = Compression
            };
        }
    }
}

namespace NPipeline.Connectors;

/// <summary>
///     Represents a single item returned by the ListAsync method of a storage provider.
/// </summary>
/// <remarks>
///     <para>
///         This record captures basic metadata about a storage item without requiring full metadata retrieval.
///         Use GetMetadataAsync on the storage provider for additional details.
///     </para>
///     <para>
///         <strong>Cloud Provider Notes:</strong>
///         For S3 and Azure providers, <see cref="IsDirectory" /> may indicate logical grouping (keys with "/" delimiters) rather than true directories.
///     </para>
/// </remarks>
public sealed record StorageItem
{
    /// <summary>
    ///     The URI of the storage item.
    /// </summary>
    public required StorageUri Uri { get; init; }

    /// <summary>
    ///     The size of the item in bytes. For directories or items where size cannot be determined, may be zero.
    /// </summary>
    public required long Size { get; init; }

    /// <summary>
    ///     The last modification time of the item in UTC. Implementations should provide the most accurate timestamp available.
    /// </summary>
    public required DateTimeOffset LastModified { get; init; }

    /// <summary>
    ///     Indicates whether the item represents a directory/container rather than a file/blob.
    /// </summary>
    public bool IsDirectory { get; init; }
}

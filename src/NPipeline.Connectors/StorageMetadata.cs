namespace NPipeline.Connectors;

/// <summary>
///     Metadata describing a specific storage resource, returned by GetMetadataAsync on a storage provider.
/// </summary>
/// <remarks>
///     <para>
///         This record provides a richer set of properties than <see cref="StorageItem" /> for scenarios
///         requiring detailed information about a single resource.
///     </para>
///     <para>
///         <strong>Field Availability:</strong>
///         Not all fields are available from every provider:
///         - <see cref="ContentType" /> is populated by filesystem provider; cloud providers may or may not provide it.
///         - <see cref="CustomMetadata" /> is populated from cloud storage object metadata (S3 tags, Azure properties).
///         - <see cref="ETag" /> is populated where supported (S3, Azure); filesystem provider includes last-write-time hash.
///     </para>
///     <para>
///         Consumers should check for null values and consult the storage provider documentation
///         to understand what fields are guaranteed vs. optional.
///     </para>
/// </remarks>
public sealed record StorageMetadata
{
    /// <summary>
    ///     The size of the resource in bytes. For directories or when size cannot be determined, may be zero.
    /// </summary>
    public required long Size { get; init; }

    /// <summary>
    ///     The last modification time of the resource in UTC.
    /// </summary>
    public required DateTimeOffset LastModified { get; init; }

    /// <summary>
    ///     The MIME type of the resource (e.g., "text/csv", "application/json") if available.
    ///     May be null if not supported by the storage backend.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    ///     Custom or provider-specific metadata as key-value pairs (e.g., S3 object tags, Azure blob properties).
    ///     Keys are case-insensitive. May be empty if no custom metadata is available.
    /// </summary>
    public IReadOnlyDictionary<string, string> CustomMetadata { get; init; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Indicates whether the resource represents a directory/container rather than a file/blob.
    /// </summary>
    public bool IsDirectory { get; init; }

    /// <summary>
    ///     An entity tag or version identifier for the resource (e.g., S3 ETag, Azure blob version).
    ///     Useful for optimistic concurrency control and cache validation.
    ///     May be null if not supported by the storage backend.
    /// </summary>
    public string? ETag { get; init; }
}

namespace NPipeline.StorageProviders.Models;

/// <summary>
///     Describes a storage provider's capabilities for discovery, validation, and diagnostics.
/// </summary>
/// <remarks>
///     This metadata can be exposed by providers and surfaced via resolvers to enable:
///     - Capability validation (e.g., read/write support)
///     - Scheme discovery (e.g., "file", "s3")
///     - Optional capability negotiation via free-form <see cref="Capabilities" />
/// </remarks>
public sealed record StorageProviderMetadata
{
    /// <summary>
    ///     Human-friendly name for the provider (e.g., "File System", "Amazon S3").
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    ///     List of supported URI schemes (e.g., ["file"], ["s3"], ["azure"]).
    /// </summary>
    public required string[] SupportedSchemes { get; init; } = [];

    /// <summary>
    ///     Indicates whether the provider supports read (OpenRead) operations.
    /// </summary>
    public bool SupportsRead { get; init; } = true;

    /// <summary>
    ///     Indicates whether the provider supports write (OpenWrite) operations.
    /// </summary>
    public bool SupportsWrite { get; init; }

    /// <summary>
    ///     Indicates whether the provider supports delete (DeleteAsync) operations.
    /// </summary>
    public bool SupportsDelete { get; init; }

    /// <summary>
    ///     Indicates whether the provider supports listing (ListAsync) operations.
    /// </summary>
    public bool SupportsListing { get; init; }

    /// <summary>
    ///     Indicates whether the provider supports detailed metadata retrieval (GetMetadataAsync) operations.
    /// </summary>
    public bool SupportsMetadata { get; init; }

    /// <summary>
    ///     Indicates whether the provider has a meaningful hierarchy concept (directories, prefixes, containers).
    ///     When true, recursive listing walks nested structures; when false, ListAsync recursion semantics are provider-specific.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Providers with hierarchy (filesystem, S3, Azure):
    ///         - recursive=false: Returns direct children only
    ///         - recursive=true: Returns all descendants
    ///     </para>
    ///     <para>
    ///         Providers without hierarchy (some databases):
    ///         - Recursion semantics may not apply; refer to provider documentation
    ///     </para>
    ///     <para>
    ///         <strong>Permission Handling (Recursive Listing):</strong>
    ///         Providers should skip inaccessible items/subdirectories during recursive enumeration rather than aborting.
    ///         This ensures robust partial results even in shared or restricted environments.
    ///     </para>
    /// </remarks>
    public bool SupportsHierarchy { get; init; } = true;

    /// <summary>
    ///     A free-form set of additional capabilities (e.g., {"region": "ap-southeast-2"}).
    ///     Keys are case-insensitive.
    /// </summary>
    public IReadOnlyDictionary<string, object> Capabilities { get; init; }
        = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Checks if the provider supports the given scheme.
    /// </summary>
    public bool SupportsScheme(string? scheme)
    {
        return !string.IsNullOrWhiteSpace(scheme)
               && Array.Exists(SupportedSchemes, s => string.Equals(s, scheme, StringComparison.OrdinalIgnoreCase));
    }
}

using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.Configuration;

/// <summary>
///     Options used when building a <see cref="StorageResolver" /> via <see cref="StorageProviderFactory" />.
/// </summary>
public sealed record StorageResolverOptions
{
    /// <summary>
    ///     Optional configuration describing providers to materialize.
    /// </summary>
    public ConnectorConfiguration? Configuration { get; init; }

    /// <summary>
    ///     Additional provider instances to register alongside configured providers.
    /// </summary>
    public IEnumerable<IStorageProvider>? AdditionalProviders { get; init; }

    /// <summary>
    ///     When true, automatically registers FileSystemStorageProvider from NPipeline.StorageProviders.
    /// </summary>
    public bool IncludeFileSystem { get; init; } = true;

    /// <summary>
    ///     When true, collects per-provider instantiation errors in the returned result.
    /// </summary>
    public bool CollectErrors { get; init; }
}

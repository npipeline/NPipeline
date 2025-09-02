namespace NPipeline.Connectors.Abstractions;

/// <summary>
///     Discovers and resolves storage providers capable of handling a given <see cref="StorageUri" />.
/// </summary>
/// <remarks>
///     Design goals:
///     - Support automatic discovery (reflection-based) and manual registration
///     - Thread-safe provider enumeration and resolution
///     - No DI dependency; usable in both DI and non-DI scenarios
/// </remarks>
public interface IStorageResolver
{
    /// <summary>
    ///     Resolves a provider capable of handling the specified <paramref name="uri" />.
    ///     Returns null if no provider can handle the URI.
    /// </summary>
    /// <param name="uri">The storage URI to resolve for.</param>
    /// <returns>An <see cref="IStorageProvider" /> or null if none found.</returns>
    IStorageProvider? ResolveProvider(StorageUri uri);

    /// <summary>
    ///     Returns a snapshot of all currently available providers.
    /// </summary>
    IEnumerable<IStorageProvider> GetAvailableProviders();

    /// <summary>
    ///     Registers a provider instance for consideration during resolution.
    ///     Implementations should be idempotent if the same instance/type is registered multiple times.
    /// </summary>
    /// <param name="provider">The provider to register.</param>
    void RegisterProvider(IStorageProvider provider);

    /// <summary>
    ///     Triggers discovery of providers if the implementation supports it and it has not yet run.
    ///     Implementations may perform no-op if discovery is not supported or already completed.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for discovery workloads.</param>
    void EnsureDiscovered(CancellationToken cancellationToken = default);
}

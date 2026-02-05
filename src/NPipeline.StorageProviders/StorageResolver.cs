using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders;

/// <summary>
///     Default implementation of <see cref="IStorageResolver" /> with thread-safe registration
///     and scheme-based resolution.
/// </summary>
/// <remarks>
///     - No DI dependency: works in any application architecture
///     - Manual registration only; auto-discovery is handled by higher-level factories
///     - Resolution strategy:
///     1) Prefer providers whose <see cref="IStorageProvider.CanHandle(StorageUri)" /> returns true
///     2) Fallback to first provider whose <see cref="IStorageProvider.Scheme" /> matches <see cref="StorageUri.Scheme" />
/// </remarks>
public sealed class StorageResolver : IStorageResolver
{
    private readonly List<IStorageProvider> _providers = [];
    private readonly object _sync = new();

    /// <summary>Create a new resolver.</summary>
    public StorageResolver()
    {
    }

    /// <inheritdoc />
    public IStorageProvider? ResolveProvider(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        // Take a snapshot to avoid holding locks during provider calls.
        IStorageProvider[] snapshot;

        lock (_sync)
        {
            snapshot = _providers.ToArray();
        }

        // 1) Prefer explicit CanHandle
        foreach (var provider in snapshot)
        {
            if (SafeCanHandle(provider, uri))
                return provider;
        }

        // 2) Fallback to scheme equality
        foreach (var provider in snapshot)
        {
            if (provider.Scheme.Equals(uri.Scheme))
                return provider;
        }

        return null;
    }

    /// <inheritdoc />
    public IEnumerable<IStorageProvider> GetAvailableProviders()
    {
        lock (_sync)
        {
            return _providers.ToArray();
        }
    }

    /// <inheritdoc />
    public void RegisterProvider(IStorageProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);

        lock (_sync)
        {
            // Idempotent by instance to avoid accidental duplicate registrations.
            if (_providers.Contains(provider))
                return;

            _providers.Add(provider);
        }
    }

    /// <inheritdoc />
    public void EnsureDiscovered(CancellationToken cancellationToken = default)
    {
        // Auto-discovery removed: method retained for interface compatibility only.
    }

    private static bool SafeCanHandle(IStorageProvider provider, StorageUri uri)
    {
        try
        {
            return provider.CanHandle(uri);
        }
        catch
        {
            return false;
        }
    }
}

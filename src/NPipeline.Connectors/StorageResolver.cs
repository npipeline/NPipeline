using NPipeline.Connectors.Abstractions;

namespace NPipeline.Connectors;

/// <summary>
///     Default implementation of <see cref="IStorageResolver" /> with lazy provider discovery,
///     thread-safe registration, and scheme-based resolution.
/// </summary>
/// <remarks>
///     - No DI dependency: works in any application architecture
///     - Lazy reflection-based discovery to minimize startup overhead
///     - Manual registration supported for custom scenarios and tests
///     - Resolution strategy:
///     1) Prefer providers whose <see cref="IStorageProvider.CanHandle(StorageUri)" /> returns true
///     2) Fallback to first provider whose <see cref="IStorageProvider.Scheme" /> matches <see cref="StorageUri.Scheme" />
/// </remarks>
public sealed class StorageResolver : IStorageResolver
{
    private readonly List<IStorageProvider> _providers = [];
    private readonly object _sync = new();
    private volatile bool _discovered;

    /// <summary>Create a new resolver.</summary>
    public StorageResolver()
    {
    }

    /// <inheritdoc />
    public IStorageProvider? ResolveProvider(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        EnsureDiscovered();

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
        EnsureDiscovered();

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
            // Idempotent by instance and by type to avoid duplicates when discovery also finds it.
            if (_providers.Contains(provider))
                return;

            if (_providers.Any(p => p.GetType() == provider.GetType()))
                return;

            _providers.Add(provider);
        }
    }

    /// <inheritdoc />
    public void EnsureDiscovered(CancellationToken cancellationToken = default)
    {
        if (_discovered)
            return;

        lock (_sync)
        {
            if (_discovered)
                return;

            DiscoverProviders(cancellationToken);
            _discovered = true;
        }
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

    private void DiscoverProviders(CancellationToken ct)
    {
        // Discover providers from already-loaded assemblies.
        // This avoids scanning disk and keeps discovery fast and deterministic.
        var assemblies = AppDomain.CurrentDomain.GetAssemblies();

        foreach (var asm in assemblies)
        {
            ct.ThrowIfCancellationRequested();

            if (asm.IsDynamic)
                continue;

            Type[] types;

            try
            {
                types = asm.GetExportedTypes();
            }
            catch
            {
                // Skip assemblies that cannot be reflected.
                continue;
            }

            foreach (var type in types)
            {
                ct.ThrowIfCancellationRequested();

                if (type.IsAbstract || type.IsInterface)
                    continue;

                if (!typeof(IStorageProvider).IsAssignableFrom(type))
                    continue;

                // Only auto-register providers that have a public parameterless constructor.
                var ctor = type.GetConstructor(Type.EmptyTypes);

                if (ctor is null)
                    continue;

                // Avoid duplicates by type
                if (_providers.Any(p => p.GetType() == type))
                    continue;

                try
                {
                    if (Activator.CreateInstance(type) is IStorageProvider instance)
                        _providers.Add(instance);
                }
                catch
                {
                    // Skip types that fail to construct
                }
            }
        }
    }
}

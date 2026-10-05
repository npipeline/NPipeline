using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders;

/// <summary>
///     An immutable <see cref="IStorageResolver" /> that looks a provider up by URI scheme. Two providers that claim
///     the same scheme are a configuration error and fail at construction.
/// </summary>
public sealed class StorageResolver : IStorageResolver
{
    private static readonly Lazy<StorageResolver> DefaultResolver = new(
        static () => new StorageResolver([new FileSystemStorageProvider()]),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly FrozenDictionary<StorageScheme, IStorageProvider> _providersByScheme;

    /// <summary>Creates a resolver over <paramref name="providers" />.</summary>
    /// <param name="providers">The providers to route to.</param>
    /// <exception cref="ArgumentException">Two providers claim the same scheme.</exception>
    public StorageResolver(IEnumerable<IStorageProvider> providers)
    {
        ArgumentNullException.ThrowIfNull(providers);

        var distinct = new List<IStorageProvider>();
        var providersByScheme = new Dictionary<StorageScheme, IStorageProvider>();

        foreach (var provider in providers)
        {
            ArgumentNullException.ThrowIfNull(provider, nameof(providers));

            // The same instance listed twice is harmless.
            if (distinct.Contains(provider))
                continue;

            distinct.Add(provider);

            foreach (var scheme in provider.Schemes)
            {
                if (!providersByScheme.TryAdd(scheme, provider))
                {
                    throw new ArgumentException(
                        $"Providers '{providersByScheme[scheme].Name}' and '{provider.Name}' both serve the '{scheme}' scheme. Register one of them, or give one a different scheme.",
                        nameof(providers));
                }
            }
        }

        Providers = distinct.AsReadOnly();
        _providersByScheme = providersByScheme.ToFrozenDictionary();
    }

    /// <summary>A shared resolver that serves the file system only.</summary>
    public static StorageResolver Default => DefaultResolver.Value;

    /// <inheritdoc />
    public IReadOnlyCollection<IStorageProvider> Providers { get; }

    /// <inheritdoc />
    public IStorageProvider Resolve(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        return _providersByScheme.TryGetValue(uri.Scheme, out var provider)
            ? provider
            : throw new StorageProviderNotFoundException(uri);
    }

    /// <inheritdoc />
    public bool TryResolve(StorageUri uri, [NotNullWhen(true)] out IStorageProvider? provider)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return _providersByScheme.TryGetValue(uri.Scheme, out provider);
    }
}

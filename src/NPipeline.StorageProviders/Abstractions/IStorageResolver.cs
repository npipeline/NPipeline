using System.Diagnostics.CodeAnalysis;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Abstractions;

/// <summary>Routes a <see cref="StorageUri" /> to the <see cref="IStorageProvider" /> that serves its scheme.</summary>
public interface IStorageResolver
{
    /// <summary>The providers this resolver routes to.</summary>
    IReadOnlyCollection<IStorageProvider> Providers { get; }

    /// <summary>Returns the provider for the URI's scheme.</summary>
    /// <param name="uri">The URI to resolve.</param>
    /// <exception cref="Exceptions.StorageProviderNotFoundException">No provider serves the scheme.</exception>
    IStorageProvider Resolve(StorageUri uri);

    /// <summary>Returns the provider for the URI's scheme, if there is one.</summary>
    /// <param name="uri">The URI to resolve.</param>
    /// <param name="provider">The provider, when found.</param>
    bool TryResolve(StorageUri uri, [NotNullWhen(true)] out IStorageProvider? provider);
}

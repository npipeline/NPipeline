using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Aws.Redshift;

/// <summary>
///     Factory class for creating storage resolvers with Redshift support.
/// </summary>
/// <remarks>
///     This factory provides a convenient way to create a <see cref="StorageResolver" />
///     pre-configured with the <see cref="RedshiftDatabaseStorageProvider" />.
///     This allows Redshift connector nodes to use a default resolver that includes
///     the Redshift provider without requiring manual registration.
/// </remarks>
public static class RedshiftStorageResolverFactory
{
    /// <summary>
    ///     Creates a new <see cref="StorageResolver" /> with the Redshift provider registered.
    /// </summary>
    /// <returns>
    ///     A <see cref="StorageResolver" /> instance with <see cref="RedshiftDatabaseStorageProvider" /> registered.
    /// </returns>
    /// <remarks>
    ///     The returned resolver can be used to resolve Redshift storage URIs to
    ///     <see cref="RedshiftDatabaseStorageProvider" /> instances. Additional providers
    ///     can be registered on the returned resolver if needed.
    /// </remarks>
    public static StorageResolver CreateResolver()
    {
        var resolver = new StorageResolver();
        resolver.RegisterProvider(new RedshiftDatabaseStorageProvider());
        return resolver;
    }

    /// <summary>
    ///     Registers the Redshift provider with an existing resolver.
    /// </summary>
    /// <param name="resolver">The resolver to register with.</param>
    /// <returns>The resolver for method chaining.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="resolver" /> is null.</exception>
    public static IStorageResolver AddRedshiftProvider(this IStorageResolver resolver)
    {
        ArgumentNullException.ThrowIfNull(resolver);

        resolver.RegisterProvider(new RedshiftDatabaseStorageProvider());
        return resolver;
    }
}

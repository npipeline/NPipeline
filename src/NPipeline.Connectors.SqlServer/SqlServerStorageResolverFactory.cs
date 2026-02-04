using NPipeline.Connectors.Abstractions;

namespace NPipeline.Connectors.SqlServer;

/// <summary>
///     Factory class for creating storage resolvers with SQL Server support.
/// </summary>
/// <remarks>
///     This factory provides a convenient way to create a <see cref="StorageResolver" />
///     pre-configured with the <see cref="SqlServerDatabaseStorageProvider" />.
///     This allows SQL Server connector nodes to use a default resolver that includes
///     the SQL Server provider without requiring manual registration.
/// </remarks>
public static class SqlServerStorageResolverFactory
{
    /// <summary>
    ///     Creates a new <see cref="StorageResolver" /> with the SQL Server provider registered.
    /// </summary>
    /// <returns>
    ///     A <see cref="StorageResolver" /> instance with <see cref="SqlServerDatabaseStorageProvider" /> registered.
    /// </returns>
    /// <remarks>
    ///     The returned resolver can be used to resolve SQL Server storage URIs to
    ///     <see cref="SqlServerDatabaseStorageProvider" /> instances. Additional providers
    ///     can be registered on the returned resolver if needed.
    /// </remarks>
    public static StorageResolver CreateResolver()
    {
        var resolver = new StorageResolver();
        resolver.RegisterProvider(new SqlServerDatabaseStorageProvider());
        return resolver;
    }
}

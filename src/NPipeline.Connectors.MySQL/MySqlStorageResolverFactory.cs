using NPipeline.StorageProviders;

namespace NPipeline.Connectors.MySql;

/// <summary>
///     Factory for creating <see cref="StorageResolver" /> instances pre-configured with MySQL support.
/// </summary>
public static class MySqlStorageResolverFactory
{
    /// <summary>
    ///     Creates a <see cref="StorageResolver" /> with <see cref="MySqlDatabaseStorageProvider" /> registered.
    /// </summary>
    public static StorageResolver CreateResolver()
    {
        return new StorageResolver([new MySqlDatabaseStorageProvider()]);
    }
}

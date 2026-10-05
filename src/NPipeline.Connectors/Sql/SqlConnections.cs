using System.Data.Common;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Sql;

/// <summary>Opens the connection a SQL node's options name: a connection string, or a database URI through its storage provider.</summary>
internal static class SqlConnections
{
    public static async Task<DbConnection> OpenAsync(
        SqlNodeOptions options,
        Func<string, DbConnection> create,
        Func<IStorageResolver> defaultResolver,
        CancellationToken cancellationToken)
    {
        var connectionString = options.ConnectionString ?? ResolveConnectionString(options, defaultResolver);
        var connection = create(connectionString);

        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static string ResolveConnectionString(SqlNodeOptions options, Func<IStorageResolver> defaultResolver)
    {
        var uri = options.Uri ?? throw new InvalidOperationException("The options name no connection string or URI.");
        var provider = options.Provider ?? (options.Resolver ?? defaultResolver()).Resolve(uri);

        return provider is IDatabaseStorageProvider database
            ? database.GetConnectionString(uri)
            : throw new InvalidOperationException($"The provider for '{uri.Scheme}' is not a database provider ({nameof(IDatabaseStorageProvider)}).");
    }
}

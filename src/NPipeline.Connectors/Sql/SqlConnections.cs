using System.Data.Common;
using NPipeline.Connectors.Database;

namespace NPipeline.Connectors.Sql;

/// <summary>Opens the connection a SQL node's options name: a connection string, or a database URI through its connection provider.</summary>
internal static class SqlConnections
{
    public static async Task<DbConnection> OpenAsync(
        SqlNodeOptions options,
        Func<string, DbConnection> create,
        Func<IDatabaseConnectionProvider> defaultProvider,
        CancellationToken cancellationToken)
    {
        var connectionString = options.ConnectionString ?? ResolveConnectionString(options, defaultProvider);
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

    private static string ResolveConnectionString(SqlNodeOptions options, Func<IDatabaseConnectionProvider> defaultProvider)
    {
        var uri = options.Uri ?? throw new InvalidOperationException("The options name no connection string or URI.");
        var provider = options.Provider ?? defaultProvider();

        return provider.GetConnectionString(uri);
    }
}

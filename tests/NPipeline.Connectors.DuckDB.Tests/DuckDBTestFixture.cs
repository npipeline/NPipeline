using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Mapping;
using NPipeline.Connectors.DuckDB.Writers;

namespace NPipeline.Connectors.DuckDB.Tests;

/// <summary>
///     Shared test fixture for DuckDB connector tests.
///     Provides temp database lifecycle and helper methods.
/// </summary>
public sealed class DuckDBTestFixture : IAsyncLifetime
{
    public string TestDatabasePath { get; } = DuckDBTestHelper.GetTempDatabasePath();

    public Task InitializeAsync()
    {
        var directory = Path.GetDirectoryName(TestDatabasePath);

        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        DuckDBTestHelper.CleanupDatabase(TestDatabasePath);
        return Task.CompletedTask;
    }

    public static DuckDBConnection CreateInMemoryConnection()
    {
        return DuckDBTestHelper.CreateInMemoryConnection();
    }

    public static async Task SeedTableAsync<T>(DuckDBConnection connection, string tableName, IEnumerable<T> records)
    {
        var ddl = DuckDBSchemaBuilder.BuildCreateTable<T>(tableName);

        await using (var create = connection.CreateCommand())
        {
            create.CommandText = ddl;
            await create.ExecuteNonQueryAsync();
        }

        await using var writer = new DuckDBAppenderWriter<T>(connection, tableName);

        foreach (var record in records)
        {
            await writer.WriteAsync(record, CancellationToken.None);
        }

        await writer.FlushAsync(CancellationToken.None);
    }
}

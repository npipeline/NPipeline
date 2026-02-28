using DuckDB.NET.Data;

namespace NPipeline.Connectors.DuckDB.Tests;

/// <summary>
///     Helper class providing in-memory DuckDB connections and utilities for tests.
/// </summary>
public static class DuckDBTestHelper
{
    /// <summary>
    ///     Creates an in-memory DuckDB connection. Caller is responsible for disposal.
    /// </summary>
    public static DuckDBConnection CreateInMemoryConnection()
    {
        var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        return connection;
    }

    /// <summary>
    ///     Returns a unique temp file path for a DuckDB database.
    /// </summary>
    public static string GetTempDatabasePath()
    {
        return Path.Combine(Path.GetTempPath(), $"npipeline_test_{Guid.NewGuid():N}.duckdb");
    }

    /// <summary>
    ///     Deletes a temp database file and its related files (.wal etc.).
    /// </summary>
    public static void CleanupDatabase(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);

            if (File.Exists(path + ".wal"))
                File.Delete(path + ".wal");
        }
        catch
        {
            // Best effort cleanup
        }
    }

    /// <summary>
    ///     Creates a test table and inserts sample data.
    /// </summary>
    public static void SeedTestRecords(DuckDBConnection connection, string tableName, int count)
    {
        using var cmd = connection.CreateCommand();

        cmd.CommandText = $"""
                           CREATE TABLE IF NOT EXISTS "{tableName}" (
                               "Id" INTEGER,
                               "Name" VARCHAR,
                               "Value" DOUBLE
                           )
                           """;

        cmd.ExecuteNonQuery();

        for (var i = 1; i <= count; i++)
        {
            using var insertCmd = connection.CreateCommand();
            insertCmd.CommandText = $"""INSERT INTO "{tableName}" VALUES ({i}, 'Item{i}', {i * 1.5})""";
            insertCmd.ExecuteNonQuery();
        }
    }

    /// <summary>
    ///     Reads the row count from a table.
    /// </summary>
    public static long GetRowCount(DuckDBConnection connection, string tableName)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM \"{tableName}\"";
        return (long)cmd.ExecuteScalar()!;
    }
}

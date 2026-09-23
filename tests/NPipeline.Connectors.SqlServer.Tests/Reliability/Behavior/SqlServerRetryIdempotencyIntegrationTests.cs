using AwesomeAssertions;
using Microsoft.Data.SqlClient;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.Connectors.SqlServer.Reliability;
using NPipeline.Connectors.SqlServer.Tests.Fixtures;
using NPipeline.Connectors.SqlServer.Writers;
using NPipeline.StorageProviders.Abstractions;
using NResilience;

namespace NPipeline.Connectors.SqlServer.Tests.Reliability.Behavior;

/// <summary>
///     SQL1: a write that fails and is retried must not insert rows an earlier attempt already committed. Each test makes
///     the fifth of six rows fail once (a unique-index clash with a blocker row, which the retry listener then removes)
///     and asserts the table ends up with each row exactly once.
/// </summary>
[Collection("SqlServer")]
public sealed class SqlServerRetryIdempotencyIntegrationTests(SqlServerTestContainerFixture fixture)
{
    [Theory]
    [InlineData(SqlServerWriteStrategy.PerRow)]
    [InlineData(SqlServerWriteStrategy.Batch)]
    [InlineData(SqlServerWriteStrategy.BulkCopy)]
    public async Task ARetriedWriteLeavesEachRowExactlyOnce(SqlServerWriteStrategy strategy)
    {
        var table = $"retry_{strategy.ToString().ToLowerInvariant()}";
        await ExecuteAsync($"IF OBJECT_ID('dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.{table}");
        await ExecuteAsync($"CREATE TABLE dbo.{table} (Id INT NOT NULL, Name NVARCHAR(50) NOT NULL)");

        // Only row5 is unique, so a blocker row makes it fail while every other row could be inserted twice.
        await ExecuteAsync($"CREATE UNIQUE INDEX ux_{table} ON dbo.{table} (Name) WHERE Name = 'row5'");
        await ExecuteAsync($"INSERT INTO dbo.{table} (Id, Name) VALUES (0, 'row5')");

        var retries = 0;

        var resilience = (SqlServerConnectorResilience.Default with
        {
            Backoff = Backoff.Default with { TransientBase = TimeSpan.FromMilliseconds(1) },

            // 2601 (duplicate key in a unique index) stands in for a transient failure here.
            Classifier = SqlServerConnectorResilience.Classifier.On<SqlException>(e => e.Number == 2601 ? Verdict.Transient : Verdict.Permanent),
        }).WithListener(e =>
        {
            if (e.Kind != CallEventKind.Retrying)
                return;

            retries++;
            Execute($"DELETE FROM dbo.{table} WHERE Id = 0");
        });

        var configuration = new SqlServerConfiguration
        {
            Resilience = resilience,
            BatchSize = 6,
            BulkCopyBatchSize = 6,
        };

        await using (var connection = await OpenAsync())
        {
            var writer = CreateWriter(strategy, connection, table, configuration);
            await writer.WriteBatchAsync(Enumerable.Range(1, 6).Select(i => new Row { Id = i, Name = $"row{i}" }));
            await writer.DisposeAsync();
        }

        retries.Should().Be(1);

        var ids = await QueryIdsAsync(table);
        ids.Should().Equal(1, 2, 3, 4, 5, 6);

        await ExecuteAsync($"DROP TABLE dbo.{table}");
    }

    private static IDatabaseWriter<Row> CreateWriter(SqlServerWriteStrategy strategy, IDatabaseConnection connection, string table,
        SqlServerConfiguration configuration)
    {
        return strategy switch
        {
            SqlServerWriteStrategy.PerRow => new SqlServerPerRowWriter<Row>(connection, "dbo", table, null, configuration),
            SqlServerWriteStrategy.Batch => new SqlServerBatchWriter<Row>(connection, "dbo", table, null, configuration),
            _ => new SqlServerBulkCopyWriter<Row>(connection, "dbo", table, null, configuration),
        };
    }

    private async Task<SqlServerDatabaseConnection> OpenAsync()
    {
        var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        return new SqlServerDatabaseConnection(connection);
    }

    private async Task<List<int>> QueryIdsAsync(string table)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"SELECT Id FROM dbo.{table} ORDER BY Id", connection);
        await using var reader = await command.ExecuteReaderAsync();

        var ids = new List<int>();

        while (await reader.ReadAsync())
        {
            ids.Add(reader.GetInt32(0));
        }

        return ids;
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        _ = await command.ExecuteNonQueryAsync();
    }

    private void Execute(string sql)
    {
        using var connection = new SqlConnection(fixture.ConnectionString);
        connection.Open();
        using var command = new SqlCommand(sql, connection);
        _ = command.ExecuteNonQuery();
    }

    private sealed class Row
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}

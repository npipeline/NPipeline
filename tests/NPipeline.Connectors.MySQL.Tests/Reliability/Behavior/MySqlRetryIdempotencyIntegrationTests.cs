using AwesomeAssertions;
using MySqlConnector;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Connection;
using NPipeline.Connectors.MySql.Reliability;
using NPipeline.Connectors.MySql.Tests.Fixtures;
using NPipeline.Connectors.MySql.Writers;
using NPipeline.StorageProviders.Abstractions;
using NResilience;

namespace NPipeline.Connectors.MySql.Tests.Reliability.Behavior;

/// <summary>
///     SQL1: a write that fails and is retried must not insert rows an earlier attempt already committed. A trigger fails
///     the fifth of six rows while a flag row exists (SIGNAL, error 1644, stands in for a transient error); the retry
///     listener removes the flag. The table must end up with each row exactly once.
/// </summary>
[Collection("MySql")]
public sealed class MySqlRetryIdempotencyIntegrationTests(MySqlTestContainerFixture fixture)
{
    private string ConnectionString => new MySqlConnectionStringBuilder(fixture.ConnectionString) { AllowLoadLocalInfile = true }.ConnectionString;

    [Theory]
    [InlineData(MySqlWriteStrategy.PerRow)]
    [InlineData(MySqlWriteStrategy.Batch)]
    [InlineData(MySqlWriteStrategy.BulkLoad)]
    public async Task ARetriedWriteLeavesEachRowExactlyOnce(MySqlWriteStrategy strategy)
    {
        var table = $"retry_{strategy.ToString().ToLowerInvariant()}";
        var flag = $"{table}_fail";

        if (strategy == MySqlWriteStrategy.BulkLoad)
            await ExecuteAsync("SET GLOBAL local_infile = 1");

        await ExecuteAsync($"DROP TABLE IF EXISTS {table}");
        await ExecuteAsync($"DROP TABLE IF EXISTS {flag}");
        await ExecuteAsync($"CREATE TABLE {table} (Id INT NOT NULL, Name VARCHAR(50) NOT NULL) ENGINE = InnoDB");
        await ExecuteAsync($"CREATE TABLE {flag} (Id INT NOT NULL) ENGINE = InnoDB");
        await ExecuteAsync($"INSERT INTO {flag} (Id) VALUES (1)");

        await ExecuteAsync($"""
                            CREATE TRIGGER {table}_inject BEFORE INSERT ON {table} FOR EACH ROW
                            BEGIN
                                IF NEW.Name = 'row5' AND EXISTS (SELECT 1 FROM {flag}) THEN
                                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected';
                                END IF;
                            END
                            """);

        var retries = 0;

        var resilience = (MySqlConnectorResilience.Default with
        {
            Backoff = Backoff.Default with { TransientBase = TimeSpan.FromMilliseconds(1) },
            Classifier = MySqlConnectorResilience.Classifier.On<MySqlException>(e => e.Number == 1644
                ? Verdict.Transient
                : Verdict.Permanent),
        }).WithListener(e =>
        {
            if (e.Kind != CallEventKind.Retrying)
                return;

            retries++;
            Execute($"DELETE FROM {flag}");
        });

        var configuration = new MySqlConfiguration { Resilience = resilience, BatchSize = 6, BulkLoadBatchSize = 6 };

        await using (var connection = await OpenAsync())
        {
            var writer = CreateWriter(strategy, connection, table, configuration);
            await writer.WriteBatchAsync(Enumerable.Range(1, 6).Select(i => new Row { Id = i, Name = $"row{i}" }));
            await writer.DisposeAsync();
        }

        retries.Should().Be(1);
        (await QueryIdsAsync(table)).Should().Equal(1, 2, 3, 4, 5, 6);

        await ExecuteAsync($"DROP TABLE {table}");
        await ExecuteAsync($"DROP TABLE {flag}");
    }

    private static IDatabaseWriter<Row> CreateWriter(MySqlWriteStrategy strategy, IDatabaseConnection connection, string table,
        MySqlConfiguration configuration)
    {
        return strategy switch
        {
            MySqlWriteStrategy.PerRow => new MySqlPerRowWriter<Row>(connection, table, null, configuration),
            MySqlWriteStrategy.Batch => new MySqlBatchWriter<Row>(connection, table, null, configuration),
            _ => new MySqlBulkLoadWriter<Row>(connection, table, null, configuration),
        };
    }

    private async Task<MySqlDatabaseConnection> OpenAsync()
    {
        var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync();
        return new MySqlDatabaseConnection(connection);
    }

    private async Task<List<int>> QueryIdsAsync(string table)
    {
        await using var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new MySqlCommand($"SELECT Id FROM {table} ORDER BY Id", connection);
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
        await using var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new MySqlCommand(sql, connection);
        _ = await command.ExecuteNonQueryAsync();
    }

    private void Execute(string sql)
    {
        using var connection = new MySqlConnection(ConnectionString);
        connection.Open();
        using var command = new MySqlCommand(sql, connection);
        _ = command.ExecuteNonQuery();
    }

    private sealed class Row
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}

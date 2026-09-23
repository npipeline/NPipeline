using AwesomeAssertions;
using Npgsql;
using NPipeline.Connectors.Postgres.Configuration;
using NPipeline.Connectors.Postgres.Connection;
using NPipeline.Connectors.Postgres.Nodes;
using NPipeline.Connectors.Postgres.Reliability;
using NPipeline.Connectors.Postgres.Tests.Fixtures;
using NPipeline.Connectors.Postgres.Writers;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;
using NResilience;
using PipelinePostgresException = NPipeline.Connectors.Postgres.Exceptions.PostgresException;

namespace NPipeline.Connectors.Postgres.Tests.Reliability.Behavior;

/// <summary>
///     Retries against a real server. SQL1: a write that fails and is retried must not insert rows an earlier attempt
///     already committed. The source retries until its reader opens, and not once rows are flowing.
/// </summary>
[Collection("PostgresTestCollection")]
public sealed class PostgresRetryIntegrationTests(PostgresTestContainerFixture fixture)
{
    // Binary COPY is not covered: its statement specifies DELIMITER, which PostgreSQL rejects in BINARY mode (a separate,
    // pre-existing defect).
    [Theory]
    [InlineData(PostgresWriteStrategy.PerRow, false)]
    [InlineData(PostgresWriteStrategy.Batch, false)]
    [InlineData(PostgresWriteStrategy.Copy, false)]
    public async Task ARetriedWriteLeavesEachRowExactlyOnce(PostgresWriteStrategy strategy, bool binaryCopy)
    {
        // The fifth of six rows clashes with a blocker row once; the retry listener removes the blocker. Only row5 is
        // unique, so any row an earlier attempt committed would show up twice.
        var table = $"retry_{strategy.ToString().ToLowerInvariant()}_{(binaryCopy ? "binary" : "text")}";

        // The writers quote "schema.table" as one identifier (a separate, pre-existing defect), so the test's table carries
        // that literal name.
        var quoted = $"\"public.{table}\"";
        await ExecuteAsync($"DROP TABLE IF EXISTS {table}");
        await ExecuteAsync($"DROP TABLE IF EXISTS {quoted}");
        await ExecuteAsync($"CREATE TABLE {quoted} (id INT NOT NULL, name TEXT NOT NULL)");
        await ExecuteAsync($"CREATE UNIQUE INDEX ux_{table} ON {quoted} (name) WHERE name = 'row5'");
        await ExecuteAsync($"INSERT INTO {quoted} (id, name) VALUES (0, 'row5')");

        var retries = 0;

        var resilience = Transient("23505").WithListener(e =>
        {
            if (e.Kind != CallEventKind.Retrying)
                return;

            retries++;
            Execute($"DELETE FROM {quoted} WHERE id = 0");
        });

        var configuration = new PostgresConfiguration { Resilience = resilience, BatchSize = 6, UseBinaryCopy = binaryCopy };

        await using (var connection = await OpenAsync())
        {
            var writer = CreateWriter(strategy, connection, table, configuration);
            await writer.WriteBatchAsync(Enumerable.Range(1, 6).Select(i => new Row { Id = i, Name = $"row{i}" }));
            await writer.DisposeAsync();
        }

        retries.Should().Be(1);
        (await QueryIdsAsync($"SELECT id FROM {quoted} ORDER BY id")).Should().Equal(1, 2, 3, 4, 5, 6);

        await ExecuteAsync($"DROP TABLE {quoted}");
    }

    [Fact]
    public async Task Source_RetriesAFailureBeforeTheReaderOpens()
    {
        await ExecuteAsync("DROP TABLE IF EXISTS retry_source_late");
        var attempts = 0;

        // The table does not exist on the first attempt (42P01, standing in for a transient error); the retry creates it.
        var resilience = Transient("42P01").WithListener(e =>
        {
            if (e.Kind != CallEventKind.Retrying)
                return;

            attempts++;
            Execute("CREATE TABLE retry_source_late AS SELECT g AS id FROM generate_series(1, 3) g");
        });

        var rows = await ReadAsync("SELECT id FROM retry_source_late ORDER BY id", resilience);

        attempts.Should().Be(1);
        rows.Should().Equal(1, 2, 3);

        await ExecuteAsync("DROP TABLE retry_source_late");
    }

    [Fact]
    public async Task Source_DoesNotRetryAFailureAfterTheFirstRow()
    {
        var retries = 0;

        // Division by zero on the third row (22012, standing in for a transient error). Rerunning the query would emit
        // rows 1 and 2 twice, so the failure surfaces.
        var resilience = Transient("22012").WithListener(e =>
        {
            if (e.Kind == CallEventKind.Retrying)
                retries++;
        });

        var emitted = new List<int>();
        var act = () => ReadAsync("SELECT 10 / (3 - g) AS id FROM generate_series(1, 5) g", resilience, emitted);

        _ = await act.Should().ThrowAsync<Exception>();
        emitted.Should().Equal(5, 10);
        retries.Should().Be(0);
    }

    [Fact]
    public async Task Source_StopsWithoutRetryingWhenThePipelineIsCancelled()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var act = () => ReadAsync("SELECT 1 AS id", PostgresConnectorResilience.Default, cancellationToken: cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
    }

    private static NResilience.Resilience Transient(string sqlState)
    {
        return PostgresConnectorResilience.Default with
        {
            Backoff = Backoff.Default with { TransientBase = TimeSpan.FromMilliseconds(1) },
            Classifier = PostgresConnectorResilience.Classifier
                .On<Npgsql.PostgresException>(e => e.SqlState == sqlState ? Verdict.Transient : Verdict.Permanent)
                .On<PipelinePostgresException>(e => e.ErrorCode == sqlState ? Verdict.Transient : Verdict.Permanent),
        };
    }

    private async Task<List<int>> ReadAsync(string sql, NResilience.Resilience resilience, List<int>? emitted = null,
        CancellationToken cancellationToken = default)
    {
        emitted ??= [];

        var source = new PostgresSourceNode<int>(fixture.ConnectionString, sql, row => row.Get<int>(0),
            new PostgresConfiguration { Resilience = resilience });

        await foreach (var id in source.OpenStream(new PipelineContext(), cancellationToken).WithCancellation(cancellationToken))
        {
            emitted.Add(id);
        }

        return emitted;
    }

    private static IDatabaseWriter<Row> CreateWriter(PostgresWriteStrategy strategy, IDatabaseConnection connection, string table,
        PostgresConfiguration configuration)
    {
        return strategy switch
        {
            PostgresWriteStrategy.PerRow => new PostgresPerRowWriter<Row>(connection, "public", table, null, configuration),
            PostgresWriteStrategy.Batch => new PostgresBatchWriter<Row>(connection, "public", table, null, configuration),
            _ => new PostgresCopyWriter<Row>(connection, "public", table, null, configuration),
        };
    }

    private async Task<PostgresDatabaseConnection> OpenAsync()
    {
        var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        return new PostgresDatabaseConnection(connection);
    }

    private async Task<List<int>> QueryIdsAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
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
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        _ = await command.ExecuteNonQueryAsync();
    }

    private void Execute(string sql)
    {
        using var connection = new NpgsqlConnection(fixture.ConnectionString);
        connection.Open();
        using var command = new NpgsqlCommand(sql, connection);
        _ = command.ExecuteNonQuery();
    }

    private sealed class Row
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}

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
    [Theory]
    [InlineData(PostgresWriteStrategy.PerRow, false, "public")]
    [InlineData(PostgresWriteStrategy.Batch, false, "public")]
    [InlineData(PostgresWriteStrategy.Copy, false, "public")]
    [InlineData(PostgresWriteStrategy.Copy, true, "public")]
    [InlineData(PostgresWriteStrategy.PerRow, false, "retry_sales")]
    [InlineData(PostgresWriteStrategy.Batch, false, "retry_sales")]
    [InlineData(PostgresWriteStrategy.Copy, false, "retry_sales")]
    [InlineData(PostgresWriteStrategy.Copy, true, "retry_sales")]
    public async Task ARetriedWriteLeavesEachRowExactlyOnce(PostgresWriteStrategy strategy, bool binaryCopy, string schema)
    {
        // The fifth of six rows clashes with a blocker row once; the retry listener removes the blocker. Only row5 is
        // unique, so any row an earlier attempt committed would show up twice.
        var table = $"retry_{strategy.ToString().ToLowerInvariant()}_{(binaryCopy ? "binary" : "text")}";
        var qualified = await CreateTableAsync(schema, table);
        await ExecuteAsync($"CREATE UNIQUE INDEX ON {qualified} (name) WHERE name = 'row5'");
        await ExecuteAsync($"INSERT INTO {qualified} (id, name) VALUES (0, 'row5')");

        var retries = 0;

        var resilience = Transient("23505").WithListener(e =>
        {
            if (e.Kind != CallEventKind.Retrying)
                return;

            retries++;
            Execute($"DELETE FROM {qualified} WHERE id = 0");
        });

        var configuration = new PostgresConfiguration { Resilience = resilience, BatchSize = 6, UseBinaryCopy = binaryCopy };

        await WriteAsync(strategy, schema, table, configuration, 6);

        retries.Should().Be(1);
        (await QueryIdsAsync($"SELECT id FROM {qualified} ORDER BY id")).Should().Equal(1, 2, 3, 4, 5, 6);

        await ExecuteAsync($"DROP TABLE {qualified}");
    }

    [Theory]
    [InlineData(PostgresWriteStrategy.PerRow, false)]
    [InlineData(PostgresWriteStrategy.Batch, false)]
    [InlineData(PostgresWriteStrategy.Copy, false)]
    [InlineData(PostgresWriteStrategy.Copy, true)]
    public async Task Writers_QuoteSchemaAndTableSeparately_EscapingEmbeddedQuotes(PostgresWriteStrategy strategy, bool binaryCopy)
    {
        // A schema with a space and upper case, and a table with a double quote in its name: each part must be quoted on
        // its own, with the embedded quote doubled.
        const string schema = "Retry Sales";
        var table = $"odd\"{strategy.ToString().ToLowerInvariant()}_{(binaryCopy ? "binary" : "text")}";
        var qualified = await CreateTableAsync(schema, table);

        var configuration = new PostgresConfiguration
        {
            Resilience = NResilience.Resilience.None,
            BatchSize = 3,
            UseBinaryCopy = binaryCopy,
        };

        await WriteAsync(strategy, schema, table, configuration, 3);

        (await QueryIdsAsync($"SELECT id FROM {qualified} ORDER BY id")).Should().Equal(1, 2, 3);

        await ExecuteAsync($"DROP TABLE {qualified}");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Copy_IsBoundedByCopyTimeout(bool binaryCopy)
    {
        // A trigger makes the server spend three seconds on the rows; a one-second CopyTimeout must give up first.
        var table = $"retry_copy_timeout_{(binaryCopy ? "binary" : "text")}";
        var qualified = await CreateTableAsync("public", table);

        await ExecuteAsync($"""
                            CREATE OR REPLACE FUNCTION {table}_slow() RETURNS trigger AS $$
                            BEGIN PERFORM pg_sleep(1); RETURN NEW; END; $$ LANGUAGE plpgsql
                            """);
        await ExecuteAsync($"CREATE TRIGGER {table}_slow BEFORE INSERT ON {qualified} FOR EACH ROW EXECUTE FUNCTION {table}_slow()");

        var configuration = new PostgresConfiguration
        {
            Resilience = NResilience.Resilience.None,
            BatchSize = 3,
            UseBinaryCopy = binaryCopy,
            CopyTimeout = 1,
        };

        var started = DateTime.UtcNow;
        var act = () => WriteAsync(PostgresWriteStrategy.Copy, "public", table, configuration, 3);

        _ = await act.Should().ThrowAsync<Exception>();
        (DateTime.UtcNow - started).Should().BeLessThan(TimeSpan.FromSeconds(2.9));

        await ExecuteAsync($"DROP TABLE {qualified}");
        await ExecuteAsync($"DROP FUNCTION {table}_slow()");
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

    private async Task WriteAsync(PostgresWriteStrategy strategy, string schema, string table, PostgresConfiguration configuration,
        int rows)
    {
        await using var connection = await OpenAsync();

        IDatabaseWriter<Row> writer = strategy switch
        {
            PostgresWriteStrategy.PerRow => new PostgresPerRowWriter<Row>(connection, schema, table, null, configuration),
            PostgresWriteStrategy.Batch => new PostgresBatchWriter<Row>(connection, schema, table, null, configuration),
            _ => new PostgresCopyWriter<Row>(connection, schema, table, null, configuration),
        };

        try
        {
            await writer.WriteBatchAsync(Enumerable.Range(1, rows).Select(i => new Row { Id = i, Name = $"row{i}" }));
        }
        finally
        {
            await writer.DisposeAsync();
        }
    }

    /// <summary>Creates schema.table, quoting each part, and returns the quoted name.</summary>
    private async Task<string> CreateTableAsync(string schema, string table)
    {
        var qualified = $"{Quote(schema)}.{Quote(table)}";
        await ExecuteAsync($"CREATE SCHEMA IF NOT EXISTS {Quote(schema)}");
        await ExecuteAsync($"DROP TABLE IF EXISTS {qualified}");
        await ExecuteAsync($"CREATE TABLE {qualified} (id INT NOT NULL, name TEXT NOT NULL)");
        return qualified;
    }

    private static string Quote(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
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

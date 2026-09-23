using AwesomeAssertions;
using FakeItEasy;
using Npgsql;
using NPipeline.Connectors.Postgres.Configuration;
using NPipeline.Connectors.Postgres.Reliability;
using NPipeline.Connectors.Postgres.Writers;
using NPipeline.StorageProviders.Abstractions;
using NResilience;
using PipelinePostgresException = NPipeline.Connectors.Postgres.Exceptions.PostgresException;

namespace NPipeline.Connectors.Postgres.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the PostgreSQL connector's NResilience policy (SQL1 and X1 in <c>plans/resilience-improvements.md</c>).
///     They assert what reached the database, not what is configured.
/// </summary>
public sealed class PostgresResilienceBehaviorTests
{
    // The shipped preset with near-zero backoff, so the tests barely wait between attempts.
    private static readonly NResilience.Resilience Fast = PostgresConnectorResilience.Default with
    {
        Backoff = PostgresConnectorResilience.Default.Backoff with
        {
            TransientBase = TimeSpan.FromMilliseconds(1),
            ThrottledBase = TimeSpan.FromMilliseconds(1),
        },
    };

    [Fact]
    public void DefaultPreset_PreservesTheAttemptCountsAndDelaysOfTheSettingsItReplaces()
    {
        var preset = PostgresConnectorResilience.Default;

        // The COPY writer made MaxRetryAttempts + 1 = four calls with RetryDelay = 1 s doubled per retry. (The source
        // made only MaxRetryAttempts = three, an off-by-one against the setting's documented meaning; it now makes four.)
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromSeconds(1));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(30));
        preset.Backoff.ThrottledBase.Should().Be(TimeSpan.FromSeconds(5));

        // A COPY of many rows must not be cut off at NResilience's 10 s attempt timeout or 30 s deadline.
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse("the preset reproduces a fixed attempt count");
        preset.Validate();
    }

    [Theory]
    [InlineData("53300", VerdictKind.Throttled)]
    [InlineData("40001", VerdictKind.Transient)]
    [InlineData("40P01", VerdictKind.Transient)]
    [InlineData("08006", VerdictKind.Transient)]
    [InlineData("57P01", VerdictKind.Transient)]
    [InlineData("23505", VerdictKind.Permanent)]
    [InlineData("42P01", VerdictKind.Permanent)]
    public void Classifier_JudgesServerErrorsBySqlState(string sqlState, VerdictKind expected)
    {
        var exception = new Npgsql.PostgresException("injected", "ERROR", "ERROR", sqlState);

        PostgresConnectorResilience.Classifier.ClassifyException(exception).Kind.Should().Be(expected);
    }

    [Fact]
    public void Classifier_JudgesTheConnectorsOwnWrapperByWhatItCarries()
    {
        var classifier = PostgresConnectorResilience.Classifier;

        classifier.ClassifyException(new PipelinePostgresException("wrapped", "40P01")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new PipelinePostgresException("wrapped", "23505")).Kind.Should().Be(VerdictKind.Permanent);

        classifier.ClassifyException(new PipelinePostgresException("wrapped", new NpgsqlException("io", new IOException())))
            .Kind.Should().Be(VerdictKind.Transient);
    }

    [Fact]
    public void Classifier_FollowsNpgsqlForClientErrors_AndTreatsForeignCancellationAsPermanent()
    {
        var classifier = PostgresConnectorResilience.Classifier;

        classifier.ClassifyException(new NpgsqlException("broken", new IOException())).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new NpgsqlException("protocol violation")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new TimeoutException()).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new OperationCanceledException()).Kind.Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public async Task PerRow_RetriesATransientFailureFourTimesThenThrows()
    {
        var connection = new ScriptedConnection((_, _) => Server("40P01"));
        var writer = PerRowWriter(connection);

        var act = () => writer.WriteAsync(new Row { Id = 1 });

        _ = await act.Should().ThrowAsync<Npgsql.PostgresException>();
        connection.Executed.Should().HaveCount(4);
    }

    [Fact]
    public async Task PerRow_DoesNotRetryAPermanentFailure()
    {
        var connection = new ScriptedConnection((_, _) => Server("23505"));
        var writer = PerRowWriter(connection);

        var act = () => writer.WriteAsync(new Row { Id = 1 });

        _ = await act.Should().ThrowAsync<Npgsql.PostgresException>();
        connection.Executed.Should().ContainSingle();
    }

    [Fact]
    public async Task Batch_RetriesOnlyTheChunkThatFailed()
    {
        var connection = new ScriptedConnection((index, _) => index == 2 ? Server("40001") : null);
        var writer = BatchWriter(connection, batchSize: 2);

        await writer.WriteBatchAsync(Rows(6));

        connection.Executed.Should().HaveCount(4);
        CommittedIds(connection).Should().Equal(1, 2, 3, 4, 5, 6);
    }

    [Fact]
    public async Task Batch_DoesNotResendAFailedChunkWhenDisposed()
    {
        var connection = new ScriptedConnection((_, _) => Server("23505"));
        var writer = BatchWriter(connection, batchSize: 10);

        var act = () => writer.WriteBatchAsync(Rows(3));
        _ = await act.Should().ThrowAsync<Npgsql.PostgresException>();

        await writer.DisposeAsync();

        connection.Executed.Should().ContainSingle("the failure was reported; disposing must not quietly write the rows again");
    }

    [Fact]
    public async Task Writers_MakeOneAttemptInsideATransactionTheyDoNotOwn()
    {
        // PostgreSQL aborts the whole transaction on the first error (25P02 for everything after), so a retry inside the
        // sink's ExactlyOnce transaction can only fail again. The failure goes to the transaction's owner.
        var connection = new ScriptedConnection((_, _) => Server("40P01"))
        {
            CurrentTransaction = A.Fake<IDatabaseTransaction>(),
        };

        var perRow = () => PerRowWriter(connection).WriteAsync(new Row { Id = 1 });
        _ = await perRow.Should().ThrowAsync<Npgsql.PostgresException>();

        var batch = () => BatchWriter(connection, batchSize: 10).WriteBatchAsync(Rows(2));
        _ = await batch.Should().ThrowAsync<Npgsql.PostgresException>();

        connection.Executed.Should().HaveCount(2);
    }

    [Fact]
    public async Task PipelineCancellation_StopsWithoutAnotherAttempt()
    {
        using var cts = new CancellationTokenSource();

        var connection = new ScriptedConnection((_, _) =>
        {
            cts.Cancel();
            return Server("40P01");
        });

        var writer = PerRowWriter(connection, PostgresConnectorResilience.Default);

        var act = () => writer.WriteAsync(new Row { Id = 1 }, cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        connection.Executed.Should().ContainSingle();
    }

    [Fact]
    public async Task ForeignCancellation_IsAFailureThatIsNotRetried()
    {
        var connection = new ScriptedConnection((_, _) => new OperationCanceledException("not the pipeline's token"));
        var writer = PerRowWriter(connection);

        var act = () => writer.WriteAsync(new Row { Id = 1 });

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        connection.Executed.Should().ContainSingle();
    }

    private static Npgsql.PostgresException Server(string sqlState)
    {
        return new Npgsql.PostgresException("injected", "ERROR", "ERROR", sqlState);
    }

    private static PostgresPerRowWriter<Row> PerRowWriter(IDatabaseConnection connection, NResilience.Resilience? resilience = null)
    {
        return new PostgresPerRowWriter<Row>(connection, "public", "rows", null,
            new PostgresConfiguration { Resilience = resilience ?? Fast });
    }

    private static PostgresBatchWriter<Row> BatchWriter(IDatabaseConnection connection, int batchSize)
    {
        return new PostgresBatchWriter<Row>(connection, "public", "rows", null,
            new PostgresConfiguration { Resilience = Fast, BatchSize = batchSize });
    }

    private static IEnumerable<Row> Rows(int count)
    {
        return Enumerable.Range(1, count).Select(i => new Row { Id = i });
    }

    private static IEnumerable<int> CommittedIds(ScriptedConnection connection)
    {
        return connection.Committed.SelectMany(c => c.Parameters).OfType<int>();
    }

    private sealed class Row
    {
        public int Id { get; set; }
    }
}

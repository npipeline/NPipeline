using AwesomeAssertions;
using FakeItEasy;
using MySqlConnector;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Reliability;
using NPipeline.Connectors.MySql.Writers;
using NPipeline.StorageProviders.Abstractions;
using NResilience;

namespace NPipeline.Connectors.MySql.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the MySQL connector's NResilience policy (SQL1 and X1 in <c>plans/resilience-improvements.md</c>).
///     They assert what reached the database, not what is configured.
/// </summary>
public sealed class MySqlResilienceBehaviorTests
{
    // The shipped preset with near-zero backoff, so the tests barely wait between attempts.
    private static readonly Resilience Fast = MySqlConnectorResilience.Default with
    {
        Backoff = MySqlConnectorResilience.Default.Backoff with
        {
            TransientBase = TimeSpan.FromMilliseconds(1),
            ThrottledBase = TimeSpan.FromMilliseconds(1),
        },
    };

    [Fact]
    public void DefaultPreset_PreservesTheAttemptCountsAndDelaysOfTheSettingsItReplaces()
    {
        var preset = MySqlConnectorResilience.Default;

        // MaxRetryAttempts = 3 made MaxRetryAttempts + 1 = four calls; RetryDelay = 2 s doubled per retry, capped at 30 s.
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromSeconds(2));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(30));
        preset.Backoff.ThrottledBase.Should().Be(TimeSpan.FromSeconds(5));

        // A bulk load of many rows must not be cut off at NResilience's 10 s attempt timeout or 30 s deadline.
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse("the preset reproduces a fixed attempt count");
        preset.Validate();
    }

    [Theory]
    [InlineData(1040, VerdictKind.Throttled)]
    [InlineData(1203, VerdictKind.Throttled)]
    [InlineData(1205, VerdictKind.Transient)]
    [InlineData(1213, VerdictKind.Transient)]
    [InlineData(2006, VerdictKind.Transient)]
    [InlineData(2013, VerdictKind.Transient)]
    [InlineData(1062, VerdictKind.Permanent)]
    [InlineData(1146, VerdictKind.Permanent)]
    public void Classifier_JudgesServerErrorsByNumber(int number, VerdictKind expected)
    {
        MySqlConnectorResilience.Classifier.ClassifyException(MySqlExceptions.WithNumber(number)).Kind.Should().Be(expected);
    }

    [Fact]
    public void Classifier_JudgesOtherExceptionsLikeTheDetector_ExceptCancellation()
    {
        var classifier = MySqlConnectorResilience.Classifier;

        classifier.ClassifyException(new TimeoutException()).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new InvalidOperationException("Connection must be Open.")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new InvalidOperationException("Bad mapping.")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new ObjectDisposedException("MySqlConnection")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new OperationCanceledException()).Kind.Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public async Task PerRow_RetriesATransientFailureFourTimesThenThrows()
    {
        var connection = new ScriptedConnection((_, _) => MySqlExceptions.WithNumber(1213));
        var writer = PerRowWriter(connection);

        var act = () => writer.WriteAsync(new Row { Id = 1 });

        _ = await act.Should().ThrowAsync<MySqlException>();
        connection.Executed.Should().HaveCount(4);
    }

    [Fact]
    public async Task PerRow_DoesNotRetryAPermanentFailure()
    {
        var connection = new ScriptedConnection((_, _) => MySqlExceptions.WithNumber(1062));
        var writer = PerRowWriter(connection);

        var act = () => writer.WriteAsync(new Row { Id = 1 });

        _ = await act.Should().ThrowAsync<MySqlException>();
        connection.Executed.Should().ContainSingle();
    }

    [Fact]
    public async Task Batch_RetriesOnlyTheChunkThatFailed()
    {
        var connection = new ScriptedConnection((index, _) => index == 2
            ? MySqlExceptions.WithNumber(1205)
            : null);

        var writer = BatchWriter(connection, 2);

        await writer.WriteBatchAsync(Rows(6));

        connection.Executed.Should().HaveCount(4);
        CommittedIds(connection).Should().Equal(1, 2, 3, 4, 5, 6);
    }

    [Fact]
    public async Task Batch_DoesNotResendAFailedChunkWhenDisposed()
    {
        var connection = new ScriptedConnection((_, _) => MySqlExceptions.WithNumber(1062));
        var writer = BatchWriter(connection, 10);

        var act = () => writer.WriteBatchAsync(Rows(3));
        _ = await act.Should().ThrowAsync<MySqlException>();

        await writer.DisposeAsync();

        connection.Executed.Should().ContainSingle("the failure was reported; disposing must not quietly write the rows again");
    }

    [Fact]
    public async Task Writers_MakeOneAttemptInsideATransactionTheyDoNotOwn()
    {
        // A deadlock (1213) rolls back the whole transaction in InnoDB, so a retry inside the sink's ExactlyOnce
        // transaction would commit this statement without the ones before it. The failure goes to the transaction's owner.
        var connection = new ScriptedConnection((_, _) => MySqlExceptions.WithNumber(1213))
        {
            CurrentTransaction = A.Fake<IDatabaseTransaction>(),
        };

        var perRow = () => PerRowWriter(connection).WriteAsync(new Row { Id = 1 });
        _ = await perRow.Should().ThrowAsync<MySqlException>();

        var batch = () => BatchWriter(connection, 10).WriteBatchAsync(Rows(2));
        _ = await batch.Should().ThrowAsync<MySqlException>();

        connection.Executed.Should().HaveCount(2);
    }

    [Fact]
    public async Task PipelineCancellation_StopsWithoutAnotherAttempt()
    {
        using var cts = new CancellationTokenSource();

        var connection = new ScriptedConnection((_, _) =>
        {
            cts.Cancel();
            return MySqlExceptions.WithNumber(1213);
        });

        var writer = PerRowWriter(connection, MySqlConnectorResilience.Default);

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

    private static MySqlPerRowWriter<Row> PerRowWriter(IDatabaseConnection connection, Resilience? resilience = null) =>
        new(connection, "rows", null, new MySqlConfiguration { Resilience = resilience ?? Fast });

    private static MySqlBatchWriter<Row> BatchWriter(IDatabaseConnection connection, int batchSize) =>
        new(connection, "rows", null, new MySqlConfiguration { Resilience = Fast, BatchSize = batchSize });

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

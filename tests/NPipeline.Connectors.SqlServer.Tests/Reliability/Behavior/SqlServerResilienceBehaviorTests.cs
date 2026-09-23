using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Reliability;
using NPipeline.Connectors.SqlServer.Writers;
using NPipeline.StorageProviders.Abstractions;
using NResilience;

namespace NPipeline.Connectors.SqlServer.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the SQL Server connector's NResilience policy (SQL1 and X1 in <c>plans/resilience-improvements.md</c>).
///     They assert what reached the database, not what is configured.
/// </summary>
public sealed class SqlServerResilienceBehaviorTests
{
    // The shipped preset with near-zero backoff, so the tests barely wait between attempts.
    private static readonly NResilience.Resilience Fast = SqlServerConnectorResilience.Default with
    {
        Backoff = SqlServerConnectorResilience.Default.Backoff with
        {
            TransientBase = TimeSpan.FromMilliseconds(1),
            ThrottledBase = TimeSpan.FromMilliseconds(1),
        },
    };

    [Fact]
    public void DefaultPreset_PreservesTheAttemptCountsAndDelaysOfTheSettingsItReplaces()
    {
        var preset = SqlServerConnectorResilience.Default;

        // MaxRetryAttempts = 3 made MaxRetryAttempts + 1 = four calls; RetryDelay = 1 s doubled per retry, capped at 30 s.
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromSeconds(1));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(30));
        preset.Backoff.ThrottledBase.Should().Be(TimeSpan.FromSeconds(10));

        // A bulk copy of many rows must not be cut off at NResilience's 10 s attempt timeout or 30 s deadline; the
        // driver's CommandTimeout and BulkCopyTimeout bound each attempt instead.
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse("the preset reproduces a fixed attempt count");
        preset.Validate();
    }

    [Theory]
    [InlineData(40501)]
    [InlineData(10928)]
    [InlineData(10929)]
    [InlineData(49918)]
    public void Classifier_TreatsAzureThrottlingAsThrottled(int number)
    {
        SqlServerConnectorResilience.Classifier.ClassifyException(SqlExceptions.WithNumber(number)).Kind
            .Should().Be(VerdictKind.Throttled);
    }

    [Theory]
    [InlineData(-2)]
    [InlineData(1205)]
    [InlineData(40613)]
    public void Classifier_TreatsTheDetectorsTransientErrorsAsTransient(int number)
    {
        SqlServerConnectorResilience.Classifier.ClassifyException(SqlExceptions.WithNumber(number)).Kind
            .Should().Be(VerdictKind.Transient);
    }

    [Theory]
    [InlineData(2627)]
    [InlineData(547)]
    [InlineData(208)]
    public void Classifier_TreatsOtherSqlErrorsAsPermanent(int number)
    {
        SqlServerConnectorResilience.Classifier.ClassifyException(SqlExceptions.WithNumber(number)).Kind
            .Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public void Classifier_JudgesOtherExceptionsLikeTheDetector_ExceptCancellation()
    {
        var classifier = SqlServerConnectorResilience.Classifier;

        classifier.ClassifyException(new TimeoutException()).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new InvalidOperationException("The connection is broken.")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new InvalidOperationException("Bad mapping.")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new ObjectDisposedException("SqlConnection")).Kind.Should().Be(VerdictKind.Permanent);

        // An OperationCanceledException the pipeline did not ask for is a failure, not something to retry.
        classifier.ClassifyException(new OperationCanceledException()).Kind.Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public async Task PerRow_RetriesATransientFailureFourTimesThenThrows()
    {
        var connection = new ScriptedConnection((_, _) => new TimeoutException("injected"));
        var writer = PerRowWriter(connection);

        var act = () => writer.WriteAsync(new Row { Id = 1 });

        _ = await act.Should().ThrowAsync<TimeoutException>();
        connection.Executed.Should().HaveCount(4);
    }

    [Fact]
    public async Task PerRow_RecoversWhenARetrySucceeds_WritingTheRowOnce()
    {
        var connection = new ScriptedConnection((index, _) => index == 0 ? SqlExceptions.WithNumber(1205) : null);
        var writer = PerRowWriter(connection);

        await writer.WriteAsync(new Row { Id = 7 });

        connection.Executed.Should().HaveCount(2);
        connection.Committed.Should().ContainSingle().Which.Parameters.Should().Contain(7);
    }

    [Fact]
    public async Task PerRow_DoesNotRetryAPermanentFailure()
    {
        var connection = new ScriptedConnection((_, _) => SqlExceptions.WithNumber(2627));
        var writer = PerRowWriter(connection);

        var act = () => writer.WriteAsync(new Row { Id = 1 });

        _ = await act.Should().ThrowAsync<Microsoft.Data.SqlClient.SqlException>();
        connection.Executed.Should().ContainSingle();
    }

    [Fact]
    public async Task PerRow_ReopensAConnectionTheFailureClosedBeforeRetrying()
    {
        ScriptedConnection connection = null!;

        connection = new ScriptedConnection((index, _) =>
        {
            if (index > 0)
                return null;

            connection.IsOpen = false;
            return SqlExceptions.WithNumber(64);
        });

        var writer = PerRowWriter(connection);

        await writer.WriteAsync(new Row { Id = 1 });

        connection.Opens.Should().Be(1);
        connection.Committed.Should().ContainSingle();
    }

    [Fact]
    public async Task Batch_RetriesOnlyTheChunkThatFailed()
    {
        // Three chunks of two rows. The third fails once: the first two are already committed and must not be sent again.
        var connection = new ScriptedConnection((index, _) => index == 2 ? new TimeoutException("injected") : null);
        var writer = BatchWriter(connection, batchSize: 2);

        await writer.WriteBatchAsync(Rows(6));

        connection.Executed.Should().HaveCount(4);
        CommittedIds(connection).Should().Equal(1, 2, 3, 4, 5, 6);
    }

    [Fact]
    public async Task Batch_DoesNotResendAFailedChunkWhenDisposed()
    {
        var connection = new ScriptedConnection((_, _) => SqlExceptions.WithNumber(2627));
        var writer = BatchWriter(connection, batchSize: 10);

        var act = () => writer.WriteBatchAsync(Rows(3));
        _ = await act.Should().ThrowAsync<Microsoft.Data.SqlClient.SqlException>();

        await writer.DisposeAsync();

        connection.Executed.Should().ContainSingle("the failure was reported; disposing must not quietly write the rows again");
    }

    [Fact]
    public async Task Writers_MakeOneAttemptInsideATransactionTheyDoNotOwn()
    {
        // ExactlyOnce: the sink's transaction may already have been rolled back by the failure (a deadlock does that), so a
        // retry would commit this statement without the ones before it. The failure goes to the transaction's owner.
        var connection = new ScriptedConnection((_, _) => SqlExceptions.WithNumber(1205))
        {
            CurrentTransaction = A.Fake<IDatabaseTransaction>(),
        };

        var perRow = () => PerRowWriter(connection).WriteAsync(new Row { Id = 1 });
        _ = await perRow.Should().ThrowAsync<Microsoft.Data.SqlClient.SqlException>();

        var batch = () => BatchWriter(connection, batchSize: 10).WriteBatchAsync(Rows(2));
        _ = await batch.Should().ThrowAsync<Microsoft.Data.SqlClient.SqlException>();

        connection.Executed.Should().HaveCount(2);
    }

    [Fact]
    public async Task PipelineCancellation_StopsWithoutAnotherAttempt()
    {
        using var cts = new CancellationTokenSource();

        var connection = new ScriptedConnection((_, _) =>
        {
            cts.Cancel();
            return new TimeoutException("injected");
        });

        var writer = PerRowWriter(connection, SqlServerConnectorResilience.Default);

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

    private static SqlServerPerRowWriter<Row> PerRowWriter(IDatabaseConnection connection, NResilience.Resilience? resilience = null)
    {
        return new SqlServerPerRowWriter<Row>(connection, "dbo", "rows", null,
            new SqlServerConfiguration { Resilience = resilience ?? Fast });
    }

    private static SqlServerBatchWriter<Row> BatchWriter(IDatabaseConnection connection, int batchSize)
    {
        return new SqlServerBatchWriter<Row>(connection, "dbo", "rows", null,
            new SqlServerConfiguration { Resilience = Fast, BatchSize = batchSize });
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

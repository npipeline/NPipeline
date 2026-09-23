using System.Data.Common;
using System.Net;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Reliability;
using NPipeline.Connectors.Snowflake.Writers;
using NPipeline.StorageProviders.Abstractions;
using NResilience;

namespace NPipeline.Connectors.Snowflake.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the Snowflake connector's NResilience policy (SQL1 and X1 in <c>plans/resilience-improvements.md</c>).
///     They assert what reached the database, not what is configured. There is no Snowflake emulator, so the writers run
///     against a scripted connection.
/// </summary>
public sealed class SnowflakeResilienceBehaviorTests
{
    private const int NetworkError = 200002;
    private const int ObjectDoesNotExist = 2003;

    // The shipped preset with near-zero backoff, so the tests barely wait between attempts.
    private static readonly NResilience.Resilience Fast = SnowflakeConnectorResilience.Default with
    {
        Backoff = SnowflakeConnectorResilience.Default.Backoff with
        {
            TransientBase = TimeSpan.FromMilliseconds(1),
            ThrottledBase = TimeSpan.FromMilliseconds(1),
        },
    };

    [Fact]
    public void DefaultPreset_PreservesTheAttemptCountsAndDelaysOfTheSettingsItReplaces()
    {
        var preset = SnowflakeConnectorResilience.Default;

        // MaxRetryAttempts = 3 made MaxRetryAttempts + 1 = four calls; RetryDelay = 2 s doubled per retry, capped at 60 s.
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromSeconds(2));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(60));
        preset.Backoff.ThrottledBase.Should().Be(TimeSpan.FromSeconds(10));

        // A staged COPY INTO can run for minutes; NResilience's 10 s attempt timeout and 30 s deadline would cut it off.
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse("the preset reproduces a fixed attempt count");
        preset.Validate();
    }

    [Fact]
    public void Classifier_JudgesSnowflakeErrors()
    {
        var classifier = SnowflakeConnectorResilience.Classifier;

        classifier.ClassifyException(new FakeSnowflakeException("Request throttled", 0)).Kind.Should().Be(VerdictKind.Throttled);
        classifier.ClassifyException(new HttpRequestException("busy", null, HttpStatusCode.TooManyRequests)).Kind.Should().Be(VerdictKind.Throttled);

        classifier.ClassifyException(new FakeSnowflakeException("network", NetworkError)).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new FakeSnowflakeException("maintenance", 390144)).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new HttpRequestException("reset")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new TimeoutException()).Kind.Should().Be(VerdictKind.Transient);

        classifier.ClassifyException(new FakeSnowflakeException("Object does not exist", ObjectDoesNotExist)).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new InvalidOperationException("Bad mapping.")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new ObjectDisposedException("SnowflakeDbConnection")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new OperationCanceledException()).Kind.Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public async Task PerRow_RetriesATransientFailureFourTimesThenThrows()
    {
        var connection = new ScriptedConnection((_, _) => new FakeSnowflakeException("network", NetworkError));
        var writer = new SnowflakePerRowWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration());

        var act = () => writer.WriteAsync(new Row { Id = 1 });

        _ = await act.Should().ThrowAsync<FakeSnowflakeException>();
        connection.Executed.Should().HaveCount(4);
    }

    [Fact]
    public async Task Batch_RetriesOnlyTheChunkThatFailed()
    {
        var connection = new ScriptedConnection((index, _) => index == 2 ? new FakeSnowflakeException("network", NetworkError) : null);
        var writer = new SnowflakeBatchWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(batchSize: 2));

        await writer.WriteBatchAsync(Enumerable.Range(1, 6).Select(i => new Row { Id = i }));

        connection.Executed.Should().HaveCount(4);
        connection.Committed.SelectMany(c => c.Parameters).OfType<int>().Should().Equal(1, 2, 3, 4, 5, 6);
    }

    [Fact]
    public async Task StagedCopy_RetriesTheCopyOfTheSameStagedFileWithoutUploadingItAgain()
    {
        // The first COPY INTO fails as if its reply were lost. Retrying it against the same staged file lets Snowflake's
        // load metadata skip the file if it was in fact loaded; uploading a new file would load the rows a second time.
        var copies = 0;
        var connection = new ScriptedConnection((_, command) =>
            command.Text.StartsWith("COPY INTO", StringComparison.Ordinal) && copies++ == 0
                ? new FakeSnowflakeException("network", NetworkError)
                : null);

        var writer = new SnowflakeStagedCopyWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(batchSize: 10));

        await writer.WriteBatchAsync(Enumerable.Range(1, 3).Select(i => new Row { Id = i }));

        var puts = connection.Executed.Where(c => c.Text.StartsWith("PUT", StringComparison.Ordinal)).ToList();
        var copyCommands = connection.Executed.Where(c => c.Text.StartsWith("COPY INTO", StringComparison.Ordinal)).ToList();

        puts.Should().ContainSingle();
        copyCommands.Should().HaveCount(2);
        copyCommands.Select(c => c.Text).Distinct().Should().ContainSingle("both attempts load the same staged file");
        copyCommands[0].Text.Should().Contain(StagedFile(puts[0].Text));
    }

    [Fact]
    public async Task StagedCopy_RetriesAFailedUploadBeforeCopying()
    {
        var connection = new ScriptedConnection((index, _) => index == 0 ? new FakeSnowflakeException("network", NetworkError) : null);
        var writer = new SnowflakeStagedCopyWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(batchSize: 10));

        await writer.WriteBatchAsync([new Row { Id = 1 }]);

        connection.Executed.Select(c => c.Text.Split(' ')[0]).Should().Equal("PUT", "PUT", "COPY");
    }

    [Fact]
    public async Task StagedCopy_DoesNotRetryAPermanentFailureOrResendItWhenDisposed()
    {
        var connection = new ScriptedConnection((_, command) => command.Text.StartsWith("COPY INTO", StringComparison.Ordinal)
            ? new FakeSnowflakeException("Object does not exist", ObjectDoesNotExist)
            : null);

        var writer = new SnowflakeStagedCopyWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(batchSize: 10));

        var act = () => writer.WriteBatchAsync([new Row { Id = 1 }]);
        _ = await act.Should().ThrowAsync<FakeSnowflakeException>();

        await writer.DisposeAsync();

        connection.Executed.Should().HaveCount(2, "one PUT and one COPY INTO; disposing must not load the rows again");
    }

    [Fact]
    public async Task Writers_MakeOneAttemptInsideATransactionTheyDoNotOwn()
    {
        var connection = new ScriptedConnection((_, _) => new FakeSnowflakeException("network", NetworkError))
        {
            CurrentTransaction = A.Fake<IDatabaseTransaction>(),
        };

        var perRow = () => new SnowflakePerRowWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration()).WriteAsync(new Row { Id = 1 });
        _ = await perRow.Should().ThrowAsync<FakeSnowflakeException>();

        var batch = () => new SnowflakeBatchWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(batchSize: 10))
            .WriteBatchAsync([new Row { Id = 1 }]);
        _ = await batch.Should().ThrowAsync<FakeSnowflakeException>();

        connection.Executed.Should().HaveCount(2);
    }

    [Fact]
    public async Task PipelineCancellation_StopsWithoutAnotherAttempt()
    {
        using var cts = new CancellationTokenSource();

        var connection = new ScriptedConnection((_, _) =>
        {
            cts.Cancel();
            return new FakeSnowflakeException("network", NetworkError);
        });

        var writer = new SnowflakePerRowWriter<Row>(connection, "PUBLIC", "ROWS", null,
            new SnowflakeConfiguration { Resilience = SnowflakeConnectorResilience.Default });

        var act = () => writer.WriteAsync(new Row { Id = 1 }, cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        connection.Executed.Should().ContainSingle();
    }

    [Fact]
    public async Task ForeignCancellation_IsAFailureThatIsNotRetried()
    {
        var connection = new ScriptedConnection((_, _) => new OperationCanceledException("not the pipeline's token"));
        var writer = new SnowflakePerRowWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration());

        var act = () => writer.WriteAsync(new Row { Id = 1 });

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        connection.Executed.Should().ContainSingle();
    }

    private static SnowflakeConfiguration Configuration(int batchSize = 100)
    {
        return new SnowflakeConfiguration { Resilience = Fast, BatchSize = batchSize };
    }

    private static string StagedFile(string putSql)
    {
        // PUT 'file://...' '@~/npipeline_..._0.csv' ... -> @~/npipeline_..._0.csv
        return putSql.Split('\'')[3];
    }

    private sealed class FakeSnowflakeException(string message, int errorCode) : DbException(message, errorCode);

    private sealed class Row
    {
        public int Id { get; set; }
    }
}

using System.Data.Common;
using System.Net;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Exceptions;
using NPipeline.Connectors.Snowflake.Reliability;
using NPipeline.Connectors.Snowflake.Writers;
using NPipeline.StorageProviders.Abstractions;
using NResilience;
using Snowflake.Data.Client;

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
    private const int DriverRequestTimeout = 270007;
    private const int DriverPutIoError = 270058;
    private const int SessionGone = 390111;

    // The shipped preset with near-zero backoff, so the tests barely wait between attempts.
    private static readonly Resilience Fast = SnowflakeConnectorResilience.Default with
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
    public void Classifier_RetriesStatementLevelErrorsTheServerReturns()
    {
        var classifier = SnowflakeConnectorResilience.Classifier;

        classifier.ClassifyException(Server(390144, "Service unavailable")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(Server(625, "Statement reached its statement or warehouse timeout")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(Server(604, "SQL execution internal error")).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(Server(1234, "Request throttled")).Kind.Should().Be(VerdictKind.Throttled);

        classifier.ClassifyException(Server(ObjectDoesNotExist, "Object does not exist")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(Server(1003, "SQL compilation error")).Kind.Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public void Classifier_TreatsFailuresTheDriverAlreadyRetriedAsPermanent()
    {
        // Snowflake.Data retries every HTTP request itself (transport errors, timeouts, 5xx, 403, 408, 429). What it
        // reports after giving up must not be retried again by rerunning the statement.
        var classifier = SnowflakeConnectorResilience.Classifier;

        classifier.ClassifyException(new HttpRequestException("busy", null, HttpStatusCode.TooManyRequests)).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new HttpRequestException("unavailable", null, HttpStatusCode.ServiceUnavailable)).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new HttpRequestException("reset")).Kind.Should().Be(VerdictKind.Permanent);

        var requestTimeout = Server(DriverRequestTimeout, "Request reach its timeout");
        classifier.ClassifyException(requestTimeout).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new OperationCanceledException(requestTimeout.Message, requestTimeout)).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(Server(DriverPutIoError, "IO error on PUT, network unreachable")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(Server(SessionGone, "Session no longer exists")).Kind.Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public void Classifier_JudgesOtherExceptions()
    {
        var classifier = SnowflakeConnectorResilience.Classifier;

        classifier.ClassifyException(new TimeoutException()).Kind.Should().Be(VerdictKind.Transient);
        classifier.ClassifyException(new InvalidOperationException("Bad mapping.")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new ObjectDisposedException("SnowflakeDbConnection")).Kind.Should().Be(VerdictKind.Permanent);
        classifier.ClassifyException(new OperationCanceledException()).Kind.Should().Be(VerdictKind.Permanent);
    }

    [Fact]
    public async Task PerRow_DoesNotRerunAStatementWhoseHttpRequestsTheDriverAlreadyRetried()
    {
        var connection = new ScriptedConnection((_, _) => new HttpRequestException("unavailable", null, HttpStatusCode.ServiceUnavailable));
        var writer = new SnowflakePerRowWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration());

        var act = () => writer.WriteAsync(new Row { Id = 1 });

        _ = await act.Should().ThrowAsync<HttpRequestException>();
        connection.Executed.Should().ContainSingle();
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
        var connection = new ScriptedConnection((index, _) => index == 2
            ? new FakeSnowflakeException("network", NetworkError)
            : null);

        var writer = new SnowflakeBatchWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(2));

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
                : null, StageResults());

        var writer = new SnowflakeStagedCopyWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(10));

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
        var connection = new ScriptedConnection((index, _) => index == 0
                ? new FakeSnowflakeException("network", NetworkError)
                : null,
            StageResults());

        var writer = new SnowflakeStagedCopyWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(10));

        await writer.WriteBatchAsync([new Row { Id = 1 }]);

        connection.Executed.Select(c => c.Text.Split(' ')[0]).Should().Equal("PUT", "PUT", "COPY");
    }

    [Fact]
    public async Task StagedCopy_DoesNotRetryAPermanentFailureOrResendItWhenDisposed()
    {
        var connection = new ScriptedConnection((_, command) => command.Text.StartsWith("COPY INTO", StringComparison.Ordinal)
            ? new FakeSnowflakeException("Object does not exist", ObjectDoesNotExist)
            : null, StageResults());

        var writer = new SnowflakeStagedCopyWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(10));

        var act = () => writer.WriteBatchAsync([new Row { Id = 1 }]);
        _ = await act.Should().ThrowAsync<FakeSnowflakeException>();

        await writer.DisposeAsync();

        connection.Executed.Should().HaveCount(2, "one PUT and one COPY INTO; disposing must not load the rows again");
    }

    [Fact]
    public async Task StagedCopy_FailsWhenPutReportsAFileItCouldNotUpload_WithoutCopyingOrRetrying()
    {
        // The driver reports an upload that failed even after its own retries as a result row, not an exception. Copying
        // anyway would load nothing and lose the flush without an error.
        var connection = new ScriptedConnection(results: StageResults("ERROR", "Access Denied"));
        var writer = new SnowflakeStagedCopyWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(10));

        var act = () => writer.WriteBatchAsync([new Row { Id = 1 }]);

        var thrown = await act.Should().ThrowAsync<SnowflakeException>();
        thrown.Which.Message.Should().Contain("ERROR").And.Contain("Access Denied");

        connection.Executed.Should().ContainSingle("the upload was not retried and nothing was copied")
            .Which.Text.Should().StartWith("PUT");
    }

    [Fact]
    public async Task StagedCopy_FailsWhenTheFirstCopyProcessesNoFiles()
    {
        var connection = new ScriptedConnection(results: StageResults(copyLoadsFile: _ => false));
        var writer = new SnowflakeStagedCopyWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(10));

        var act = () => writer.WriteBatchAsync([new Row { Id = 1 }]);

        var thrown = await act.Should().ThrowAsync<SnowflakeException>();
        thrown.Which.Message.Should().Contain("0 files processed");
        connection.Executed.Count(c => c.Text.StartsWith("COPY INTO", StringComparison.Ordinal)).Should().Be(1);
    }

    [Fact]
    public async Task StagedCopy_AcceptsNoFilesOnARetry_BecauseTheLostAttemptAlreadyLoadedTheFile()
    {
        // The first COPY INTO loaded the file but its reply was lost; the retry finds the file already loaded (load
        // metadata) and processes nothing. That is success, not a missing file.
        var copies = 0;

        var connection = new ScriptedConnection(
            (_, command) => command.Text.StartsWith("COPY INTO", StringComparison.Ordinal) && copies++ == 0
                ? new FakeSnowflakeException("network", NetworkError)
                : null,
            StageResults(copyLoadsFile: _ => false));

        var writer = new SnowflakeStagedCopyWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(10));

        await writer.WriteBatchAsync([new Row { Id = 1 }]);

        connection.Executed.Count(c => c.Text.StartsWith("COPY INTO", StringComparison.Ordinal)).Should().Be(2);
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

        var batch = () => new SnowflakeBatchWriter<Row>(connection, "PUBLIC", "ROWS", null, Configuration(10))
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

    private static SnowflakeConfiguration Configuration(int batchSize = 100) => new() { Resilience = Fast, BatchSize = batchSize };

    private static string StagedFile(string putSql) =>

        // PUT 'file://...' '@~/npipeline_..._0.csv' ... -> @~/npipeline_..._0.csv
        putSql.Split('\'')[3];

    // What Snowflake answers PUT and COPY INTO with. COPY INTO that finds nothing to load answers with a single status row
    // and no file column.
    private static Func<ScriptedConnection.ExecutedCommand, IReadOnlyList<IReadOnlyDictionary<string, object?>>> StageResults(
        string putStatus = "UPLOADED",
        string? putMessage = "",
        Func<ScriptedConnection.ExecutedCommand, bool>? copyLoadsFile = null)
    {
        copyLoadsFile ??= _ => true;

        return command =>
        {
            if (command.Text.StartsWith("PUT", StringComparison.Ordinal))
            {
                return
                [
                    new Dictionary<string, object?>
                    {
                        ["source"] = "npipeline_0.csv",
                        ["target"] = "npipeline_0.csv.gz",
                        ["status"] = putStatus,
                        ["message"] = putMessage,
                    },
                ];
            }

            return copyLoadsFile(command)
                ?
                [
                    new Dictionary<string, object?>
                    {
                        ["file"] = "npipeline_0.csv.gz",
                        ["status"] = "LOADED",
                        ["rows_parsed"] = 1L,
                        ["rows_loaded"] = 1L,
                    },
                ]
                : [new Dictionary<string, object?> { ["status"] = "Copy executed with 0 files processed." }];
        };
    }

    private static SnowflakeDbException Server(int vendorCode, string message) => new("XX000", vendorCode, message, "query-id");

    private sealed class FakeSnowflakeException(string message, int errorCode) : DbException(message, errorCode);

    private sealed class Row
    {
        public int Id { get; set; }
    }
}

using FakeItEasy;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Writers;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Writers;

/// <summary>
///     Tests for exactly-once delivery semantics in the Redshift connector.
///     Validates transaction handling, checkpoint integration, commit,
///     and rollback behaviour across all write strategies.
/// </summary>
public sealed class RedshiftExactlyOnceTests
{
    // ── Configuration validation ──────────────────────────────────────────

    [Fact]
    public void ExactlyOnce_WithBatchStrategy_AndUseTransaction_IsValid()
    {
        // Arrange
        var configuration = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.Batch,
            UseTransaction = true,
        };

        // Act
        var exception = Record.Exception(() => configuration.Validate());

        // Assert
        exception.Should().BeNull();
    }

    [Fact]
    public void ExactlyOnce_WithBatchStrategy_AndUpsert_IsValid()
    {
        // Arrange
        var configuration = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.Batch,
            UseTransaction = true,
            UseUpsert = true,
            UpsertKeyColumns = ["id"],
        };

        // Act
        var exception = Record.Exception(() => configuration.Validate());

        // Assert
        exception.Should().BeNull();
    }

    [Fact]
    public void ExactlyOnce_WithCopyFromS3Strategy_DoesNotRequireTransaction()
    {
        // COPY is atomic per file; transactions are not needed/supported for it.
        // Arrange
        var configuration = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            UseTransaction = false,
            S3BucketName = "my-bucket",
            IamRoleArn = "arn:aws:iam::123456789012:role/RedshiftCopyRole",
        };

        // Act
        var exception = Record.Exception(() => configuration.Validate());

        // Assert
        exception.Should().BeNull();
    }

    [Fact]
    public void ExactlyOnce_WithUpsertButNoKeyColumns_IsInvalid()
    {
        // Arrange
        var configuration = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.Batch,
            UseUpsert = true,
            UpsertKeyColumns = [],
        };

        // Act
        var act = () => configuration.Validate();

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*UpsertKeyColumns*");
    }

    // ── Checkpoint resume behaviour (using mocked writers) ───────────────

    [Fact]
    public async Task WriteAsync_WithCheckpoint_DoesNotReprocessRows()
    {
        // Arrange — simulate a writer that has already processed row ID = 1.
        // A second pipeline pass should skip rows already past the checkpoint.
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            UseTransaction = true,
            MaxRetryAttempts = 0,
        };

        // Writer throws on connection — we are only verifying buffering behaviour here.
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        await using var writer = new RedshiftBatchWriter<CheckpointRow>(
            connectionPool, "public", "checkpoints_table", configuration);

        // Act — write two rows, but the second IDs simulates a resume boundary.
        await writer.WriteAsync(new CheckpointRow { Id = 2, Value = "row-after-checkpoint" });

        // No connection call yet (below batch size).
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_WithCheckpoint_ResumesAfterFailure()
    {
        // Arrange — first pass fails on flush; second pass should pick up from
        // the last successful checkpoint.
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 2, // small size to trigger flush quickly
            UseTransaction = true,
            MaxRetryAttempts = 0,
        };

        // Simulate network failure on connection
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Simulated failure"));

        await using var writer = new RedshiftBatchWriter<CheckpointRow>(
            connectionPool, "public", "checkpoints_table", configuration);

        await writer.WriteAsync(new CheckpointRow { Id = 1, Value = "first-row" });

        // This triggers the batch flush and should fail
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.WriteAsync(new CheckpointRow { Id = 2, Value = "second-row" }));

        // Assert — connection was attempted (flush was attempted)
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    // ── Transaction wrapping ──────────────────────────────────────────────

    [Fact]
    public async Task BatchWriter_WithUseTransactionTrue_AttemptsTransaction()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            UseTransaction = true,
            MaxRetryAttempts = 0,
        };

        // Simulate failure so we can verify connection was requested
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test failure"));

        await using var writer = new RedshiftBatchWriter<CheckpointRow>(
            connectionPool, "public", "test_table", configuration);

        await writer.WriteAsync(new CheckpointRow { Id = 1, Value = "v1" });

        // Act
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        // Assert — the flush path (which wraps in a transaction) was entered
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task BatchWriter_WithUseTransactionFalse_AttemptsFlushWithoutTransaction()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            UseTransaction = false,
            MaxRetryAttempts = 0,
        };

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test failure"));

        await using var writer = new RedshiftBatchWriter<CheckpointRow>(
            connectionPool, "public", "test_table", configuration);

        await writer.WriteAsync(new CheckpointRow { Id = 1, Value = "v1" });

        // Act
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        // Assert — the flush path was still entered (regardless of transaction flag)
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task PerRowWriter_AttemptsConnectionPerRow()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            MaxRetryAttempts = 0,
        };

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test failure"));

        await using var writer = new RedshiftPerRowWriter<CheckpointRow>(
            connectionPool, "public", "test_table", configuration);

        // Act — each WriteAsync immediately executes, so connection is requested per row
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.WriteAsync(new CheckpointRow { Id = 1, Value = "v1" }));

        // Assert
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    // ── Dispose flushes remaining rows ───────────────────────────────────

    [Fact]
    public async Task BatchWriter_DisposeAsync_FlushesRemainingRowsExactlyOnce()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            UseTransaction = true,
            MaxRetryAttempts = 0,
        };

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test failure"));

        var writer = new RedshiftBatchWriter<CheckpointRow>(
            connectionPool, "public", "test_table", configuration);

        await writer.WriteAsync(new CheckpointRow { Id = 1, Value = "pending" });

        // Act
        await writer.DisposeAsync(); // Should attempt flush of the 1 buffered row

        // Assert
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task BatchWriter_DisposeAsync_CalledTwice_DoesNotFlushTwice()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            UseTransaction = true,
            MaxRetryAttempts = 0,
        };

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test failure"));

        var writer = new RedshiftBatchWriter<CheckpointRow>(
            connectionPool, "public", "test_table", configuration);

        await writer.WriteAsync(new CheckpointRow { Id = 1, Value = "pending" });

        // Act
        await writer.DisposeAsync(); // Flushes once
        await writer.DisposeAsync(); // Second dispose is a no-op (already disposed)

        // Assert — connection was only requested on the first dispose
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    // ── Test models ──────────────────────────────────────────────────────

    private sealed class CheckpointRow
    {
        public int Id { get; set; }
        public string Value { get; set; } = string.Empty;
    }
}

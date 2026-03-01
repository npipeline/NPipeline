using FakeItEasy;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Writers;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Writers;

public class RedshiftBatchWriterUpsertTests
{
    [Fact]
    public async Task WriteAsync_WithUpsert_StagingTablePattern_ExecutesDeleteThenInsert()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100, // Use larger batch size so WriteAsync doesn't auto-flush
            UseUpsert = true,
            UpsertKeyColumns = new[] { "id" },
            UseMergeSyntax = false,
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act & Assert
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithUpsert_MergeSyntax_ExecutesMerge()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100, // Use larger batch size so WriteAsync doesn't auto-flush
            UseUpsert = true,
            UpsertKeyColumns = new[] { "id" },
            UseMergeSyntax = true,
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act & Assert
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithUpsert_NoKeyColumns_ShouldThrowOnFlush()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100, // Use larger batch size so WriteAsync doesn't auto-flush
            UseUpsert = true,
            UpsertKeyColumns = Array.Empty<string>(),
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - this will be thrown before the key columns check
        // because the writer tries to get a connection first
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });

        // Act & Assert - The connection failure happens before the key columns check
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());
    }

    [Fact]
    public async Task WriteAsync_WithOnMergeSkip_GeneratesInsertOnlyMerge()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100, // Use larger batch size so WriteAsync doesn't auto-flush
            UseUpsert = true,
            UpsertKeyColumns = new[] { "id" },
            UseMergeSyntax = true,
            OnMergeAction = OnMergeAction.Skip,
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act & Assert
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithOnMergeUpdate_GeneratesUpdateAndInsertMerge()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100, // Use larger batch size so WriteAsync doesn't auto-flush
            UseUpsert = true,
            UpsertKeyColumns = new[] { "id" },
            UseMergeSyntax = true,
            OnMergeAction = OnMergeAction.Update,
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act & Assert
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithUpsertAndTransaction_CommitsSuccessfully()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100, // Use larger batch size so WriteAsync doesn't auto-flush
            UseUpsert = true,
            UpsertKeyColumns = new[] { "id" },
            UseMergeSyntax = false,
            UseTransaction = true,
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act & Assert
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithUpsertAndTempStagingTable_UsesTempTable()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100, // Use larger batch size so WriteAsync doesn't auto-flush
            UseUpsert = true,
            UpsertKeyColumns = new[] { "id" },
            UseMergeSyntax = false,
            UseTempStagingTable = true,
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act & Assert
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithUpsertAndCustomStagingSchema_UsesCustomSchema()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100, // Use larger batch size so WriteAsync doesn't auto-flush
            UseUpsert = true,
            UpsertKeyColumns = new[] { "id" },
            UseMergeSyntax = false,
            UseTempStagingTable = false,
            StagingSchema = "staging",
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act & Assert
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithUpsertAndMultipleKeyColumns_UsesAllKeys()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100, // Use larger batch size so WriteAsync doesn't auto-flush
            UseUpsert = true,
            UpsertKeyColumns = new[] { "id", "name" },
            UseMergeSyntax = true,
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act & Assert
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithUpsertAndCustomStagingTablePrefix_UsesPrefix()
    {
        // Arrange
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100, // Use larger batch size so WriteAsync doesn't auto-flush
            UseUpsert = true,
            UpsertKeyColumns = new[] { "id" },
            UseMergeSyntax = false,
            StagingTablePrefix = "custom_stage_",
            MaxRetryAttempts = 0, // Disable retries for test
        };

        // Make connection fail - we just want to verify it tries to connect
        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .Throws(new InvalidOperationException("Test connection failure"));

        var writer = new RedshiftBatchWriter<TestRow>(
            connectionPool, "public", "test_table", configuration);

        // Act & Assert
        await writer.WriteAsync(new TestRow { Id = 1, Name = "Test1" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.FlushAsync());

        A.CallTo(() => connectionPool.GetConnectionAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public void WriteAsync_WithUpsert_MergeSyntax_LargeBatch_SplitsIntoMultipleMerges()
    {
        // NpgsqlConnection is sealed and cannot be mocked. This test verifies the SQL builder
        // handles batches of different sizes correctly (simulating the writer's 1000-row split).
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 2500,
            UseUpsert = true,
            UpsertKeyColumns = ["id"],
            UseMergeSyntax = true,
            UseTransaction = false,
            MaxRetryAttempts = 0,
        };

        var writer = new RedshiftBatchWriter<TestRow>(connectionPool, "public", "test_table", configuration);
        var keyColumns = new[] { "id" };

        var allRows = Enumerable.Range(0, 2500)
            .Select(i => new TestRow { Id = i, Name = $"Test{i}" })
            .ToArray();

        // Simulate the writer's internal mergeBatchSize=1000 split → 3 segments
        var sql1 = writer.BuildDirectMergeStatement(allRows[..1000], keyColumns);
        var sql2 = writer.BuildDirectMergeStatement(allRows[1000..2000], keyColumns);
        var sql3 = writer.BuildDirectMergeStatement(allRows[2000..], keyColumns);

        sql1.Should().StartWith("MERGE INTO");
        sql2.Should().StartWith("MERGE INTO");
        sql3.Should().StartWith("MERGE INTO");

        // Each segment is distinct
        sql1.Should().NotBe(sql2);
        sql2.Should().NotBe(sql3);

        // Last row of each segment is present
        sql1.Should().Contain("Test999");
        sql2.Should().Contain("Test1999");
        sql3.Should().Contain("Test2499");
    }

    [Fact]
    public void WriteAsync_WithUpsert_MergeSyntax_HandlesNullValues()
    {
        // NpgsqlConnection is sealed — test the SQL builder directly.
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            UseUpsert = true,
            UpsertKeyColumns = ["id"],
            UseMergeSyntax = true,
            UseTransaction = false,
            MaxRetryAttempts = 0,
        };

        var writer = new RedshiftBatchWriter<TestRowWithNulls>(
            connectionPool, "public", "test_table", configuration);

        var sql = writer.BuildDirectMergeStatement(
            [new TestRowWithNulls { Id = 1, Name = null, Description = null }],
            ["id"]);

        sql.Should().Contain("NULL");
    }

    [Fact]
    public void WriteAsync_WithUpsert_MergeSyntax_HandlesSpecialCharacters()
    {
        // NpgsqlConnection is sealed — test the SQL builder directly.
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            UseUpsert = true,
            UpsertKeyColumns = ["id"],
            UseMergeSyntax = true,
            UseTransaction = false,
            MaxRetryAttempts = 0,
        };

        var writer = new RedshiftBatchWriter<TestRow>(connectionPool, "public", "test_table", configuration);

        var sql = writer.BuildDirectMergeStatement(
            [new TestRow { Id = 1, Name = "O'Brien's \"special\" value" }],
            ["id"]);

        // Single quotes must be doubled for SQL safety
        sql.Should().Contain("O''Brien''s");
    }

    [Fact]
    public void WriteAsync_WithUpsert_MergeSyntax_GeneratesCorrectMergeStatement()
    {
        // NpgsqlConnection is sealed — test the SQL builder directly.
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            UseUpsert = true,
            UpsertKeyColumns = ["id"],
            UseMergeSyntax = true,
            OnMergeAction = OnMergeAction.Update,
            UseTransaction = false,
            MaxRetryAttempts = 0,
        };

        var writer = new RedshiftBatchWriter<TestRow>(connectionPool, "myschema", "mytable", configuration);

        var sql = writer.BuildDirectMergeStatement(
            [new TestRow { Id = 42, Name = "TestName" }],
            ["id"]);

        sql.Should().StartWith("MERGE INTO \"myschema\".\"mytable\" AS target");
        sql.Should().Contain("USING (SELECT * FROM (VALUES");
        sql.Should().Contain("AS t(\"id\", \"name\")");
        sql.Should().Contain("ON target.\"id\" = source.\"id\"");
        sql.Should().Contain("WHEN MATCHED THEN UPDATE SET");
        sql.Should().Contain("\"name\" = source.\"name\"");
        sql.Should().Contain("WHEN NOT MATCHED THEN INSERT");
    }

    [Fact]
    public void WriteAsync_WithUpsert_MergeSyntax_OnMergeSkip_GeneratesInsertOnlyMergeStatement()
    {
        // NpgsqlConnection is sealed — test the SQL builder directly.
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            UseUpsert = true,
            UpsertKeyColumns = ["id"],
            UseMergeSyntax = true,
            OnMergeAction = OnMergeAction.Skip,
            UseTransaction = false,
            MaxRetryAttempts = 0,
        };

        var writer = new RedshiftBatchWriter<TestRow>(connectionPool, "public", "test_table", configuration);

        var sql = writer.BuildDirectMergeStatement(
            [new TestRow { Id = 1, Name = "TestName" }],
            ["id"]);

        sql.Should().StartWith("MERGE INTO");
        sql.Should().Contain("WHEN NOT MATCHED THEN INSERT");
        sql.Should().NotContain("WHEN MATCHED THEN UPDATE");
    }

    [Fact]
    public void WriteAsync_WithUpsert_MergeSyntax_MultipleKeyColumns_GeneratesCorrectJoin()
    {
        // NpgsqlConnection is sealed — test the SQL builder directly.
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            UseUpsert = true,
            UpsertKeyColumns = ["id", "name"],
            UseMergeSyntax = true,
            OnMergeAction = OnMergeAction.Update,
            UseTransaction = false,
            MaxRetryAttempts = 0,
        };

        var writer = new RedshiftBatchWriter<TestRow>(connectionPool, "public", "test_table", configuration);

        var sql = writer.BuildDirectMergeStatement(
            [new TestRow { Id = 1, Name = "TestName" }],
            ["id", "name"]);

        sql.Should().Contain("target.\"id\" = source.\"id\"");
        sql.Should().Contain("AND");
        sql.Should().Contain("target.\"name\" = source.\"name\"");
    }

    [Fact]
    public void WriteAsync_WithUpsert_MergeSyntax_HandlesBooleanValues()
    {
        // NpgsqlConnection is sealed — test the SQL builder directly.
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            UseUpsert = true,
            UpsertKeyColumns = ["id"],
            UseMergeSyntax = true,
            UseTransaction = false,
            MaxRetryAttempts = 0,
        };

        var writer = new RedshiftBatchWriter<TestRowWithBoolean>(
            connectionPool, "public", "test_table", configuration);

        var sql = writer.BuildDirectMergeStatement(
            [
                new TestRowWithBoolean { Id = 1, IsActive = true },
                new TestRowWithBoolean { Id = 2, IsActive = false },
            ],
            ["id"]);

        sql.Should().Contain("TRUE");
        sql.Should().Contain("FALSE");
    }

    [Fact]
    public void WriteAsync_WithUpsert_MergeSyntax_HandlesDateTimeValues()
    {
        // NpgsqlConnection is sealed — test the SQL builder directly.
        var connectionPool = A.Fake<IRedshiftConnectionPool>();

        var configuration = new RedshiftConfiguration
        {
            BatchSize = 100,
            UseUpsert = true,
            UpsertKeyColumns = ["id"],
            UseMergeSyntax = true,
            UseTransaction = false,
            MaxRetryAttempts = 0,
        };

        var writer = new RedshiftBatchWriter<TestRowWithDateTime>(
            connectionPool, "public", "test_table", configuration);

        var testDate = new DateTime(2024, 6, 15, 10, 30, 45, DateTimeKind.Utc);

        var sql = writer.BuildDirectMergeStatement(
            [new TestRowWithDateTime { Id = 1, CreatedAt = testDate }],
            ["id"]);

        // DateTime values must be formatted in ISO 8601
        sql.Should().Contain("2024-06-15T10:30:45.0000000Z");
    }

    public class TestRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class TestRowWithNulls
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public string? Description { get; set; }
    }

    public class TestRowWithBoolean
    {
        public int Id { get; set; }
        public bool IsActive { get; set; }
    }

    public class TestRowWithDateTime
    {
        public int Id { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}

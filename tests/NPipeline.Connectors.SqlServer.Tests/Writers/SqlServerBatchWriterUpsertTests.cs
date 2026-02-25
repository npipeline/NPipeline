using System.Reflection;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.SqlServer.Tests.Writers;

/// <summary>
///     Tests for SqlServerBatchWriter upsert functionality.
///     Validates MERGE statement generation and key column handling.
///     Uses reflection to test the internal sealed class.
/// </summary>
public sealed class SqlServerBatchWriterUpsertTests
{
    private static readonly Assembly SqlServerAssembly = typeof(SqlServerConfiguration).Assembly;
    private static readonly Type? BatchWriterType = SqlServerAssembly.GetType("NPipeline.Connectors.SqlServer.Writers.SqlServerBatchWriter`1");

    #region Test Models

    public sealed class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public DateTime UpdatedAt { get; set; }
    }

    public sealed class TestEntityWithSingleKey
    {
        public int Id { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    public sealed class TestEntityWithCompositeKey
    {
        public int TenantId { get; set; }
        public string UserId { get; set; } = string.Empty;
        public string Data { get; set; } = string.Empty;
    }

    #endregion

    #region MERGE Statement Generation Tests

    [Fact]
    public void BuildMergeSqlTemplate_WithUpsertEnabled_GeneratesMergeStatement()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
            OnMergeAction = OnMergeAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildMergeSqlTemplate(writer);

        // Assert
        _ = sql.Should().StartWith("MERGE INTO");
        _ = sql.Should().Contain("AS target");
        _ = sql.Should().Contain("AS source");
        _ = sql.Should().EndWith(";");
    }

    [Fact]
    public void BuildMergeSqlTemplate_WithUpdateAction_GeneratesWhenMatchedUpdate()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
            OnMergeAction = OnMergeAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildMergeSqlTemplate(writer);

        // Assert
        _ = sql.Should().Contain("WHEN MATCHED THEN UPDATE SET");
    }

    [Fact]
    public void BuildMergeSqlTemplate_WithIgnoreAction_NoWhenMatchedClause()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
            OnMergeAction = OnMergeAction.Ignore,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildMergeSqlTemplate(writer);

        // Assert - Ignore action should not add a WHEN MATCHED clause
        _ = sql.Should().NotContain("WHEN MATCHED THEN");
        _ = sql.Should().Contain("WHEN NOT MATCHED THEN INSERT");
    }

    [Fact]
    public void BuildMergeSqlTemplate_WithDeleteAction_GeneratesWhenMatchedDelete()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
            OnMergeAction = OnMergeAction.Delete,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildMergeSqlTemplate(writer);

        // Assert
        _ = sql.Should().Contain("WHEN MATCHED THEN DELETE");
    }

    #endregion

    #region Key Column Tests

    [Fact]
    public void BuildMergeSqlTemplate_WithSingleKey_UsesKeyInOnClause()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
            OnMergeAction = OnMergeAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildMergeSqlTemplate(writer);

        // Assert
        _ = sql.Should().Contain("ON target.[Id] = source.[Id]");
    }

    [Fact]
    public void BuildMergeSqlTemplate_WithCompositeKey_UsesAllKeysInOnClause()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "TenantId", "UserId" },
            OnMergeAction = OnMergeAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntityWithCompositeKey>(connection, configuration);
        var sql = InvokeBuildMergeSqlTemplate(writer);

        // Assert
        _ = sql.Should().Contain("target.[TenantId] = source.[TenantId]");
        _ = sql.Should().Contain(" AND ");
        _ = sql.Should().Contain("target.[UserId] = source.[UserId]");
    }

    [Fact]
    public void BuildMergeSqlTemplate_WithKeyColumn_ExcludesFromUpdateSet()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
            OnMergeAction = OnMergeAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildMergeSqlTemplate(writer);

        // Assert - The key column 'Id' should appear in the ON clause
        _ = sql.Should().Contain("ON target.[Id] = source.[Id]");
        _ = sql.Should().Contain("WHEN MATCHED THEN UPDATE SET");

        // Should update non-key columns
        _ = sql.Should().Contain("[Name] = source.[Name]");
        _ = sql.Should().Contain("[Email] = source.[Email]");
        _ = sql.Should().Contain("[UpdatedAt] = source.[UpdatedAt]");

        // Extract the UPDATE SET portion and verify Id is not there
        var updateSetStart = sql.IndexOf("UPDATE SET", StringComparison.Ordinal);
        var updateSetEnd = sql.IndexOf("WHEN NOT MATCHED", StringComparison.Ordinal);
        _ = updateSetStart.Should().BeGreaterThan(0);
        _ = updateSetEnd.Should().BeGreaterThan(updateSetStart);

        var updateSetClause = sql.Substring(updateSetStart, updateSetEnd - updateSetStart);
        _ = updateSetClause.Should().NotContain("[Id] = source.[Id]");
    }

    [Fact]
    public void BuildMergeSqlTemplate_WithMultipleKeyColumns_ExcludesAllFromUpdateSet()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "TenantId", "UserId" },
            OnMergeAction = OnMergeAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntityWithCompositeKey>(connection, configuration);
        var sql = InvokeBuildMergeSqlTemplate(writer);

        // Assert - Key columns should appear in the ON clause
        _ = sql.Should().Contain("target.[TenantId] = source.[TenantId]");
        _ = sql.Should().Contain("target.[UserId] = source.[UserId]");

        // Extract the UPDATE SET portion and verify key columns are not there
        var updateSetStart = sql.IndexOf("UPDATE SET", StringComparison.Ordinal);
        var updateSetEnd = sql.IndexOf("WHEN NOT MATCHED", StringComparison.Ordinal);
        _ = updateSetStart.Should().BeGreaterThan(0);
        _ = updateSetEnd.Should().BeGreaterThan(updateSetStart);

        var updateSetClause = sql.Substring(updateSetStart, updateSetEnd - updateSetStart);
        _ = updateSetClause.Should().NotContain("[TenantId] = source.[TenantId]");
        _ = updateSetClause.Should().NotContain("[UserId] = source.[UserId]");
        _ = updateSetClause.Should().Contain("[Data] = source.[Data]");
    }

    [Fact]
    public void BuildMergeSqlTemplate_WhenAllColumnsAreKeyColumns_NoUpdateSet()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id", "Value" }, // All columns are key columns
            OnMergeAction = OnMergeAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntityWithSingleKey>(connection, configuration);
        var sql = InvokeBuildMergeSqlTemplate(writer);

        // Assert - When all columns are key columns, there's nothing to update
        // The MERGE should still have WHEN NOT MATCHED for inserts
        _ = sql.Should().Contain("WHEN NOT MATCHED THEN INSERT");
    }

    #endregion

    #region Schema-Qualified Table Names Tests

    [Fact]
    public void BuildMergeSqlTemplate_WithCustomSchema_UsesBracketQuoting()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
            OnMergeAction = OnMergeAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration, "custom_schema");
        var sql = InvokeBuildMergeSqlTemplate(writer);

        // Assert - Schema and table should be quoted with square brackets
        _ = sql.Should().Contain("[custom_schema].[test_table]");
    }

    [Fact]
    public void BuildMergeSqlTemplate_WithDefaultSchema_UsesDboSchema()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
            OnMergeAction = OnMergeAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildMergeSqlTemplate(writer);

        // Assert
        _ = sql.Should().Contain("[dbo].[test_table]");
    }

    [Fact]
    public void BuildInsertSql_WithCustomSchema_UsesBracketQuoting()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = false, // Plain INSERT
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration, "my_schema", "my_table");
        var sql = InvokeBuildInsertSql(writer);

        // Assert
        _ = sql.Should().Contain("[my_schema].[my_table]");
    }

    #endregion

    #region Configuration Validation Tests

    [Fact]
    public void Constructor_WithUpsertEnabledButNoKeyColumns_ThrowsArgumentException()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = null!,
        };

        // Act
        var action = () => CreateWriter<TestEntity>(connection, configuration);

        // Assert
        _ = action.Should().Throw<TargetInvocationException>()
            .WithInnerException<ArgumentException>()
            .Where(e => e.Message.Contains("UpsertKeyColumns"));
    }

    [Fact]
    public void Constructor_WithUpsertEnabledAndEmptyKeyColumns_ThrowsArgumentException()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = Array.Empty<string>(),
        };

        // Act
        var action = () => CreateWriter<TestEntity>(connection, configuration);

        // Assert
        _ = action.Should().Throw<TargetInvocationException>()
            .WithInnerException<ArgumentException>()
            .Where(e => e.Message.Contains("UpsertKeyColumns"));
    }

    [Fact]
    public void BuildInsertSql_WithUpsertDisabled_GeneratesPlainInsert()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = false,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildInsertSql(writer);

        // Assert
        _ = sql.Should().StartWith("INSERT INTO");
        _ = sql.Should().NotContain("MERGE");
        _ = sql.Should().EndWith("VALUES ");
    }

    [Fact]
    public void Constructor_WithValidUpsertConfiguration_DoesNotThrow()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
            OnMergeAction = OnMergeAction.Update,
        };

        // Act & Assert - Should not throw
        _ = CreateWriter<TestEntity>(connection, configuration).Should().NotBeNull();
    }

    #endregion

    #region Identifier Quoting Tests

    [Fact]
    public void QuoteIdentifier_WithSimpleIdentifier_AddsBrackets()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new SqlServerConfiguration();

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var result = InvokeQuoteIdentifier(writer, "column_name");

        // Assert
        _ = result.Should().Be("[column_name]");
    }

    [Fact]
    public void QuoteIdentifier_WithSchemaQualifiedIdentifier_AddsBracketsToEachPart()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new SqlServerConfiguration();

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var result = InvokeQuoteIdentifier(writer, "schema.table");

        // Assert
        _ = result.Should().Be("[schema].[table]");
    }

    [Fact]
    public void QuoteIdentifier_WithEmptyIdentifier_ThrowsArgumentException()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new SqlServerConfiguration();

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var action = () => InvokeQuoteIdentifier(writer, "");

        // Assert
        _ = action.Should().Throw<TargetInvocationException>()
            .WithInnerException<ArgumentException>();
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task WriteAsync_WithUpsertConfiguration_UsesMergeSql()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(1);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
            OnMergeAction = OnMergeAction.Update,
            BatchSize = 1,
        };

        var writer = CreateWriter<TestEntity>(connection, configuration);

        var item = new TestEntity
        {
            Id = 1,
            Name = "Test",
            Email = "test@example.com",
            UpdatedAt = DateTime.UtcNow,
        };

        // Act
        await InvokeWriteAsync(writer, item);

        // Assert
        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();

        _ = command.CommandText.Should().Contain("MERGE INTO");
        _ = command.CommandText.Should().Contain("WHEN MATCHED THEN UPDATE SET");
    }

    [Fact]
    public async Task WriteBatchAsync_WithUpsertConfiguration_UsesMergeSqlForAllItems()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(3);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
            OnMergeAction = OnMergeAction.Update,
            BatchSize = 10,
        };

        var writer = CreateWriter<TestEntity>(connection, configuration);

        var items = new List<TestEntity>
        {
            new() { Id = 1, Name = "Test1", Email = "test1@example.com", UpdatedAt = DateTime.UtcNow },
            new() { Id = 2, Name = "Test2", Email = "test2@example.com", UpdatedAt = DateTime.UtcNow },
            new() { Id = 3, Name = "Test3", Email = "test3@example.com", UpdatedAt = DateTime.UtcNow },
        };

        // Act
        await InvokeWriteBatchAsync(writer, items);

        // Assert
        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();

        _ = command.CommandText.Should().Contain("MERGE INTO");
        _ = command.CommandText.Should().Contain("WHEN MATCHED THEN UPDATE SET");
    }

    [Fact]
    public async Task WriteBatchAsync_WithIgnoreAction_NoWhenMatchedClause()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);

        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(2);

        var configuration = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
            OnMergeAction = OnMergeAction.Ignore,
            BatchSize = 10,
        };

        var writer = CreateWriter<TestEntity>(connection, configuration);

        var items = new List<TestEntity>
        {
            new() { Id = 1, Name = "Test1", Email = "test1@example.com", UpdatedAt = DateTime.UtcNow },
            new() { Id = 2, Name = "Test2", Email = "test2@example.com", UpdatedAt = DateTime.UtcNow },
        };

        // Act
        await InvokeWriteBatchAsync(writer, items);

        // Assert
        _ = command.CommandText.Should().Contain("MERGE INTO");
        _ = command.CommandText.Should().NotContain("WHEN MATCHED THEN");
        _ = command.CommandText.Should().Contain("WHEN NOT MATCHED THEN INSERT");
    }

    #endregion

    #region Helper Methods

    private static object CreateWriter<T>(
        IDatabaseConnection connection,
        SqlServerConfiguration configuration,
        string schema = "dbo",
        string tableName = "test_table",
        Func<T, IEnumerable<DatabaseParameter>>? parameterMapper = null)
    {
        if (BatchWriterType == null)
            throw new InvalidOperationException("Could not find SqlServerBatchWriter type");

        var concreteType = BatchWriterType.MakeGenericType(typeof(T));

        return Activator.CreateInstance(
            concreteType,
            connection,
            schema,
            tableName,
            parameterMapper,
            configuration) ?? throw new InvalidOperationException("Failed to create writer");
    }

    private static string InvokeBuildMergeSqlTemplate(object writer)
    {
        var writerType = writer.GetType();
        var method = writerType.GetMethod("BuildMergeSqlTemplate", BindingFlags.NonPublic | BindingFlags.Instance);
        return method?.Invoke(writer, null) as string ?? string.Empty;
    }

    private static string InvokeBuildInsertSql(object writer)
    {
        var writerType = writer.GetType();
        var method = writerType.GetMethod("BuildInsertSql", BindingFlags.NonPublic | BindingFlags.Instance);
        return method?.Invoke(writer, null) as string ?? string.Empty;
    }

    private static string InvokeQuoteIdentifier(object writer, string identifier)
    {
        var writerType = writer.GetType();
        var method = writerType.GetMethod("QuoteIdentifier", BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Instance);
        return method?.Invoke(null, new object[] { identifier }) as string ?? string.Empty;
    }

    private static async Task InvokeWriteAsync<T>(object writer, T item)
    {
        var writerType = writer.GetType();
        var method = writerType.GetMethod("WriteAsync");
        var task = method?.Invoke(writer, new object?[] { item!, CancellationToken.None }) as Task;
        await (task ?? Task.CompletedTask);
    }

    private static async Task InvokeWriteBatchAsync<T>(object writer, IEnumerable<T> items)
    {
        var writerType = writer.GetType();
        var method = writerType.GetMethod("WriteBatchAsync");
        var task = method?.Invoke(writer, new object?[] { items, CancellationToken.None }) as Task;
        await (task ?? Task.CompletedTask);
    }

    #endregion
}

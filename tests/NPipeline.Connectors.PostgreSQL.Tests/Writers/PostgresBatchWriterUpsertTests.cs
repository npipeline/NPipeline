using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using System.Reflection;

namespace NPipeline.Connectors.PostgreSQL.Tests.Writers;

/// <summary>
///     Tests for PostgresBatchWriter upsert functionality.
///     Validates ON CONFLICT clause generation and conflict column handling.
///     Uses reflection to test the internal sealed class.
/// </summary>
public sealed class PostgresBatchWriterUpsertTests
{
    private static readonly Assembly PostgresAssembly = typeof(PostgresConfiguration).Assembly;
    private static readonly Type? BatchWriterType = PostgresAssembly.GetType("NPipeline.Connectors.PostgreSQL.Writers.PostgresBatchWriter`1");

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

    #region ON CONFLICT DO UPDATE Tests

    [Fact]
    public void BuildInsertSql_WithUpsertEnabled_GeneratesOnConflictUpdate()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "id" },
            OnConflictAction = OnConflictAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildInsertSql(writer);

        // Assert
        _ = sql.Should().StartWith("INSERT INTO");
        _ = sql.Should().Contain("ON CONFLICT");
        _ = sql.Should().Contain("DO UPDATE SET");
    }

    [Fact]
    public void BuildInsertSql_WithSingleConflictColumn_SpecifiesColumnInConflictTarget()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "id" },
            OnConflictAction = OnConflictAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildInsertSql(writer);

        // Assert
        _ = sql.Should().Contain("ON CONFLICT (\"id\")");
    }

    [Fact]
    public void BuildInsertSql_WithCompositeConflictColumns_SpecifiesAllColumns()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "tenant_id", "user_id" },
            OnConflictAction = OnConflictAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntityWithCompositeKey>(connection, configuration);
        var sql = InvokeBuildInsertSql(writer);

        // Assert
        _ = sql.Should().Contain("ON CONFLICT (\"tenant_id\", \"user_id\")");
    }

    [Fact]
    public void BuildInsertSql_WithConflictColumn_ExcludesFromUpdateSet()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "id" },
            OnConflictAction = OnConflictAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildInsertSql(writer);

        // Assert - The conflict column 'id' should not appear in the UPDATE SET clause
        _ = sql.Should().Contain("DO UPDATE SET");

        // Should update non-conflict columns
        _ = sql.Should().Contain("EXCLUDED.\"name\"");
        _ = sql.Should().Contain("EXCLUDED.\"email\"");
        _ = sql.Should().Contain("EXCLUDED.\"updated_at\"");

        // Should NOT update the conflict column
        _ = sql.Should().NotContain("\"id\" = EXCLUDED.\"id\"");
    }

    [Fact]
    public void BuildInsertSql_WithMultipleConflictColumns_ExcludesAllFromUpdateSet()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "tenant_id", "user_id" },
            OnConflictAction = OnConflictAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntityWithCompositeKey>(connection, configuration);
        var sql = InvokeBuildInsertSql(writer);

        // Assert - Neither conflict column should be in UPDATE SET
        _ = sql.Should().NotContain("\"tenant_id\" = EXCLUDED.\"tenant_id\"");
        _ = sql.Should().NotContain("\"user_id\" = EXCLUDED.\"user_id\"");
        _ = sql.Should().Contain("\"data\" = EXCLUDED.\"data\"");
    }

    [Fact]
    public void BuildInsertSql_WhenAllColumnsAreConflictColumns_GeneratesDoNothing()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "id", "value" }, // All columns are conflict columns
            OnConflictAction = OnConflictAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntityWithSingleKey>(connection, configuration);
        var sql = InvokeBuildInsertSql(writer);

        // Assert - When all columns are conflict columns, there's nothing to update
        _ = sql.Should().Contain("DO NOTHING");
    }

    #endregion

    #region ON CONFLICT DO NOTHING Tests

    [Fact]
    public void BuildInsertSql_WithOnConflictDoNothing_GeneratesCorrectClause()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "id" },
            OnConflictAction = OnConflictAction.Ignore,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildInsertSql(writer);

        // Assert
        _ = sql.Should().Contain("ON CONFLICT (\"id\")");
        _ = sql.Should().Contain("DO NOTHING");
        _ = sql.Should().NotContain("DO UPDATE SET");
    }

    [Fact]
    public void BuildInsertSql_WithDoNothingAndCompositeKey_GeneratesCorrectClause()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "tenant_id", "user_id" },
            OnConflictAction = OnConflictAction.Ignore,
        };

        // Act
        var writer = CreateWriter<TestEntityWithCompositeKey>(connection, configuration);
        var sql = InvokeBuildInsertSql(writer);

        // Assert
        _ = sql.Should().Contain("ON CONFLICT (\"tenant_id\", \"user_id\")");
        _ = sql.Should().Contain("DO NOTHING");
    }

    #endregion

    #region Configuration Validation Tests

    [Fact]
    public void Constructor_WithUpsertEnabledButNoConflictColumns_ThrowsArgumentException()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = null!,
        };

        // Act
        var action = () => CreateWriter<TestEntity>(connection, configuration);

        // Assert
        _ = action.Should().Throw<TargetInvocationException>()
            .WithInnerException<ArgumentException>()
            .Where(e => e.Message.Contains("UpsertConflictColumns"));
    }

    [Fact]
    public void Constructor_WithUpsertEnabledAndEmptyConflictColumns_ThrowsArgumentException()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = Array.Empty<string>(),
        };

        // Act
        var action = () => CreateWriter<TestEntity>(connection, configuration);

        // Assert
        _ = action.Should().Throw<TargetInvocationException>()
            .WithInnerException<ArgumentException>()
            .Where(e => e.Message.Contains("UpsertConflictColumns"));
    }

    [Fact]
    public void BuildInsertSql_WithUpsertDisabled_GeneratesPlainInsert()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = false,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildInsertSql(writer);

        // Assert
        _ = sql.Should().StartWith("INSERT INTO");
        _ = sql.Should().NotContain("ON CONFLICT");
        _ = sql.Should().EndWith("VALUES ");
    }

    [Fact]
    public void Constructor_WithValidUpsertConfiguration_DoesNotThrow()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "id" },
            OnConflictAction = OnConflictAction.Update,
        };

        // Act & Assert - Should not throw
        _ = CreateWriter<TestEntity>(connection, configuration).Should().NotBeNull();
    }

    #endregion

    #region SQL Format Tests

    [Fact]
    public void BuildInsertSql_WithCustomSchema_IncludesSchemaInInsert()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "id" },
            OnConflictAction = OnConflictAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration, "custom_schema", "test_table");
        var sql = InvokeBuildInsertSql(writer);

        // Assert - Schema and table are quoted together
        _ = sql.Should().Contain("\"custom_schema.test_table\"");
    }

    [Fact]
    public void BuildInsertSql_WithSnakeCaseColumns_UsesSnakeCaseNames()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "id" },
            OnConflictAction = OnConflictAction.Update,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildInsertSql(writer);

        // Assert
        _ = sql.Should().Contain("\"id\"");
        _ = sql.Should().Contain("\"name\"");
        _ = sql.Should().Contain("\"email\"");
        _ = sql.Should().Contain("\"updated_at\"");
    }

    [Fact]
    public void BuildInsertSql_WithValidateIdentifiers_QuotesAllIdentifiers()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(command);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "id" },
            OnConflictAction = OnConflictAction.Update,
            ValidateIdentifiers = true,
        };

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var sql = InvokeBuildInsertSql(writer);

        // Assert - Schema and table are quoted together
        _ = sql.Should().Contain("\"public.test_table\"");
        _ = sql.Should().Contain("\"id\"");
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task WriteAsync_WithUpsertConfiguration_UsesUpsertSql()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);
        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(1);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "id" },
            OnConflictAction = OnConflictAction.Update,
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

        _ = command.CommandText.Should().Contain("ON CONFLICT");
        _ = command.CommandText.Should().Contain("DO UPDATE SET");
    }

    [Fact]
    public async Task WriteBatchAsync_WithUpsertConfiguration_UsesUpsertSqlForAllItems()
    {
        // Arrange
        var connection = A.Fake<IDatabaseConnection>();
        var command = A.Fake<IDatabaseCommand>();

        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._))
            .Returns(command);
        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._))
            .Returns(3);

        var configuration = new PostgresConfiguration
        {
            UseUpsert = true,
            UpsertConflictColumns = new[] { "id" },
            OnConflictAction = OnConflictAction.Update,
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

        _ = command.CommandText.Should().Contain("ON CONFLICT");
        _ = command.CommandText.Should().Contain("DO UPDATE SET");
    }

    #endregion

    #region Helper Methods

    private static object CreateWriter<T>(
        IDatabaseConnection connection,
        PostgresConfiguration configuration,
        string schema = "public",
        string tableName = "test_table",
        Func<T, IEnumerable<DatabaseParameter>>? parameterMapper = null)
    {
        if (BatchWriterType == null)
        {
            throw new InvalidOperationException("Could not find PostgresBatchWriter type");
        }

        var concreteType = BatchWriterType.MakeGenericType(typeof(T));

        return Activator.CreateInstance(
            concreteType,
            connection,
            schema,
            tableName,
            parameterMapper,
            configuration) ?? throw new InvalidOperationException("Failed to create writer");
    }

    private static string InvokeBuildInsertSql(object writer)
    {
        var writerType = writer.GetType();
        var method = writerType.GetMethod("BuildInsertSql", BindingFlags.NonPublic | BindingFlags.Instance);
        return method?.Invoke(writer, null) as string ?? string.Empty;
    }

    private static async Task InvokeWriteAsync<T>(object writer, T item)
    {
        var writerType = writer.GetType();
        var method = writerType.GetMethod("WriteAsync");
        var task = method?.Invoke(writer, new object?[] { item, CancellationToken.None }) as Task;
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

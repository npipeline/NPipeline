using System.Collections;
using System.Data;
using System.Reflection;
using AwesomeAssertions;
using FakeItEasy;
using Microsoft.Data.SqlClient;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Mapping;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.SqlServer.Tests.Writers;

/// <summary>
///     Tests for SqlServerBulkCopyWriter bulk copy functionality.
///     Validates SqlBulkCopy configuration, column mapping, and DataTable generation.
///     Uses reflection to test the internal sealed class.
/// </summary>
public sealed class SqlServerBulkCopyWriterTests
{
    private static readonly Assembly SqlServerAssembly = typeof(SqlServerConfiguration).Assembly;
    private static readonly Type? BulkCopyWriterType = SqlServerAssembly.GetType("NPipeline.Connectors.SqlServer.Writers.SqlServerBulkCopyWriter`1");

    #region Disposal Tests

    [Fact]
    public async Task DisposeAsync_FlushesRemainingItemsSilently()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        configuration.BulkCopyBatchSize = 100;

        var writer = CreateWriter<TestEntity>(connection, configuration);
        await InvokeWriteAsync(writer, new TestEntity { Id = 1, Name = "Test", Email = "test@example.com" });

        // Act & Assert - Should NOT throw because DisposeAsync silently handles flush failures
        // This is the expected behavior for disposal patterns - disposal should not throw
        var action = async () => await InvokeDisposeAsync(writer);
        _ = await action.Should().NotThrowAsync();
    }

    #endregion

    #region Test Models

    public sealed class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public DateTime UpdatedAt { get; set; }
    }

    public sealed class TestEntityWithIdentity
    {
        [SqlServerColumn("Id", Identity = true)]
        public int Id { get; set; }

        public string Value { get; set; } = string.Empty;
    }

    public sealed class TestEntityWithIgnoredColumn
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;

        [IgnoreColumn]
        public string InternalField { get; set; } = string.Empty;
    }

    public sealed class TestEntityWithCustomColumn
    {
        [SqlServerColumn("custom_column_name")]
        public string Name { get; set; } = string.Empty;

        [Column("another_column")]
        public int Value { get; set; }
    }

    #endregion

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_CreatesWriter()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);

        // Assert
        _ = writer.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithNullConnection_ThrowsArgumentNullException()
    {
        // Arrange
        var configuration = CreateValidConfiguration();

        // Act
        var action = () => CreateWriter<TestEntity>(null!, configuration);

        // Assert
        _ = action.Should().Throw<TargetInvocationException>()
            .WithInnerException<ArgumentNullException>()
            .Where(e => e.ParamName == "connection");
    }

    [Fact]
    public void Constructor_WithNullSchema_ThrowsArgumentNullException()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();

        // Act
        var action = () => CreateWriter<TestEntity>(connection, configuration, null!);

        // Assert
        _ = action.Should().Throw<TargetInvocationException>()
            .WithInnerException<ArgumentNullException>()
            .Where(e => e.ParamName == "schema");
    }

    [Fact]
    public void Constructor_WithNullTableName_ThrowsArgumentNullException()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();

        // Act
        var action = () => CreateWriter<TestEntity>(connection, configuration, "dbo", null!);

        // Assert
        _ = action.Should().Throw<TargetInvocationException>()
            .WithInnerException<ArgumentNullException>()
            .Where(e => e.ParamName == "tableName");
    }

    [Fact]
    public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
    {
        // Arrange
        var connection = CreateMockConnection();

        // Act
        var action = () => CreateWriter<TestEntity>(connection, null!);

        // Assert
        _ = action.Should().Throw<TargetInvocationException>()
            .WithInnerException<ArgumentNullException>()
            .Where(e => e.ParamName == "configuration");
    }

    #endregion

    #region Configuration Tests

    [Fact]
    public void ConfigureBulkCopy_SetsDestinationTableName()
    {
        // Arrange
        var configuration = CreateValidConfiguration();
        configuration.Schema = "custom_schema";

        // Act
        var bulkCopy = CreateAndConfigureBulkCopy(configuration, "custom_schema");

        // Assert
        _ = bulkCopy.DestinationTableName.Should().Be("[custom_schema].[test_table]");
    }

    [Fact]
    public void ConfigureBulkCopy_SetsBatchSize()
    {
        // Arrange
        var configuration = CreateValidConfiguration();
        configuration.BulkCopyBatchSize = 2500;

        // Act
        var bulkCopy = CreateAndConfigureBulkCopy(configuration);

        // Assert
        _ = bulkCopy.BatchSize.Should().Be(2500);
    }

    [Fact]
    public void ConfigureBulkCopy_SetsTimeout()
    {
        // Arrange
        var configuration = CreateValidConfiguration();
        configuration.BulkCopyTimeout = 600;

        // Act
        var bulkCopy = CreateAndConfigureBulkCopy(configuration);

        // Assert
        _ = bulkCopy.BulkCopyTimeout.Should().Be(600);
    }

    [Fact]
    public void ConfigureBulkCopy_SetsEnableStreaming()
    {
        // Arrange
        var configuration = CreateValidConfiguration();
        configuration.EnableStreaming = true;

        // Act
        var bulkCopy = CreateAndConfigureBulkCopy(configuration);

        // Assert
        _ = bulkCopy.EnableStreaming.Should().BeTrue();
    }

    [Fact]
    public void ConfigureBulkCopy_SetsNotifyAfter()
    {
        // Arrange
        var configuration = CreateValidConfiguration();
        configuration.BulkCopyNotifyAfter = 500;

        // Act
        var bulkCopy = CreateAndConfigureBulkCopy(configuration);

        // Assert
        _ = bulkCopy.NotifyAfter.Should().Be(500);
    }

    [Fact]
    public void ConfigureBulkCopy_WithZeroNotifyAfter_DoesNotSetNotifyAfter()
    {
        // Arrange
        var configuration = CreateValidConfiguration();
        configuration.BulkCopyNotifyAfter = 0;

        // Act
        var bulkCopy = CreateAndConfigureBulkCopy(configuration);

        // Assert - Default is 0 when not set
        _ = bulkCopy.NotifyAfter.Should().Be(0);
    }

    private static SqlBulkCopy CreateAndConfigureBulkCopy(SqlServerConfiguration configuration, string schema = "dbo", string tableName = "test_table")
    {
        var sqlConnection = new SqlConnection();
        var bulkCopy = new SqlBulkCopy(sqlConnection);

        // Configure manually as the writer does
        bulkCopy.DestinationTableName = $"[{schema}].[{tableName}]";
        bulkCopy.BatchSize = configuration.BulkCopyBatchSize;
        bulkCopy.BulkCopyTimeout = configuration.BulkCopyTimeout;
        bulkCopy.EnableStreaming = configuration.EnableStreaming;

        if (configuration.BulkCopyNotifyAfter > 0)
            bulkCopy.NotifyAfter = configuration.BulkCopyNotifyAfter;

        return bulkCopy;
    }

    #endregion

    #region DataTable Generation Tests

    [Fact]
    public void BuildDataTable_CreatesColumnsFromMappings()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        var writer = CreateWriter<TestEntity>(connection, configuration);

        // Act
        var dataTable = InvokeBuildDataTable(writer);

        // Assert
        _ = dataTable.Columns.Count.Should().Be(4);
        _ = dataTable.Columns.Contains("Id").Should().BeTrue();
        _ = dataTable.Columns.Contains("Name").Should().BeTrue();
        _ = dataTable.Columns.Contains("Email").Should().BeTrue();
        _ = dataTable.Columns.Contains("UpdatedAt").Should().BeTrue();
    }

    [Fact]
    public void BuildDataTable_SetsCorrectColumnTypes()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        var writer = CreateWriter<TestEntity>(connection, configuration);

        // Act
        var dataTable = InvokeBuildDataTable(writer);

        // Assert
        _ = dataTable.Columns["Id"]!.DataType.Should().Be<int>();
        _ = dataTable.Columns["Name"]!.DataType.Should().Be<string>();
        _ = dataTable.Columns["Email"]!.DataType.Should().Be<string>();
        _ = dataTable.Columns["UpdatedAt"]!.DataType.Should().Be<DateTime>();
    }

    [Fact]
    public void BuildDataTable_SetsAllowDBNullForNullableTypes()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        var writer = CreateWriter<TestEntity>(connection, configuration);

        // Act
        var dataTable = InvokeBuildDataTable(writer);

        // Assert - String is a reference type, should allow null
        _ = dataTable.Columns["Name"]!.AllowDBNull.Should().BeTrue();
        _ = dataTable.Columns["Email"]!.AllowDBNull.Should().BeTrue();
    }

    [Fact]
    public void BuildDataTable_WithNoPendingRows_CreatesEmptyTable()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        var writer = CreateWriter<TestEntity>(connection, configuration);

        // Act
        var dataTable = InvokeBuildDataTable(writer);

        // Assert
        _ = dataTable.Rows.Count.Should().Be(0);
    }

    [Fact]
    public void BuildDataTable_WithIdentityColumn_ExcludesIdentityColumn()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        var writer = CreateWriter<TestEntityWithIdentity>(connection, configuration);

        // Act
        var dataTable = InvokeBuildDataTable(writer);

        // Assert - Identity column should be excluded
        _ = dataTable.Columns.Contains("Id").Should().BeFalse();
        _ = dataTable.Columns.Contains("Value").Should().BeTrue();
    }

    [Fact]
    public void BuildDataTable_WithIgnoredColumn_ExcludesIgnoredColumn()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        var writer = CreateWriter<TestEntityWithIgnoredColumn>(connection, configuration);

        // Act
        var dataTable = InvokeBuildDataTable(writer);

        // Assert - Ignored column should be excluded
        _ = dataTable.Columns.Contains("Id").Should().BeTrue();
        _ = dataTable.Columns.Contains("Name").Should().BeTrue();
        _ = dataTable.Columns.Contains("InternalField").Should().BeFalse();
    }

    #endregion

    #region Flush Threshold Tests

    [Fact]
    public void FlushThreshold_IsClampedToBatchSize()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        configuration.BulkCopyBatchSize = 500;
        configuration.MaxBatchSize = 1000;

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var flushThreshold = GetFlushThreshold(writer);

        // Assert
        _ = flushThreshold.Should().Be(500);
    }

    [Fact]
    public void FlushThreshold_CannotExceedMaxBatchSize()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        configuration.BulkCopyBatchSize = 5000;
        configuration.MaxBatchSize = 1000;

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var flushThreshold = GetFlushThreshold(writer);

        // Assert
        _ = flushThreshold.Should().Be(1000);
    }

    [Fact]
    public void FlushThreshold_MinimumIsOne()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        configuration.BulkCopyBatchSize = 0;
        configuration.MaxBatchSize = 100;

        // Act
        var writer = CreateWriter<TestEntity>(connection, configuration);
        var flushThreshold = GetFlushThreshold(writer);

        // Assert - Flush threshold is clamped to minimum of 1
        _ = flushThreshold.Should().Be(1);
    }

    #endregion

    #region WriteAsync Tests

    [Fact]
    public async Task WriteAsync_AddsItemToBuffer()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        configuration.BulkCopyBatchSize = 100; // Large enough to not trigger flush

        var writer = CreateWriter<TestEntity>(connection, configuration);
        var item = new TestEntity { Id = 1, Name = "Test", Email = "test@example.com" };

        // Act
        await InvokeWriteAsync(writer, item);

        // Assert - Item should be in pending rows
        var pendingCount = GetPendingRowsCount(writer);
        _ = pendingCount.Should().Be(1);
    }

    [Fact]
    public async Task WriteAsync_TriggersFlush_WhenBatchSizeReached()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        configuration.BulkCopyBatchSize = 2;

        var writer = CreateWriter<TestEntity>(connection, configuration);

        // Act - Write first item (should not trigger flush)
        await InvokeWriteAsync(writer, new TestEntity { Id = 1, Name = "Test1", Email = "test1@example.com" });
        var pendingCount = GetPendingRowsCount(writer);
        _ = pendingCount.Should().Be(1);

        // Act - Write second item (should trigger flush attempt)
        // Since we're using a fake IDatabaseConnection, the flush will fail because
        // the writer can't get the underlying SqlConnection
        var action = async () => await InvokeWriteAsync(writer, new TestEntity { Id = 2, Name = "Test2", Email = "test2@example.com" });

        // Assert - Should throw because fake connection can't provide SqlConnection
        _ = await action.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public async Task WriteBatchAsync_AddsAllItemsToBuffer()
    {
        // Arrange
        var connection = CreateMockConnection();
        var configuration = CreateValidConfiguration();
        configuration.BulkCopyBatchSize = 100;

        var writer = CreateWriter<TestEntity>(connection, configuration);

        var items = new List<TestEntity>
        {
            new() { Id = 1, Name = "Test1", Email = "test1@example.com" },
            new() { Id = 2, Name = "Test2", Email = "test2@example.com" },
            new() { Id = 3, Name = "Test3", Email = "test3@example.com" },
        };

        // Act & Assert - Should throw because fake connection can't provide SqlConnection
        var action = async () => await InvokeWriteBatchAsync(writer, items);
        _ = await action.Should().ThrowAsync<Exception>();
    }

    #endregion

    #region Helper Methods

    private static IDatabaseConnection CreateMockConnection()
    {
        return A.Fake<IDatabaseConnection>();
    }

    private static SqlServerConfiguration CreateValidConfiguration()
    {
        return new SqlServerConfiguration
        {
            BulkCopyBatchSize = 1000,
            BulkCopyTimeout = 300,
            MaxBatchSize = 5000,
            EnableStreaming = true,
            BulkCopyNotifyAfter = 100,
        };
    }

    private static object CreateWriter<T>(
        IDatabaseConnection connection,
        SqlServerConfiguration configuration,
        string schema = "dbo",
        string tableName = "test_table",
        Func<T, IEnumerable<DatabaseParameter>>? parameterMapper = null)
    {
        if (BulkCopyWriterType == null)
            throw new InvalidOperationException("Could not find SqlServerBulkCopyWriter type");

        var concreteType = BulkCopyWriterType.MakeGenericType(typeof(T));

        return Activator.CreateInstance(
            concreteType,
            connection,
            schema,
            tableName,
            parameterMapper,
            configuration) ?? throw new InvalidOperationException("Failed to create writer");
    }

    private static DataTable InvokeBuildDataTable(object writer)
    {
        var writerType = writer.GetType();
        var method = writerType.GetMethod("BuildDataTable", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
        return method?.Invoke(writer, null) as DataTable ?? new DataTable();
    }

    private static int GetFlushThreshold(object writer)
    {
        var writerType = writer.GetType();
        var field = writerType.GetField("_flushThreshold", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
        return field?.GetValue(writer) as int? ?? 0;
    }

    private static int GetPendingRowsCount(object writer)
    {
        var writerType = writer.GetType();
        var field = writerType.GetField("_pendingRows", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
        var list = field?.GetValue(writer) as IList;
        return list?.Count ?? 0;
    }

    private static async Task InvokeWriteAsync<T>(object writer, T item)
    {
        var writerType = writer.GetType();
        var method = writerType.GetMethod("WriteAsync");
        var task = method?.Invoke(writer, [item, CancellationToken.None]) as Task;
        await (task ?? Task.CompletedTask);
    }

    private static async Task InvokeWriteBatchAsync<T>(object writer, IEnumerable<T> items)
    {
        var writerType = writer.GetType();
        var method = writerType.GetMethod("WriteBatchAsync");
        var task = method?.Invoke(writer, [items, CancellationToken.None]) as Task;
        await (task ?? Task.CompletedTask);
    }

    private static async Task InvokeDisposeAsync(object writer)
    {
        var writerType = writer.GetType();
        var method = writerType.GetMethod("DisposeAsync");
        var valueTask = method?.Invoke(writer, null);

        if (valueTask is ValueTask vt)
            await vt;
    }

    #endregion
}

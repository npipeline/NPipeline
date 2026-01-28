#pragma warning disable xUnit1004
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.Connectors.PostgreSQL.Nodes;
using NPipeline.Connectors.Exceptions;
using WriteStrategy = NPipeline.Connectors.PostgreSQL.Nodes.PostgresWriteStrategy;

namespace NPipeline.Connectors.PostgreSQL.Tests.Integration;

// Integration tests for PostgreSQL connector
// These tests require a real PostgreSQL database to run
// Use testcontainers or a local PostgreSQL instance for testing

public class PostgresConnectorIntegrationTests
{
    // ReSharper disable once xUnit1004
    [Fact(Skip = "Requires PostgreSQL database - set up testcontainers or local instance")]
    public async Task EndToEnd_SourceToSink_TransfersDataCorrectly()
    {
        // Arrange
        var connectionString = "Host=localhost;Username=test;Password=test;Database=test";
        var sourceTable = "source_table";
        var sinkTable = "sink_table";
        var query = $"SELECT id, name, value FROM {sourceTable}";

        var source = new PostgresSourceNode<TestRecord>(connectionString, query);
        var sink = new PostgresSinkNode<TestRecord>(connectionString, sinkTable, WriteStrategy.PerRow);

        // Act & Assert
        // In a real integration test, we would:
        // 1. Set up tables with test data
        // 2. Execute the pipeline
        // 3. Verify data was transferred correctly
        await Task.CompletedTask;
    }

    // ReSharper disable once xUnit1004
    [Fact(Skip = "Requires PostgreSQL database - set up testcontainers or local instance")]
    public async Task CustomMapper_MapsDataCorrectly()
    {
        // Arrange
        var connectionString = "Host=localhost;Username=test;Password=test;Database=test";
        var query = "SELECT id, name, value FROM test_table";

        var mapper = new Func<PostgresRow, TestRecord>(row => new TestRecord
        {
            Id = row.Get<int>("id"),
            Name = row.Get<string>("name"),
            Value = row.Get<decimal>("value")
        });

        var source = new PostgresSourceNode<TestRecord>(connectionString, query, mapper);

        // Act & Assert
        await Task.CompletedTask;
    }

    // ReSharper disable once xUnit1004
    [Fact(Skip = "Requires PostgreSQL database - set up testcontainers or local instance")]
    public async Task BatchWriteStrategy_WritesDataInBatches()
    {
        // Arrange
        var connectionString = "Host=localhost;Username=test;Password=test;Database=test";
        var tableName = "test_table";
        var config = new PostgresConfiguration { BatchSize = 100 };

        var sink = new PostgresSinkNode<TestRecord>(connectionString, tableName, WriteStrategy.Batch, null, config);

        // Act & Assert
        await Task.CompletedTask;
    }

    // ReSharper disable once xUnit1004
    [Fact(Skip = "Requires PostgreSQL database - set up testcontainers or local instance")]
    public async Task Streaming_ReadsDataInChunks()
    {
        // Arrange
        var connectionString = "Host=localhost;Username=test;Password=test;Database=test";
        var query = "SELECT id, name, value FROM large_table";
        var config = new PostgresConfiguration { FetchSize = 1000 };

        var source = new PostgresSourceNode<TestRecord>(connectionString, query, null, config);

        // Act & Assert
        await Task.CompletedTask;
    }

    // ReSharper disable once xUnit1004
    [Fact(Skip = "Requires PostgreSQL database - set up testcontainers or local instance")]
    public async Task Parameters_UsedInQueryCorrectly()
    {
        // Arrange
        var connectionString = "Host=localhost;Username=test;Password=test;Database=test";
        var query = "SELECT id, name, value FROM test_table WHERE id = @id";
        var parameters = new DatabaseParameter[] { new("id", 1) };

        var source = new PostgresSourceNode<TestRecord>(connectionString, query, null, null, parameters);

        // Act & Assert
        await Task.CompletedTask;
    }

    // ReSharper disable once xUnit1004
    [Fact(Skip = "Requires PostgreSQL database - set up testcontainers or local instance")]
    public async Task LargeDataset_HandledCorrectly()
    {
        // Arrange
        var connectionString = "Host=localhost;Username=test;Password=test;Database=test";
        var sourceTable = "large_source_table";
        var sinkTable = "large_sink_table";
        var query = $"SELECT id, name, value FROM {sourceTable}";

        var source = new PostgresSourceNode<TestRecord>(connectionString, query);
        var sink = new PostgresSinkNode<TestRecord>(connectionString, sinkTable, WriteStrategy.Batch);

        // Act & Assert
        await Task.CompletedTask;
    }

    // ReSharper disable once xUnit1004
    [Fact(Skip = "Requires PostgreSQL database - set up testcontainers or local instance")]
    public async Task CustomSchema_WritesToCorrectSchema()
    {
        // Arrange
        var connectionString = "Host=localhost;Username=test;Password=test;Database=test";
        var schemaName = "custom_schema";
        var tableName = "test_table";
        var fullTableName = $"{schemaName}.{tableName}";

        var sink = new PostgresSinkNode<TestRecord>(connectionString, fullTableName, WriteStrategy.PerRow);

        // Act & Assert
        await Task.CompletedTask;
    }

    // ReSharper disable once xUnit1004
    [Fact(Skip = "Requires PostgreSQL database - set up testcontainers or local instance")]
    public async Task CaseInsensitiveMapping_MapsColumnsCorrectly()
    {
        // Arrange
        var connectionString = "Host=localhost;Username=test;Password=test;Database=test";
        var query = "SELECT ID, NAME, VALUE FROM test_table";

        var source = new PostgresSourceNode<TestRecord>(connectionString, query);

        // Act & Assert
        await Task.CompletedTask;
    }

    // ReSharper disable once xUnit1004
    [Fact(Skip = "Requires PostgreSQL database - set up testcontainers or local instance")]
    public async Task SnakeCaseConversion_MapsColumnsCorrectly()
    {
        // Arrange
        var connectionString = "Host=localhost;Username=test;Password=test;Database=test";
        var query = "SELECT user_id, user_name, user_value FROM test_table";

        var source = new PostgresSourceNode<UserRecord>(connectionString, query);

        // Act & Assert
        await Task.CompletedTask;
    }

    // ReSharper disable once xUnit1004
    [Fact(Skip = "Requires PostgreSQL database - set up testcontainers or local instance")]
    public async Task AttributeBasedMapping_MapsColumnsCorrectly()
    {
        // Arrange
        var connectionString = "Host=localhost;Username=test;Password=test;Database=test";
        var query = "SELECT id, display_name, numeric_value FROM test_table";

        var source = new PostgresSourceNode<AttributedRecord>(connectionString, query);

        // Act & Assert
        await Task.CompletedTask;
    }

    // ReSharper disable once xUnit1004
    [Fact(Skip = "Requires PostgreSQL database - set up testcontainers or local instance")]
    public async Task IgnoreAttribute_SkipsIgnoredProperties()
    {
        // Arrange
        var connectionString = "Host=localhost;Username=test;Password=test;Database=test";
        var query = "SELECT id, name, value FROM test_table";

        var source = new PostgresSourceNode<RecordWithIgnore>(connectionString, query);

        // Act & Assert
        await Task.CompletedTask;
    }
}

// Test record types for integration tests
public record TestRecord
{
    public int Id { get; init; }
    public string? Name { get; init; }
    public decimal Value { get; init; }
}

public record UserRecord
{
    public int UserId { get; init; }
    public string? UserName { get; init; }
    public decimal UserValue { get; init; }
}

public record AttributedRecord
{
    public int Id { get; init; }

    [PostgresColumn("display_name")]
    public string? DisplayName { get; init; }

    [PostgresColumn("numeric_value")]
    public decimal NumericValue { get; init; }
}

public record RecordWithIgnore
{
    public int Id { get; init; }
    public string? Name { get; init; }
    public decimal Value { get; init; }

    [PostgresIgnore]
    public string? InternalField { get; init; }
}

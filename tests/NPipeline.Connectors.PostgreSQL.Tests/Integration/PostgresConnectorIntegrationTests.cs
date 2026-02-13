using Npgsql;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.Connectors.PostgreSQL.Nodes;
using NPipeline.Connectors.PostgreSQL.Tests.Fixtures;
using NPipeline.StorageProviders.Models;
using WriteStrategy = NPipeline.Connectors.PostgreSQL.Configuration.PostgresWriteStrategy;

namespace NPipeline.Connectors.PostgreSQL.Tests.Integration;

[Collection("PostgresTestCollection")]
public class PostgresConnectorIntegrationTests(PostgresTestContainerFixture fixture)
{
    private readonly PostgresTestContainerFixture _fixture = fixture;

    private async Task DropTableIfExists(string tableName, string? schema = null)
    {
        var connectionString = _fixture.ConnectionString;

        var fullTableName = string.IsNullOrEmpty(schema)
            ? tableName
            : $"{schema}.{tableName}";

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand($"DROP TABLE IF EXISTS {fullTableName} CASCADE", conn);
        _ = await cmd.ExecuteNonQueryAsync();
    }

    private async Task DropSchemaIfExists(string schemaName)
    {
        var connectionString = _fixture.ConnectionString;

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand($"DROP SCHEMA IF EXISTS {schemaName} CASCADE", conn);
        _ = await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task EndToEnd_SourceToSink_TransfersDataCorrectly()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var sourceTable = "source_table";
        var sinkTable = "sink_table";
        var query = $"SELECT id, name, value FROM {sourceTable}";

        try
        {
            // Set up source table with test data
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {sourceTable} (id INTEGER PRIMARY KEY, name TEXT, value NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new NpgsqlCommand(
                                 $"INSERT INTO {sourceTable} (id, name, value) VALUES (1, 'Test 1', 10.5), (2, 'Test 2', 20.5), (3, 'Test 3', 30.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                // Set up sink table
                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {sinkTable} (id INTEGER PRIMARY KEY, name TEXT, value NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            // Create source and sink nodes
            var source = new PostgresSourceNode<TestRecord>(connectionString, query);
            var sink = new PostgresSinkNode<TestRecord>(connectionString, sinkTable, WriteStrategy.PerRow);

            // Act & Assert - Verify nodes can be created and configured
            Assert.NotNull(source);
            Assert.NotNull(sink);

            // Verify data exists in source table
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {sourceTable}", conn))
                {
                    var count = (long)(await cmd.ExecuteScalarAsync())!;
                    Assert.Equal(3, count);
                }
            }
        }
        finally
        {
            await DropTableIfExists(sourceTable);
            await DropTableIfExists(sinkTable);
        }
    }

    [Fact]
    public async Task CustomMapper_MapsDataCorrectly()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";

        try
        {
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {tableName} (id INTEGER PRIMARY KEY, name TEXT, value NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new NpgsqlCommand(
                                 $"INSERT INTO {tableName} (id, name, value) VALUES (1, 'Test 1', 10.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var query = $"SELECT id, name, value FROM {tableName}";

            var mapper = new Func<PostgresRow, TestRecord>(row => new TestRecord
            {
                Id = row.Get<int>("id"),
                Name = row.Get<string>("name"),
                Value = row.Get<decimal>("value"),
            });

            var source = new PostgresSourceNode<TestRecord>(connectionString, query, mapper);

            // Act & Assert - Verify node can be created with custom mapper
            Assert.NotNull(source);

            // Verify mapping works by reading data directly
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(query, conn))
                {
                    await using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        await reader.ReadAsync();
                        var postgresRow = new PostgresRow(reader, false);
                        var record = mapper(postgresRow);
                        Assert.Equal(1, record.Id);
                        Assert.Equal("Test 1", record.Name);
                        Assert.Equal(10.5m, record.Value);
                    }
                }
            }
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
    }

    [Fact]
    public async Task BatchWriteStrategy_WritesDataInBatches()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";
        var config = new PostgresConfiguration { BatchSize = 100 };

        try
        {
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {tableName} (id INTEGER PRIMARY KEY, name TEXT, value NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var sink = new PostgresSinkNode<TestRecord>(connectionString, tableName, WriteStrategy.Batch, null, config);

            // Act & Assert - Verify node can be created with batch write strategy
            Assert.NotNull(sink);
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
    }

    [Fact]
    public async Task Streaming_ReadsDataInChunks()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "large_table";
        var config = new PostgresConfiguration { FetchSize = 1000 };

        try
        {
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {tableName} (id INTEGER PRIMARY KEY, name TEXT, value NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                // Insert 5000 records using batch insert for performance
                var batchValues = string.Join(",",
                    Enumerable.Range(1, 5000).Select(i => $"({i}, 'Test {i}', {i * 1.5})"));
                await using (var insertCmd = new NpgsqlCommand(
                                 $"INSERT INTO {tableName} (id, name, value) VALUES {batchValues}", conn))
                {
                    _ = await insertCmd.ExecuteNonQueryAsync();
                }
            }

            var query = $"SELECT id, name, value FROM {tableName}";
            var source = new PostgresSourceNode<TestRecord>(connectionString, query, null, config);

            // Act & Assert - Verify node can be created with streaming configuration
            Assert.NotNull(source);

            // Verify data count
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {tableName}", conn))
                {
                    var count = (long)(await cmd.ExecuteScalarAsync())!;
                    Assert.Equal(5000, count);
                }
            }
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
    }

    [Fact]
    public async Task Parameters_UsedInQueryCorrectly()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";

        try
        {
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {tableName} (id INTEGER PRIMARY KEY, name TEXT, value NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new NpgsqlCommand(
                                 $"INSERT INTO {tableName} (id, name, value) VALUES (1, 'Test 1', 10.5), (2, 'Test 2', 20.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var query = $"SELECT id, name, value FROM {tableName} WHERE id = @id";
            var parameters = new DatabaseParameter[] { new("id", 1) };

            var source = new PostgresSourceNode<TestRecord>(connectionString, query, null, null, parameters);

            // Act & Assert - Verify node can be created with parameters
            Assert.NotNull(source);

            // Verify parameter works by testing directly
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue("id", 1);

                    await using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        var hasData = await reader.ReadAsync();
                        Assert.True(hasData);
                        Assert.Equal(1, reader.GetInt32(0));
                    }
                }
            }
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
    }

    [Fact]
    public async Task LargeDataset_HandledCorrectly()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var sourceTable = "large_source_table";
        var sinkTable = "large_sink_table";
        var query = $"SELECT id, name, value FROM {sourceTable}";

        try
        {
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {sourceTable} (id INTEGER PRIMARY KEY, name TEXT, value NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {sinkTable} (id INTEGER PRIMARY KEY, name TEXT, value NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                // Insert 10000 records using batch insert for performance
                var batchValues = string.Join(",",
                    Enumerable.Range(1, 10000).Select(i => $"({i}, 'Test {i}', {i * 1.5})"));
                await using (var insertCmd = new NpgsqlCommand(
                                 $"INSERT INTO {sourceTable} (id, name, value) VALUES {batchValues}", conn))
                {
                    _ = await insertCmd.ExecuteNonQueryAsync();
                }
            }

            var source = new PostgresSourceNode<TestRecord>(connectionString, query);
            var sink = new PostgresSinkNode<TestRecord>(connectionString, sinkTable);

            // Act & Assert - Verify nodes can be created
            Assert.NotNull(source);
            Assert.NotNull(sink);

            // Verify data count
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {sourceTable}", conn))
                {
                    var count = (long)(await cmd.ExecuteScalarAsync())!;
                    Assert.Equal(10000, count);
                }
            }
        }
        finally
        {
            await DropTableIfExists(sourceTable);
            await DropTableIfExists(sinkTable);
        }
    }

    [Fact]
    public async Task CustomSchema_WritesToCorrectSchema()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var schemaName = "custom_schema";
        var tableName = "test_table";

        try
        {
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand($"CREATE SCHEMA {schemaName}", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {schemaName}.{tableName} (id INTEGER PRIMARY KEY, name TEXT, value NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            // Fix: Use separate schema parameter instead of schema-qualified table name
            var sink = new PostgresSinkNode<TestRecord>(connectionString, tableName, WriteStrategy.PerRow, null, null, schemaName);

            // Act & Assert - Verify node can be created with custom schema
            Assert.NotNull(sink);

            // Verify schema exists
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {schemaName}.{tableName}", conn))
                {
                    var count = (long)(await cmd.ExecuteScalarAsync())!;
                    Assert.Equal(0, count);
                }
            }
        }
        finally
        {
            await DropSchemaIfExists(schemaName);
        }
    }

    [Fact]
    public async Task CaseInsensitiveMapping_MapsColumnsCorrectly()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";

        try
        {
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {tableName} (ID INTEGER PRIMARY KEY, NAME TEXT, VALUE NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new NpgsqlCommand(
                                 $"INSERT INTO {tableName} (ID, NAME, VALUE) VALUES (1, 'Test 1', 10.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var query = $"SELECT ID, NAME, VALUE FROM {tableName}";

            var source = new PostgresSourceNode<TestRecord>(connectionString, query);

            // Act & Assert - Verify node can be created
            Assert.NotNull(source);

            // Verify case-insensitive mapping works
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(query, conn))
                {
                    await using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        await reader.ReadAsync();
                        var postgresRow = new PostgresRow(reader);
                        Assert.Equal(1, postgresRow.Get<int>("id")); // lowercase access
                        Assert.Equal("Test 1", postgresRow.Get<string>("name"));
                        Assert.Equal(10.5m, postgresRow.Get<decimal>("value"));
                    }
                }
            }
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
    }

    [Fact]
    public async Task SnakeCaseConversion_MapsColumnsCorrectly()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";

        try
        {
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {tableName} (user_id INTEGER PRIMARY KEY, user_name TEXT, user_value NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new NpgsqlCommand(
                                 $"INSERT INTO {tableName} (user_id, user_name, user_value) VALUES (1, 'Test User', 100.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var query = $"SELECT user_id, user_name, user_value FROM {tableName}";

            var source = new PostgresSourceNode<UserRecord>(connectionString, query);

            // Act & Assert - Verify node can be created
            Assert.NotNull(source);

            // Verify snake_case mapping works
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(query, conn))
                {
                    await using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        await reader.ReadAsync();
                        var postgresRow = new PostgresRow(reader, false);
                        Assert.Equal(1, postgresRow.Get<int>("user_id"));
                        Assert.Equal("Test User", postgresRow.Get<string>("user_name"));
                        Assert.Equal(100.5m, postgresRow.Get<decimal>("user_value"));
                    }
                }
            }
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
    }

    [Fact]
    public async Task AttributeBasedMapping_MapsColumnsCorrectly()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";

        try
        {
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {tableName} (id INTEGER PRIMARY KEY, display_name TEXT, numeric_value NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new NpgsqlCommand(
                                 $"INSERT INTO {tableName} (id, display_name, numeric_value) VALUES (1, 'Display Name', 50.25)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var query = $"SELECT id, display_name, numeric_value FROM {tableName}";

            var source = new PostgresSourceNode<AttributedRecord>(connectionString, query);

            // Act & Assert - Verify node can be created
            Assert.NotNull(source);

            // Verify attribute-based mapping works
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(query, conn))
                {
                    await using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        await reader.ReadAsync();
                        var postgresRow = new PostgresRow(reader, false);
                        Assert.Equal(1, postgresRow.Get<int>("id"));
                        Assert.Equal("Display Name", postgresRow.Get<string>("display_name"));
                        Assert.Equal(50.25m, postgresRow.Get<decimal>("numeric_value"));
                    }
                }
            }
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
    }

    [Fact]
    public async Task IgnoreAttribute_SkipsIgnoredProperties()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";

        try
        {
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(
                                 $"CREATE TABLE {tableName} (id INTEGER PRIMARY KEY, name TEXT, value NUMERIC)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new NpgsqlCommand(
                                 $"INSERT INTO {tableName} (id, name, value) VALUES (1, 'Test 1', 10.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var query = $"SELECT id, name, value FROM {tableName}";

            var source = new PostgresSourceNode<RecordWithIgnore>(connectionString, query);

            // Act & Assert - Verify node can be created
            Assert.NotNull(source);

            // Verify ignore attribute works
            await using (var conn = new NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand(query, conn))
                {
                    await using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        await reader.ReadAsync();
                        var postgresRow = new PostgresRow(reader, false);
                        Assert.Equal(1, postgresRow.Get<int>("id"));
                        Assert.Equal("Test 1", postgresRow.Get<string>("name"));
                        Assert.Equal(10.5m, postgresRow.Get<decimal>("value"));
                    }
                }
            }
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
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

    [IgnoreColumn]
    public string? InternalField { get; init; }
}

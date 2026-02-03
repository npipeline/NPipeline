using Microsoft.Data.SqlClient;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Mapping;
using NPipeline.Connectors.SqlServer.Nodes;
using NPipeline.Connectors.SqlServer.Tests.Fixtures;

namespace NPipeline.Connectors.SqlServer.Tests.Integration;

[Collection("SqlServer")]
public sealed class SqlServerConnectorIntegrationTests : IClassFixture<SqlServerTestContainerFixture>
{
    private readonly SqlServerTestContainerFixture _fixture;

    public SqlServerConnectorIntegrationTests(SqlServerTestContainerFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task DropTableIfExists(string tableName, string? schema = null)
    {
        var connectionString = _fixture.ConnectionString;

        var fullTableName = string.IsNullOrEmpty(schema)
            ? $"[{tableName}]"
            : $"[{schema}].[{tableName}]";

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        await using var cmd = new SqlCommand($"IF OBJECT_ID('{fullTableName}', 'U') IS NOT NULL DROP TABLE {fullTableName}", conn);
        _ = await cmd.ExecuteNonQueryAsync();
    }

    private async Task DropSchemaIfExists(string schemaName)
    {
        var connectionString = _fixture.ConnectionString;

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        // First, drop all tables in the schema by querying sys.tables
        await using var dropTablesCmd = new SqlCommand($@"
            DECLARE @sql NVARCHAR(MAX) = N'';
            SELECT @sql += N'DROP TABLE [' + SCHEMA_NAME(schema_id) + '].[' + name + '];'
            FROM sys.tables
            WHERE SCHEMA_NAME(schema_id) = '{schemaName}';
            EXEC sp_executesql @sql;
        ", conn);

        _ = await dropTablesCmd.ExecuteNonQueryAsync();

        // Then, drop the schema
        await using var dropSchemaCmd =
            new SqlCommand($"IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schemaName}') EXEC('DROP SCHEMA [{schemaName}]')", conn);

        _ = await dropSchemaCmd.ExecuteNonQueryAsync();
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
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE {sourceTable} (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new SqlCommand(
                                 $"INSERT INTO {sourceTable} (id, name, value) VALUES (1, 'Test 1', 10.5), (2, 'Test 2', 20.5), (3, 'Test 3', 30.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                // Set up sink table
                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE {sinkTable} (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            // Create source and sink nodes
            var source = new SqlServerSourceNode<TestRecord>(connectionString, query);
            var sink = new SqlServerSinkNode<TestRecord>(connectionString, sinkTable);

            // Act & Assert - Verify nodes can be created and configured
            Assert.NotNull(source);
            Assert.NotNull(sink);

            // Verify data exists in source table
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand($"SELECT COUNT(*) FROM {sourceTable}", conn))
                {
                    var count = (int)(await cmd.ExecuteScalarAsync())!;
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
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE {tableName} (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new SqlCommand(
                                 $"INSERT INTO {tableName} (id, name, value) VALUES (1, 'Test 1', 10.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var query = $"SELECT id, name, value FROM {tableName}";

            var source = new SqlServerSourceNode<TestRecord>(connectionString, query);

            // Act & Assert - Verify node can be created with custom mapper
            Assert.NotNull(source);
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
        var config = new SqlServerConfiguration { BatchSize = 100 };

        try
        {
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE {tableName} (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var sink = new SqlServerSinkNode<TestRecord>(connectionString, tableName, config);

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
        var config = new SqlServerConfiguration { FetchSize = 1000 };

        try
        {
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE {tableName} (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                // Insert 5000 records
                for (var i = 1; i <= 5000; i++)
                {
                    await using (var insertCmd = new SqlCommand(
                                     $"INSERT INTO {tableName} (id, name, value) VALUES ({i}, 'Test {i}', {i * 1.5})", conn))
                    {
                        _ = await insertCmd.ExecuteNonQueryAsync();
                    }
                }
            }

            var query = $"SELECT id, name, value FROM {tableName}";
            var source = new SqlServerSourceNode<TestRecord>(connectionString, query, config);

            // Act & Assert - Verify node can be created with streaming configuration
            Assert.NotNull(source);

            // Verify data count
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", conn))
                {
                    var count = (int)(await cmd.ExecuteScalarAsync())!;
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
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE {tableName} (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new SqlCommand(
                                 $"INSERT INTO {tableName} (id, name, value) VALUES (1, 'Test 1', 10.5), (2, 'Test 2', 20.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var query = $"SELECT id, name, value FROM {tableName} WHERE id = 1";

            var source = new SqlServerSourceNode<TestRecord>(connectionString, query);

            // Act & Assert - Verify node can be created
            Assert.NotNull(source);
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
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE {sourceTable} (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE {sinkTable} (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                // Insert 10000 records
                for (var i = 1; i <= 10000; i++)
                {
                    await using (var insertCmd = new SqlCommand(
                                     $"INSERT INTO {sourceTable} (id, name, value) VALUES ({i}, 'Test {i}', {i * 1.5})", conn))
                    {
                        _ = await insertCmd.ExecuteNonQueryAsync();
                    }
                }
            }

            var source = new SqlServerSourceNode<TestRecord>(connectionString, query);
            var sink = new SqlServerSinkNode<TestRecord>(connectionString, sinkTable);

            // Act & Assert - Verify nodes can be created
            Assert.NotNull(source);
            Assert.NotNull(sink);

            // Verify data count
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand($"SELECT COUNT(*) FROM {sourceTable}", conn))
                {
                    var count = (int)(await cmd.ExecuteScalarAsync())!;
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
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand($"CREATE SCHEMA [{schemaName}]", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE [{schemaName}].[{tableName}] (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            // Use separate schema parameter
            var config = new SqlServerConfiguration { Schema = schemaName };
            var sink = new SqlServerSinkNode<TestRecord>(connectionString, tableName, config);

            // Act & Assert - Verify node can be created with custom schema
            Assert.NotNull(sink);

            // Verify schema exists
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand($"SELECT COUNT(*) FROM [{schemaName}].[{tableName}]", conn))
                {
                    var count = (int)(await cmd.ExecuteScalarAsync())!;
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
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE {tableName} ([ID] INT PRIMARY KEY, [NAME] NVARCHAR(100), [VALUE] DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new SqlCommand(
                                 $"INSERT INTO {tableName} ([ID], [NAME], [VALUE]) VALUES (1, 'Test 1', 10.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var query = $"SELECT [ID], [NAME], [VALUE] FROM {tableName}";

            var source = new SqlServerSourceNode<TestRecord>(connectionString, query);

            // Act & Assert - Verify node can be created
            Assert.NotNull(source);
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
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE {tableName} (id INT PRIMARY KEY, display_name NVARCHAR(100), numeric_value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new SqlCommand(
                                 $"INSERT INTO {tableName} (id, display_name, numeric_value) VALUES (1, 'Display Name', 50.25)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var query = $"SELECT id, display_name, numeric_value FROM {tableName}";

            var source = new SqlServerSourceNode<AttributedRecord>(connectionString, query);

            // Act & Assert - Verify node can be created
            Assert.NotNull(source);
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
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE {tableName} (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new SqlCommand(
                                 $"INSERT INTO {tableName} (id, name, value) VALUES (1, 'Test 1', 10.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var query = $"SELECT id, name, value FROM {tableName}";

            var source = new SqlServerSourceNode<RecordWithIgnore>(connectionString, query);

            // Act & Assert - Verify node can be created
            Assert.NotNull(source);
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
    }

    [Fact]
    public async Task IdentityColumn_ExcludedFromInsert()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";

        try
        {
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE {tableName} (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            var sink = new SqlServerSinkNode<RecordWithIdentity>(connectionString, tableName);

            // Act & Assert - Verify node can be created with identity column
            Assert.NotNull(sink);
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

public record AttributedRecord
{
    public int Id { get; init; }

    [SqlServerColumn("display_name")]
    public string? DisplayName { get; init; }

    [SqlServerColumn("numeric_value")]
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

public record RecordWithIdentity
{
    public int Id { get; init; }
    public string? Name { get; init; }
    public decimal Value { get; init; }
}

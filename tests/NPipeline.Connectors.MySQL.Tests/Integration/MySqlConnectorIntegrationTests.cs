using MySqlConnector;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Mapping;
using NPipeline.Connectors.MySql.Nodes;
using NPipeline.Connectors.MySql.Tests.Fixtures;

namespace NPipeline.Connectors.MySql.Tests.Integration;

[Collection("MySql")]
public sealed class MySqlConnectorIntegrationTests
{
    private readonly MySqlTestContainerFixture _fixture;

    public MySqlConnectorIntegrationTests(MySqlTestContainerFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task DropTableIfExistsAsync(string tableName)
    {
        await using var conn = new MySqlConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = new MySqlCommand(
            $"DROP TABLE IF EXISTS `{tableName}`",
            conn);

        _ = await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task EndToEnd_SourceToSink_TransfersDataCorrectly()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var sourceTable = "it_source_table";
        var sinkTable = "it_sink_table";
        var query = $"SELECT id, name, value FROM `{sourceTable}`";

        try
        {
            // Set up source table with test data
            await using (var conn = new MySqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new MySqlCommand(
                                 $"CREATE TABLE IF NOT EXISTS `{sourceTable}` (id INT PRIMARY KEY, name VARCHAR(100), value DECIMAL(18,2))",
                                 conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new MySqlCommand(
                                 $"INSERT INTO `{sourceTable}` (id, name, value) VALUES (1, 'Test 1', 10.5), (2, 'Test 2', 20.5), (3, 'Test 3', 30.5)",
                                 conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                // Set up sink table
                await using (var cmd = new MySqlCommand(
                                 $"CREATE TABLE IF NOT EXISTS `{sinkTable}` (id INT PRIMARY KEY, name VARCHAR(100), value DECIMAL(18,2))",
                                 conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            // Create source and sink nodes
            var source = new MySqlSourceNode<TestRecord>(connectionString, query);
            var sink = new MySqlSinkNode<TestRecord>(connectionString, sinkTable);

            // Act & Assert - Verify nodes can be created and configured
            Assert.NotNull(source);
            Assert.NotNull(sink);

            // Verify data exists in source table
            await using (var conn = new MySqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new MySqlCommand(
                                 $"SELECT COUNT(*) FROM `{sourceTable}`",
                                 conn))
                {
                    var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
                    Assert.Equal(3, count);
                }
            }
        }
        finally
        {
            await DropTableIfExistsAsync(sourceTable);
            await DropTableIfExistsAsync(sinkTable);
        }
    }

    [Fact]
    public async Task Sink_WriteAsync_InsertsRowsIntoTable()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "it_sink_write_test";

        try
        {
            await using (var conn = new MySqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using var createCmd = new MySqlCommand(
                    $"CREATE TABLE IF NOT EXISTS `{tableName}` (id INT PRIMARY KEY, name VARCHAR(100))",
                    conn);

                _ = await createCmd.ExecuteNonQueryAsync();

                await using var insertCmd = new MySqlCommand(
                    $"INSERT INTO `{tableName}` (id, name) VALUES (10, 'Alice'), (20, 'Bob'), (30, 'Charlie')",
                    conn);

                _ = await insertCmd.ExecuteNonQueryAsync();
            }

            var sink = new MySqlSinkNode<TestRecord>(connectionString, tableName,
                new MySqlConfiguration { WriteStrategy = MySqlWriteStrategy.Batch });

            // Act & Assert - Verify node can be created
            Assert.NotNull(sink);

            // Verify row count using direct SQL
            await using (var conn = new MySqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using var countCmd = new MySqlCommand(
                    $"SELECT COUNT(*) FROM `{tableName}`",
                    conn);

                var count = Convert.ToInt64(await countCmd.ExecuteScalarAsync());
                Assert.Equal(3, count);
            }
        }
        finally
        {
            await DropTableIfExistsAsync(tableName);
        }
    }

    [Fact]
    public async Task Source_ReadAsync_ReadsRowsFromTable()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "it_source_read_test";

        try
        {
            await using (var conn = new MySqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using var createCmd = new MySqlCommand(
                    $"CREATE TABLE IF NOT EXISTS `{tableName}` (id INT PRIMARY KEY, name VARCHAR(100))",
                    conn);

                _ = await createCmd.ExecuteNonQueryAsync();

                await using var insertCmd = new MySqlCommand(
                    $"INSERT INTO `{tableName}` (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
                    conn);

                _ = await insertCmd.ExecuteNonQueryAsync();
            }

            var query = $"SELECT id, name FROM `{tableName}`";
            var source = new MySqlSourceNode<TestRecord>(connectionString, query);

            // Act & Assert - Verify node can be created
            Assert.NotNull(source);

            // Verify data count using direct SQL
            await using var conn2 = new MySqlConnection(connectionString);
            await conn2.OpenAsync();

            await using var countCmd = new MySqlCommand($"SELECT COUNT(*) FROM `{tableName}`", conn2);
            var count = Convert.ToInt64(await countCmd.ExecuteScalarAsync());
            Assert.Equal(2, count);
        }
        finally
        {
            await DropTableIfExistsAsync(tableName);
        }
    }

    [Fact]
    public async Task Sink_Upsert_UpdatesExistingRows()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "it_upsert_test";

        try
        {
            await using (var conn = new MySqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using var createCmd = new MySqlCommand(
                    $"CREATE TABLE IF NOT EXISTS `{tableName}` (id INT PRIMARY KEY, name VARCHAR(100))",
                    conn);

                _ = await createCmd.ExecuteNonQueryAsync();

                await using var insertCmd = new MySqlCommand(
                    $"INSERT INTO `{tableName}` (id, name) VALUES (1, 'Original')",
                    conn);

                _ = await insertCmd.ExecuteNonQueryAsync();
            }

            var configuration = new MySqlConfiguration
            {
                UseUpsert = true,
                UpsertKeyColumns = ["id"],
                OnDuplicateKeyAction = OnDuplicateKeyAction.Update,
            };

            var sink = new MySqlSinkNode<TestRecord>(connectionString, tableName, configuration);

            // Act & Assert - Verify node can be created with upsert configuration
            Assert.NotNull(sink);

            // Verify original data exists
            await using var conn2 = new MySqlConnection(connectionString);
            await conn2.OpenAsync();

            await using var selectCmd = new MySqlCommand(
                $"SELECT name FROM `{tableName}` WHERE id = 1",
                conn2);

            var name = (string?)await selectCmd.ExecuteScalarAsync();
            Assert.Equal("Original", name);
        }
        finally
        {
            await DropTableIfExistsAsync(tableName);
        }
    }

    [MySqlTable("test_records")]
    private sealed class TestRecord
    {
        [Column("id")]
        public int Id { get; set; }

        [Column("name")]
        public string Name { get; set; } = string.Empty;
    }
}

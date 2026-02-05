using Npgsql;
using NPipeline.Connectors.PostgreSQL.Nodes;
using NPipeline.Connectors.PostgreSQL.Tests.Fixtures;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.PostgreSQL.Tests.Integration;

[Collection("PostgresTestCollection")]
public class PostgresStorageUriIntegrationTests(PostgresTestContainerFixture fixture) : IClassFixture<PostgresTestContainerFixture>
{
    private readonly PostgresTestContainerFixture _fixture = fixture;

    private async Task DropTableIfExists(string tableName)
    {
        var connectionString = _fixture.ConnectionString;

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand($"DROP TABLE IF EXISTS {tableName} CASCADE", conn);
        _ = await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task EnvironmentSwitching_SamePipelineWorksDifferentUris()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";

        // Parse connection string to extract components for URI construction
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var host = builder.Host;
        var port = builder.Port;
        var database = builder.Database;
        var username = builder.Username;
        var password = builder.Password;
        var encodedPassword = Uri.EscapeDataString(password ?? string.Empty);
        var provider = new PostgresDatabaseStorageProvider();

        try
        {
            // Create test table
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

            // Create two different URIs representing different environments
            // Local environment URI
            var localUri = StorageUri.Parse($"postgres://{username}:{encodedPassword}@{host}:{port}/{database}");

            // Cloud environment URI (simulated with same container but different parameter)
            var cloudUri = StorageUri.Parse($"postgres://{username}:{encodedPassword}@{host}:{port}/{database}?timeout=30");

            // Build connection strings from provider
            var localConnectionString = provider.GetConnectionString(localUri);
            var cloudConnectionString = provider.GetConnectionString(cloudUri);

            // Create source nodes using the same pipeline code but different URIs
            var query = $"SELECT id, name, value FROM {tableName}";
            var localSource = new PostgresSourceNode<TestRecord>(localUri, query);
            var cloudSource = new PostgresSourceNode<TestRecord>(cloudUri, query);

            // Act & Assert - Verify both nodes can be created and configured
            Assert.NotNull(localSource);
            Assert.NotNull(cloudSource);

            // Verify both connection strings work
            await using (var conn = new NpgsqlConnection(localConnectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {tableName}", conn))
                {
                    var count = (long)(await cmd.ExecuteScalarAsync())!;
                    Assert.Equal(1, count);
                }
            }

            await using (var conn = new NpgsqlConnection(cloudConnectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {tableName}", conn))
                {
                    var count = (long)(await cmd.ExecuteScalarAsync())!;
                    Assert.Equal(1, count);
                }
            }

            // Verify URI parameters are reflected in the connection string
            var cloudBuilder = new NpgsqlConnectionStringBuilder(cloudConnectionString);
            Assert.Equal(30, cloudBuilder.Timeout);
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
    }

    [Fact]
    public async Task UriParameters_TranslateToConnectionSettings()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";

        // Parse connection string to extract components for URI construction
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var host = builder.Host;
        var port = builder.Port;
        var database = builder.Database;
        var username = builder.Username;
        var password = builder.Password;
        var encodedPassword = Uri.EscapeDataString(password ?? string.Empty);
        var provider = new PostgresDatabaseStorageProvider();

        try
        {
            // Create test table
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

            // Create URI with various parameters
            var uri = StorageUri.Parse(
                $"postgres://{username}:{encodedPassword}@{host}:{port}/{database}?" +
                "sslmode=disable&" +
                "commandtimeout=60&" +
                "pooling=true&" +
                "minpoolsize=5&" +
                "maxpoolsize=20");

            // Build connection string from provider
            var builtConnectionString = provider.GetConnectionString(uri);
            var connectionBuilder = new NpgsqlConnectionStringBuilder(builtConnectionString);

            // Act & Assert - Verify URI parameters are correctly translated
            Assert.Equal(host, connectionBuilder.Host);
            Assert.Equal(port, connectionBuilder.Port);
            Assert.Equal(database, connectionBuilder.Database);
            Assert.Equal(username, connectionBuilder.Username);
            Assert.Equal(password, connectionBuilder.Password);
            Assert.Equal(SslMode.Disable, connectionBuilder.SslMode);
            Assert.Equal(60, connectionBuilder.CommandTimeout);
            Assert.True(connectionBuilder.Pooling);
            Assert.Equal(5, connectionBuilder.MinPoolSize);
            Assert.Equal(20, connectionBuilder.MaxPoolSize);

            // Verify connection string works
            await using (var conn = new NpgsqlConnection(builtConnectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {tableName}", conn))
                {
                    var count = (long)(await cmd.ExecuteScalarAsync())!;
                    Assert.Equal(1, count);
                }
            }
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
    }

    [Fact]
    public async Task SslModeConfiguration_WorksCorrectly()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";

        // Parse connection string to extract components for URI construction
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var host = builder.Host;
        var port = builder.Port;
        var database = builder.Database;
        var username = builder.Username;
        var password = builder.Password;
        var encodedPassword = Uri.EscapeDataString(password ?? string.Empty);
        var provider = new PostgresDatabaseStorageProvider();

        try
        {
            // Create test table
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

            // Test different SSL modes
            var sslModes = new[] { "disable", "allow", "prefer", "require", "verifyca", "verifyfull" };

            foreach (var sslMode in sslModes)
            {
                // Create URI with SSL mode parameter
                var uri = StorageUri.Parse(
                    $"postgres://{username}:{encodedPassword}@{host}:{port}/{database}?sslmode={sslMode}");

                // Build connection string from provider
                var builtConnectionString = provider.GetConnectionString(uri);
                var connectionBuilder = new NpgsqlConnectionStringBuilder(builtConnectionString);

                // Verify SSL mode is correctly translated
                Assert.Equal(MapSslMode(sslMode), connectionBuilder.SslMode);

                // Create source node with SSL configuration
                var query = $"SELECT id, name, value FROM {tableName}";
                var source = new PostgresSourceNode<TestRecord>(uri, query);

                Assert.NotNull(source);
            }

            // Verify the connection string with SSL mode works
            var sslUri = StorageUri.Parse(
                $"postgres://{username}:{encodedPassword}@{host}:{port}/{database}?sslmode=disable");

            var sslConnectionString = provider.GetConnectionString(sslUri);

            await using (var conn = new NpgsqlConnection(sslConnectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {tableName}", conn))
                {
                    var count = (long)(await cmd.ExecuteScalarAsync())!;
                    Assert.Equal(1, count);
                }
            }
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
    }

    private static SslMode MapSslMode(string mode)
    {
        return mode.ToLowerInvariant() switch
        {
            "disable" => SslMode.Disable,
            "allow" => SslMode.Allow,
            "prefer" => SslMode.Prefer,
            "require" => SslMode.Require,
            "verifyca" => SslMode.VerifyCA,
            "verifyfull" => SslMode.VerifyFull,
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown SSL mode."),
        };
    }

    private sealed class TestRecord
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Value { get; set; }
    }
}

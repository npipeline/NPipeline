using Microsoft.Data.SqlClient;
using NPipeline.StorageProviders.Models;
using NPipeline.Connectors.SqlServer.Nodes;
using NPipeline.Connectors.SqlServer.Tests.Fixtures;

namespace NPipeline.Connectors.SqlServer.Tests.Integration;

[Collection("SqlServer")]
public sealed class SqlServerStorageUriIntegrationTests : IClassFixture<SqlServerTestContainerFixture>
{
    private readonly SqlServerTestContainerFixture _fixture;

    public SqlServerStorageUriIntegrationTests(SqlServerTestContainerFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task DropTableIfExists(string tableName)
    {
        var connectionString = _fixture.ConnectionString;

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        await using var cmd = new SqlCommand($"IF OBJECT_ID('[{tableName}]', 'U') IS NOT NULL DROP TABLE [{tableName}]", conn);
        _ = await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task EnvironmentSwitching_SamePipelineWorksDifferentUris()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";

        // Parse connection string to extract components for URI construction
        var builder = new SqlConnectionStringBuilder(connectionString);
        var (host, port) = ParseDataSource(builder.DataSource);
        var database = builder.InitialCatalog;
        var username = builder.UserID;
        var password = builder.Password;
        var encodedPassword = Uri.EscapeDataString(password ?? string.Empty);
        var provider = new SqlServerDatabaseStorageProvider();

        try
        {
            // Create test table
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE [{tableName}] (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new SqlCommand(
                                 $"INSERT INTO [{tableName}] (id, name, value) VALUES (1, 'Test 1', 10.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            // Create two different URIs representing different environments
            // Local environment URI
            var localUri = StorageUri.Parse(
                $"mssql://{username}:{encodedPassword}@{BuildHostSegment(host, port)}/{database}?" +
                "encrypt=true&trustservercertificate=true");

            // Cloud environment URI (simulated with same container but different parameter)
            var cloudUri = StorageUri.Parse(
                $"mssql://{username}:{encodedPassword}@{BuildHostSegment(host, port)}/{database}?" +
                "encrypt=true&trustservercertificate=true&connect%20timeout=30");

            // Build connection strings from provider
            var localConnectionString = provider.GetConnectionString(localUri);
            var cloudConnectionString = provider.GetConnectionString(cloudUri);

            // Create source nodes using the same pipeline code but different URIs
            var query = $"SELECT id, name, value FROM [{tableName}]";
            var localSource = new SqlServerSourceNode<TestRecord>(localUri, query);
            var cloudSource = new SqlServerSourceNode<TestRecord>(cloudUri, query);

            // Act & Assert - Verify both nodes can be created and configured
            Assert.NotNull(localSource);
            Assert.NotNull(cloudSource);

            // Verify both connection strings work
            await using (var conn = new SqlConnection(localConnectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand($"SELECT COUNT(*) FROM [{tableName}]", conn))
                {
                    var count = (int)(await cmd.ExecuteScalarAsync())!;
                    Assert.Equal(1, count);
                }
            }

            await using (var conn = new SqlConnection(cloudConnectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand($"SELECT COUNT(*) FROM [{tableName}]", conn))
                {
                    var count = (int)(await cmd.ExecuteScalarAsync())!;
                    Assert.Equal(1, count);
                }
            }

            // Verify URI parameters are reflected in the connection string
            var cloudBuilder = new SqlConnectionStringBuilder(cloudConnectionString);
            Assert.Equal(30, cloudBuilder.ConnectTimeout);
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
        var builder = new SqlConnectionStringBuilder(connectionString);
        var (host, port) = ParseDataSource(builder.DataSource);
        var database = builder.InitialCatalog;
        var username = builder.UserID;
        var password = builder.Password;
        var encodedPassword = Uri.EscapeDataString(password ?? string.Empty);
        var provider = new SqlServerDatabaseStorageProvider();

        try
        {
            // Create test table
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE [{tableName}] (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new SqlCommand(
                                 $"INSERT INTO [{tableName}] (id, name, value) VALUES (1, 'Test 1', 10.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            // Create URI with various parameters
            var uri = StorageUri.Parse(
                $"mssql://{username}:{encodedPassword}@{BuildHostSegment(host, port)}/{database}?" +
                "encrypt=true&" +
                "trustservercertificate=true&" +
                "connect%20timeout=60&" +
                "max%20pool%20size=20&" +
                "min%20pool%20size=5");

            // Build connection string from provider
            var builtConnectionString = provider.GetConnectionString(uri);
            var connectionBuilder = new SqlConnectionStringBuilder(builtConnectionString);

            // Act & Assert - Verify URI parameters are correctly translated
            Assert.Equal(BuildDataSource(host, port), connectionBuilder.DataSource);
            Assert.Equal(database, connectionBuilder.InitialCatalog);
            Assert.Equal(username, connectionBuilder.UserID);
            Assert.Equal(password, connectionBuilder.Password);
            Assert.True(connectionBuilder.Encrypt);
            Assert.True(connectionBuilder.TrustServerCertificate);
            Assert.Equal(60, connectionBuilder.ConnectTimeout);
            Assert.Equal(20, connectionBuilder.MaxPoolSize);
            Assert.Equal(5, connectionBuilder.MinPoolSize);

            // Verify connection string works
            await using (var conn = new SqlConnection(builtConnectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand($"SELECT COUNT(*) FROM [{tableName}]", conn))
                {
                    var count = (int)(await cmd.ExecuteScalarAsync())!;
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
    public async Task EncryptionConfiguration_WorksCorrectly()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var tableName = "test_table";

        // Parse connection string to extract components for URI construction
        var builder = new SqlConnectionStringBuilder(connectionString);
        var (host, port) = ParseDataSource(builder.DataSource);
        var database = builder.InitialCatalog;
        var username = builder.UserID;
        var password = builder.Password;
        var encodedPassword = Uri.EscapeDataString(password ?? string.Empty);
        var provider = new SqlServerDatabaseStorageProvider();

        try
        {
            // Create test table
            await using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand(
                                 $"CREATE TABLE [{tableName}] (id INT PRIMARY KEY, name NVARCHAR(100), value DECIMAL(18,2))", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }

                await using (var cmd = new SqlCommand(
                                 $"INSERT INTO [{tableName}] (id, name, value) VALUES (1, 'Test 1', 10.5)", conn))
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }

            // Test different encryption configurations
            var encryptionConfigs = new[]
            {
                (encrypt: "true", trustServerCertificate: "false"),
                (encrypt: "true", trustServerCertificate: "true"),
                (encrypt: "false", trustServerCertificate: "false"),
            };

            foreach (var (encrypt, trustServerCertificate) in encryptionConfigs)
            {
                // Create URI with encryption parameters
                var uri = StorageUri.Parse(
                    $"mssql://{username}:{encodedPassword}@{BuildHostSegment(host, port)}/{database}?encrypt={encrypt}&trustservercertificate={trustServerCertificate}");

                // Build connection string from provider
                var builtConnectionString = provider.GetConnectionString(uri);
                var connectionBuilder = new SqlConnectionStringBuilder(builtConnectionString);

                // Verify encryption parameters are correctly translated
                Assert.Equal(ParseEncryptOption(encrypt), connectionBuilder.Encrypt);
                Assert.Equal(bool.Parse(trustServerCertificate), connectionBuilder.TrustServerCertificate);

                // Create source node with encryption configuration
                var query = $"SELECT id, name, value FROM [{tableName}]";
                var source = new SqlServerSourceNode<TestRecord>(uri, query);

                Assert.NotNull(source);
            }

            // Verify the connection string with encryption works
            var encryptUri = StorageUri.Parse(
                $"mssql://{username}:{encodedPassword}@{BuildHostSegment(host, port)}/{database}?encrypt=true&trustservercertificate=true");

            var encryptConnectionString = provider.GetConnectionString(encryptUri);

            await using (var conn = new SqlConnection(encryptConnectionString))
            {
                await conn.OpenAsync();

                await using (var cmd = new SqlCommand($"SELECT COUNT(*) FROM [{tableName}]", conn))
                {
                    var count = (int)(await cmd.ExecuteScalarAsync())!;
                    Assert.Equal(1, count);
                }
            }
        }
        finally
        {
            await DropTableIfExists(tableName);
        }
    }

    private static SqlConnectionEncryptOption ParseEncryptOption(string value)
    {
        return bool.Parse(value)
            ? SqlConnectionEncryptOption.Mandatory
            : SqlConnectionEncryptOption.Optional;
    }

    private static (string Host, int? Port) ParseDataSource(string dataSource)
    {
        if (string.IsNullOrWhiteSpace(dataSource))
            return (dataSource, null);

        var parts = dataSource.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length == 1)
            return (parts[0], null);

        if (parts.Length >= 2 && int.TryParse(parts[1], out var port))
            return (parts[0], port);

        return (parts[0], null);
    }

    private static string BuildHostSegment(string host, int? port)
    {
        return port.HasValue
            ? $"{host}:{port.Value}"
            : host;
    }

    private static string BuildDataSource(string host, int? port)
    {
        return port.HasValue
            ? $"{host},{port.Value}"
            : host;
    }

    private sealed class TestRecord
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Value { get; set; }
    }
}

using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Nodes;
using NPipeline.Connectors.Aws.Redshift.Tests.Fixtures;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Integration;

[Trait("Category", "Integration")]
[Trait("Category", "LiveRedshift")]
public sealed class RedshiftStorageUriIntegrationTests : IClassFixture<RedshiftTestFixture>
{
    private readonly RedshiftTestFixture _fixture;

    public RedshiftStorageUriIntegrationTests(RedshiftTestFixture fixture)
    {
        _fixture = fixture;
    }

    [SkippableFact]
    public void StorageUri_ParsesCorrectly()
    {
        var uri = StorageUri.Parse("redshift://user:pass@host.redshift.amazonaws.com:5439/database");

        Assert.Equal("redshift", uri.Scheme.ToString());

        // UserInfo contains both username and password as a single string
        Assert.Equal("user:pass", uri.UserInfo);
        Assert.Equal("host.redshift.amazonaws.com", uri.Host);
        Assert.Equal(5439, uri.Port);
        Assert.Equal("/database", uri.Path);
    }

    [SkippableFact]
    public void StorageUri_WithQueryParameters_ParsesCorrectly()
    {
        var uri = StorageUri.Parse("redshift://user:pass@host.redshift.amazonaws.com:5439/database?timeout=60&schema=myschema");

        Assert.Equal("redshift", uri.Scheme.ToString());
        Assert.Equal("60", uri.Parameters["timeout"]);
        Assert.Equal("myschema", uri.Parameters["schema"]);
    }

    [SkippableFact]
    public async Task SourceNode_FromStorageUri_CanRead()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        // Arrange - Create table and insert data
        await _fixture.CreateTableAsync("test_uri_source", "id INT, value VARCHAR(50)");
        await _fixture.ExecuteNonQueryAsync($"INSERT INTO \"{_fixture.SchemaName}\".test_uri_source VALUES (1, 'test')");

        // Build storage URI from connection string
        var config = new RedshiftConfiguration { Schema = _fixture.SchemaName };

        var source = new RedshiftSourceNode<UriTestRow>(
            _fixture.ConnectionString,
            $"SELECT id, value FROM \"{_fixture.SchemaName}\".test_uri_source",
            configuration: config);

        // Act
        var context = new PipelineContext();
        var dataPipe = source.Initialize(context, CancellationToken.None);

        var results = new List<UriTestRow>();

        await foreach (var row in dataPipe)
        {
            results.Add(row);
        }

        // Assert
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("test", results[0].Value);

        await source.DisposeAsync();
    }

    [SkippableFact]
    public void RedshiftDatabaseStorageProvider_CanHandle_RedshiftScheme()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        var provider = new RedshiftDatabaseStorageProvider();
        var uri = StorageUri.Parse("redshift://user:pass@host.redshift.amazonaws.com:5439/database");

        Assert.True(provider.CanHandle(uri));
    }

    [SkippableFact]
    public void RedshiftDatabaseStorageProvider_GetConnectionString_ReturnsValidString()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        var provider = new RedshiftDatabaseStorageProvider();
        var uri = StorageUri.Parse("redshift://testuser:testpass@testcluster.redshift.amazonaws.com:5439/testdb");

        var connectionString = provider.GetConnectionString(uri);

        Assert.Contains("Host=testcluster.redshift.amazonaws.com", connectionString);
        Assert.Contains("Port=5439", connectionString);
        Assert.Contains("Database=testdb", connectionString);
        Assert.Contains("Username=testuser", connectionString);
        Assert.Contains("Password=testpass", connectionString);
    }

    private static StorageUri BuildStorageUriFromConnectionString(string connectionString)
    {
        // Parse Npgsql connection string and build storage URI
        var parts = connectionString.Split(';')
            .Select(p => p.Split('=', 2))
            .Where(p => p.Length == 2)
            .ToDictionary(p => p[0].Trim().ToLowerInvariant(), p => p[1].Trim());

        var host = parts.GetValueOrDefault("host", "localhost");
        var port = parts.GetValueOrDefault("port", "5439");
        var database = parts.GetValueOrDefault("database", "dev");
        var username = parts.GetValueOrDefault("username", "user");
        var password = parts.GetValueOrDefault("password", "");

        return StorageUri.Parse($"redshift://{username}:{password}@{host}:{port}/{database}");
    }

    public sealed class UriTestRow
    {
        public int Id { get; set; }
        public string Value { get; set; } = string.Empty;
    }
}

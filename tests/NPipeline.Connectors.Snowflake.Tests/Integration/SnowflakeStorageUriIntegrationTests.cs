using System.Data;
using NPipeline.Connectors.Snowflake.Tests.Fixtures;
using NPipeline.Connectors.Snowflake.Tests.Helpers;
using NPipeline.StorageProviders.Models;
using Snowflake.Data.Client;

namespace NPipeline.Connectors.Snowflake.Tests.Integration;

[Collection("Snowflake")]
public sealed class SnowflakeStorageUriIntegrationTests(SnowflakeTestFixture fixture)
{
    private readonly SnowflakeTestFixture _fixture = fixture;

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "LiveSnowflake")]
    public async Task StorageUriBuiltConnectionString_CanOpenConnection()
    {
        if (!SnowflakeTestHelpers.HasConnectionString(_fixture))
            return;

        var values = ParseConnectionString(_fixture.ConnectionString);

        if (!values.TryGetValue("account", out var account) || string.IsNullOrWhiteSpace(account))
            return;

        if (!values.TryGetValue("db", out var database) || string.IsNullOrWhiteSpace(database))
            return;

        values.TryGetValue("user", out var user);
        values.TryGetValue("password", out var password);

        var queryParts = new List<string>();

        if (!string.IsNullOrWhiteSpace(user))
            queryParts.Add($"user={Uri.EscapeDataString(user)}");

        if (!string.IsNullOrWhiteSpace(password))
            queryParts.Add($"password={Uri.EscapeDataString(password)}");

        var uri = StorageUri.Parse($"snowflake://{account}/{database}?{string.Join("&", queryParts)}");
        var provider = new SnowflakeDatabaseStorageProvider();
        var builtConnectionString = provider.GetConnectionString(uri);

        using var connection = new SnowflakeDbConnection(builtConnectionString);
        await connection.OpenAsync();

        Assert.Equal(ConnectionState.Open, connection.State);
    }

    private static Dictionary<string, string> ParseConnectionString(string connectionString)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var pairs = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);

        foreach (var pair in pairs)
        {
            var keyValue = pair.Split('=', 2);

            if (keyValue.Length == 2)
                result[keyValue[0].Trim()] = keyValue[1].Trim();
        }

        return result;
    }
}

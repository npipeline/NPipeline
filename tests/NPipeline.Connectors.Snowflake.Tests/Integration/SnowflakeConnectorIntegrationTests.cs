using Snowflake.Data.Client;
using NPipeline.Connectors.Snowflake.Tests.Fixtures;
using NPipeline.Connectors.Snowflake.Tests.Helpers;

namespace NPipeline.Connectors.Snowflake.Tests.Integration;

[Collection("Snowflake")]
public sealed class SnowflakeConnectorIntegrationTests(SnowflakeTestFixture fixture)
{
    private readonly SnowflakeTestFixture _fixture = fixture;

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "LiveSnowflake")]
    public async Task ConnectionString_WhenConfigured_CanOpenConnection()
    {
        if (!SnowflakeTestHelpers.HasConnectionString(_fixture))
            return;

        using var connection = new SnowflakeDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        Assert.Equal(System.Data.ConnectionState.Open, connection.State);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Category", "LiveSnowflake")]
    public async Task CanExecuteSimpleSelect()
    {
        if (!SnowflakeTestHelpers.HasConnectionString(_fixture))
            return;

        using var connection = new SnowflakeDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";

        var result = await command.ExecuteScalarAsync();
        Assert.NotNull(result);
    }
}

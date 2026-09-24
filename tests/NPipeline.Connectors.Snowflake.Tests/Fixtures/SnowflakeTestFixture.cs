using Snowflake.Data.Client;

namespace NPipeline.Connectors.Snowflake.Tests.Fixtures;

public sealed class SnowflakeTestFixture : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        ConnectionString = Environment.GetEnvironmentVariable("NPIPELINE_SNOWFLAKE_CONNECTION_STRING") ?? string.Empty;

        if (string.IsNullOrWhiteSpace(ConnectionString))
            return;

        using var connection = new SnowflakeDbConnection(ConnectionString);
        await connection.OpenAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

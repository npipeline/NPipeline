using NPipeline.Connectors.Snowflake.Tests.Fixtures;

namespace NPipeline.Connectors.Snowflake.Tests.Helpers;

internal static class SnowflakeTestHelpers
{
    public static bool HasConnectionString(SnowflakeTestFixture fixture) => !string.IsNullOrWhiteSpace(fixture.ConnectionString);
}

using NPipeline.Connectors.Snowflake.Tests.Fixtures;

namespace NPipeline.Connectors.Snowflake.Tests.Helpers;

internal static class SnowflakeTestHelpers
{
    public static bool HasConnectionString(SnowflakeTestFixture fixture)
    {
        return !string.IsNullOrWhiteSpace(fixture.ConnectionString);
    }
}

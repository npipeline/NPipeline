using AwesomeAssertions;
using NPipeline.Connectors.Snowflake.Configuration;

namespace NPipeline.Connectors.Snowflake.Tests;

public sealed class SnowflakeStorageResolverFactoryTests
{
    [Fact]
    public void CreateResolver_ShouldReturnNonNullResolver()
    {
        // Act
        var resolver = SnowflakeStorageResolverFactory.CreateResolver();

        // Assert
        resolver.Should().NotBeNull();
    }
}

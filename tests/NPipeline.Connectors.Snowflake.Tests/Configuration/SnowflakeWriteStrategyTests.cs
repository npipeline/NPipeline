using NPipeline.Connectors.Snowflake.Configuration;

namespace NPipeline.Connectors.Snowflake.Tests.Configuration;

public sealed class SnowflakeWriteStrategyTests
{
    [Fact]
    public void WriteStrategy_PerRow_ShouldHaveCorrectValue()
    {
        // Arrange & Act
        var strategy = SnowflakeWriteStrategy.PerRow;

        // Assert
        Assert.Equal(0, (int)strategy);
    }

    [Fact]
    public void WriteStrategy_Batch_ShouldHaveCorrectValue()
    {
        // Arrange & Act
        var strategy = SnowflakeWriteStrategy.Batch;

        // Assert
        Assert.Equal(1, (int)strategy);
    }

    [Fact]
    public void WriteStrategy_StagedCopy_ShouldHaveCorrectValue()
    {
        // Arrange & Act
        var strategy = SnowflakeWriteStrategy.StagedCopy;

        // Assert
        Assert.Equal(2, (int)strategy);
    }

    [Fact]
    public void WriteStrategy_ShouldHaveThreeValues()
    {
        // Arrange & Act
        var values = Enum.GetValues<SnowflakeWriteStrategy>();

        // Assert
        Assert.Equal(3, values.Length);
    }
}

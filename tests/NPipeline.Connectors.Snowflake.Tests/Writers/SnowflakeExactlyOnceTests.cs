using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Snowflake.Configuration;

namespace NPipeline.Connectors.Snowflake.Tests.Writers;

public sealed class SnowflakeExactlyOnceTests
{
    [Fact]
    public void ExactlyOnce_WithBatchStrategy_ShouldValidate()
    {
        // Arrange
        var configuration = new SnowflakeConfiguration
        {
            WriteStrategy = SnowflakeWriteStrategy.Batch,
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
        };

        // Act
        var exception = Record.Exception(() => configuration.Validate());

        // Assert
        Assert.Null(exception);
    }

    [Fact]
    public void ExactlyOnce_WithStagedCopyStrategy_ShouldThrow()
    {
        // Arrange
        var configuration = new SnowflakeConfiguration
        {
            WriteStrategy = SnowflakeWriteStrategy.StagedCopy,
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => configuration.Validate());
    }
}

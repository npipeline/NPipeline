using AwesomeAssertions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Snowflake.Configuration;

namespace NPipeline.Connectors.Snowflake.Tests.Configuration;

public sealed class SnowflakeConfigurationTests
{
    [Fact]
    public void DefaultConfiguration_ShouldHaveValidDefaults()
    {
        // Arrange & Act
        var config = new SnowflakeConfiguration();

        // Assert
        config.ConnectionString.Should().BeEmpty();
        config.Schema.Should().Be("PUBLIC");
        config.CommandTimeout.Should().Be(300);
        config.ConnectionTimeout.Should().Be(30);
        config.MinPoolSize.Should().Be(1);
        config.MaxPoolSize.Should().Be(10);
        config.WriteStrategy.Should().Be(SnowflakeWriteStrategy.Batch);
        config.BatchSize.Should().Be(1000);
        config.MaxBatchSize.Should().Be(16384);
        config.UseTransaction.Should().BeTrue();
        config.UseUpsert.Should().BeFalse();
        config.ContinueOnError.Should().BeFalse();
        config.ValidateIdentifiers.Should().BeTrue();
        config.MaxRetryAttempts.Should().Be(3);
        config.RetryDelay.Should().Be(TimeSpan.FromSeconds(2));
        config.CaseInsensitiveMapping.Should().BeTrue();
        config.CacheMappingMetadata.Should().BeTrue();
        config.StreamResults.Should().BeTrue();
        config.FetchSize.Should().Be(10000);
        config.ThrowOnMappingError.Should().BeTrue();
        config.DeliverySemantic.Should().Be(DeliverySemantic.AtLeastOnce);
        config.CheckpointStrategy.Should().Be(CheckpointStrategy.None);
        config.StageName.Should().Be("~");
        config.FileFormat.Should().Be("CSV");
        config.CopyCompression.Should().Be("GZIP");
        config.StageFilePrefix.Should().Be("npipeline_");
        config.PurgeAfterCopy.Should().BeTrue();
        config.OnErrorAction.Should().Be("ABORT_STATEMENT");
        config.QueryTag.Should().Be("NPipeline");
        config.CheckpointTableName.Should().Be("PIPELINE_CHECKPOINTS");
    }

    [Fact]
    public void Validate_WithValidConfiguration_ShouldNotThrow()
    {
        // Arrange
        var config = new SnowflakeConfiguration();

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithEmptySchema_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { Schema = "" };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNullSchema_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { Schema = null! };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroCommandTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { CommandTimeout = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeCommandTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { CommandTimeout = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroConnectionTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { ConnectionTimeout = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeConnectionTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { ConnectionTimeout = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeMinPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { MinPoolSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroMaxPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { MaxPoolSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithMinPoolSizeGreaterThanMaxPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { MinPoolSize = 100, MaxPoolSize = 50 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { BatchSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { BatchSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithBatchSizeGreaterThanMaxBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { BatchSize = 20000, MaxBatchSize = 16384 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeMaxRetryAttempts_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { MaxRetryAttempts = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeRetryDelay_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { RetryDelay = TimeSpan.FromSeconds(-1) };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroFetchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { FetchSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithUseUpsertEnabledButNoKeyColumns_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { UseUpsert = true };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithUseUpsertEnabledButEmptyKeyColumns_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SnowflakeConfiguration { UseUpsert = true, UpsertKeyColumns = [] };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithUseUpsertEnabled_ShouldNotThrow()
    {
        // Arrange
        var config = new SnowflakeConfiguration { UseUpsert = true, UpsertKeyColumns = ["Id"] };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void OnMergeAction_ShouldHaveDefaultValue()
    {
        // Arrange & Act
        var config = new SnowflakeConfiguration();

        // Assert
        config.OnMergeAction.Should().Be(OnMergeAction.Update);
    }

    [Fact]
    public void RowErrorHandler_ShouldBeNullByDefault()
    {
        // Arrange & Act
        var config = new SnowflakeConfiguration();

        // Assert
        config.RowErrorHandler.Should().BeNull();
    }

    [Fact]
    public void ValidateConnectionSettings_WithValidSettings_ShouldNotThrow()
    {
        // Arrange
        var config = new SnowflakeConfiguration();

        // Act & Assert
        var exception = Record.Exception(() => config.ValidateConnectionSettings());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithExactlyOnceDeliverySemantic_ShouldNotThrow()
    {
        // Arrange
        var config = new SnowflakeConfiguration { DeliverySemantic = DeliverySemantic.ExactlyOnce };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithOffsetCheckpointStrategy_ShouldNotThrow()
    {
        // Arrange
        var config = new SnowflakeConfiguration
        {
            CheckpointStrategy = CheckpointStrategy.Offset,
            CheckpointFilePath = "/tmp/checkpoints.json",
            CheckpointOffsetColumn = "offset",
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithStagedCopyWriteStrategy_ShouldNotThrow()
    {
        // Arrange
        var config = new SnowflakeConfiguration { WriteStrategy = SnowflakeWriteStrategy.StagedCopy };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }
}

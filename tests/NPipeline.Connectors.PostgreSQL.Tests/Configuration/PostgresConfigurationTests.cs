using AwesomeAssertions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.PostgreSQL.Configuration;

namespace NPipeline.Connectors.PostgreSQL.Tests.Configuration;

public sealed class PostgresConfigurationTests
{
    [Fact]
    public void DefaultConfiguration_ShouldHaveValidDefaults()
    {
        // Arrange & Act
        var config = new PostgresConfiguration();

        // Assert
        config.ConnectionString.Should().BeEmpty();
        config.Schema.Should().Be("public");
        config.CommandTimeout.Should().Be(30);
        config.CopyTimeout.Should().Be(300);
        config.ConnectionTimeout.Should().Be(15);
        config.MinPoolSize.Should().Be(1);
        config.MaxPoolSize.Should().Be(100);
        config.UseSslMode.Should().BeFalse();
        config.ReadBufferSize.Should().Be(8192);
        config.WriteStrategy.Should().Be(PostgresWriteStrategy.Batch);
        config.BatchSize.Should().Be(100);
        config.MaxBatchSize.Should().Be(1000);
        config.UseTransaction.Should().BeTrue();
        config.UseUpsert.Should().BeFalse();
        config.ContinueOnError.Should().BeFalse();
        config.ValidateIdentifiers.Should().BeTrue();
        config.MaxRetryAttempts.Should().Be(3);
        config.RetryDelay.Should().Be(TimeSpan.FromSeconds(1));
        config.CaseInsensitiveMapping.Should().BeTrue();
        config.CacheMappingMetadata.Should().BeTrue();
        config.UseBinaryCopy.Should().BeFalse();
        config.StreamResults.Should().BeTrue();
        config.FetchSize.Should().Be(1000);
        config.ThrowOnMappingError.Should().BeTrue();
        config.UsePreparedStatements.Should().BeTrue();
    }

    [Fact]
    public void Validate_WithValidConfiguration_ShouldNotThrow()
    {
        // Arrange
        var config = new PostgresConfiguration();

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithEmptySchema_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { Schema = "" };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithWhitespaceSchema_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { Schema = "   " };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNullSchema_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { Schema = null! };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroCommandTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { CommandTimeout = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeCommandTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { CommandTimeout = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroCopyTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { CopyTimeout = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeCopyTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { CopyTimeout = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroConnectionTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { ConnectionTimeout = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeConnectionTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { ConnectionTimeout = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeMinPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { MinPoolSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroMaxPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { MaxPoolSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeMaxPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { MaxPoolSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithMinPoolSizeGreaterThanMaxPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration
        {
            MinPoolSize = 100,
            MaxPoolSize = 50,
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroReadBufferSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { ReadBufferSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeReadBufferSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { ReadBufferSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { BatchSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { BatchSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroMaxBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { MaxBatchSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeMaxBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { MaxBatchSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithBatchSizeGreaterThanMaxBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration
        {
            BatchSize = 2000,
            MaxBatchSize = 1000,
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeMaxRetryAttempts_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { MaxRetryAttempts = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeRetryDelay_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new PostgresConfiguration { RetryDelay = TimeSpan.FromSeconds(-1) };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithUseUpsertEnabled_ShouldThrowNotSupportedException()
    {
        // Arrange
        var config = new PostgresConfiguration
        {
            UseUpsert = true,
        };

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithUseBinaryCopyEnabled_ShouldThrowNotSupportedException()
    {
        // Arrange
        var config = new PostgresConfiguration
        {
            UseBinaryCopy = true,
        };

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithCopyWriteStrategy_ShouldThrowNotSupportedException()
    {
        // Arrange
        var config = new PostgresConfiguration
        {
            WriteStrategy = PostgresWriteStrategy.Copy,
        };

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithExactlyOnceDeliverySemantic_ShouldThrowNotSupportedException()
    {
        // Arrange
        var config = new PostgresConfiguration
        {
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
        };

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithCheckpointStrategyEnabled_ShouldThrowNotSupportedException()
    {
        // Arrange
        var config = new PostgresConfiguration
        {
            CheckpointStrategy = CheckpointStrategy.Offset,
        };

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithInMemoryCheckpointStrategy_ShouldNotThrow()
    {
        // Arrange
        var config = new PostgresConfiguration
        {
            CheckpointStrategy = CheckpointStrategy.InMemory,
        };

        // Act
        var action = () => config.Validate();

        // Assert
        action.Should().NotThrow();
    }

    [Fact]
    public void OnConflictAction_ShouldHaveDefaultValue()
    {
        // Arrange & Act
        var config = new PostgresConfiguration();

        // Assert
        config.OnConflictAction.Should().Be(OnConflictAction.Update);
    }

    [Fact]
    public void RowErrorHandler_ShouldBeNullByDefault()
    {
        // Arrange & Act
        var config = new PostgresConfiguration();

        // Assert
        config.RowErrorHandler.Should().BeNull();
    }

    [Fact]
    public void SslMode_ShouldBeNullByDefault()
    {
        // Arrange & Act
        var config = new PostgresConfiguration();

        // Assert
        config.SslMode.Should().BeNull();
    }
}

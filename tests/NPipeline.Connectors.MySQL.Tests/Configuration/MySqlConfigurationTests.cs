using AwesomeAssertions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.MySql.Configuration;

namespace NPipeline.Connectors.MySql.Tests.Configuration;

public sealed class MySqlConfigurationTests
{
    [Fact]
    public void DefaultConfiguration_ShouldHaveValidDefaults()
    {
        // Arrange & Act
        var config = new MySqlConfiguration();

        // Assert
        config.ConnectionString.Should().BeEmpty();
        config.CommandTimeout.Should().Be(30);
        config.ConnectionTimeout.Should().Be(15);
        config.CharacterSet.Should().Be("utf8mb4");
        config.AllowUserVariables.Should().BeFalse();
        config.ConvertZeroDateTime.Should().BeTrue();
        config.MinPoolSize.Should().Be(1);
        config.MaxPoolSize.Should().Be(100);
        config.WriteStrategy.Should().Be(MySqlWriteStrategy.Batch);
        config.BatchSize.Should().Be(100);
        config.MaxBatchSize.Should().Be(1000);
        config.UseTransaction.Should().BeTrue();
        config.UsePreparedStatements.Should().BeTrue();
        config.UseUpsert.Should().BeFalse();
        config.ContinueOnError.Should().BeFalse();
        config.ValidateIdentifiers.Should().BeTrue();
        config.MaxRetryAttempts.Should().Be(3);
        config.RetryDelay.Should().Be(TimeSpan.FromSeconds(2));
        config.CaseInsensitiveMapping.Should().BeTrue();
        config.CacheMappingMetadata.Should().BeTrue();
        config.StreamResults.Should().BeTrue();
        config.ThrowOnMappingError.Should().BeTrue();
        config.BulkLoadBatchSize.Should().Be(5000);
        config.BulkLoadNotifyAfter.Should().Be(1000);
        config.DeliverySemantic.Should().Be(DeliverySemantic.AtLeastOnce);
        config.CheckpointStrategy.Should().Be(CheckpointStrategy.None);
        config.EnableCdcCheckpointing.Should().BeFalse();
        config.CdcMode.Should().Be(CdcMode.BinlogFile);
        config.EnableMetrics.Should().BeTrue();
    }

    [Fact]
    public void Validate_WithValidConfiguration_ShouldNotThrow()
    {
        // Arrange
        var config = new MySqlConfiguration();

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithZeroCommandTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { CommandTimeout = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeCommandTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { CommandTimeout = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroConnectionTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { ConnectionTimeout = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeConnectionTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { ConnectionTimeout = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeMinPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { MinPoolSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroMaxPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { MaxPoolSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeMaxPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { MaxPoolSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithMinPoolSizeGreaterThanMaxPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration
        {
            MinPoolSize = 100,
            MaxPoolSize = 50,
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { BatchSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { BatchSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroMaxBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { MaxBatchSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeMaxBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { MaxBatchSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithBatchSizeGreaterThanMaxBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration
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
        var config = new MySqlConfiguration { MaxRetryAttempts = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeRetryDelay_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { RetryDelay = TimeSpan.FromSeconds(-1) };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroBulkLoadTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { BulkLoadTimeout = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeBulkLoadTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration { BulkLoadTimeout = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithUseUpsertEnabled_ShouldNotThrow()
    {
        // Arrange
        var config = new MySqlConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = ["Id"],
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithUseUpsertEnabledButNoKeyColumns_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration
        {
            UseUpsert = true,
        };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithUseUpsertEnabledButEmptyKeyColumns_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new MySqlConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = [],
        };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithBulkLoadWriteStrategy_ShouldNotThrow()
    {
        // Arrange
        var config = new MySqlConfiguration
        {
            WriteStrategy = MySqlWriteStrategy.BulkLoad,
            AllowLoadLocalInfile = true,
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithExactlyOnceDeliverySemantic_ShouldNotThrow()
    {
        // Arrange
        var config = new MySqlConfiguration
        {
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithOffsetCheckpointStrategy_ShouldNotThrow()
    {
        // Arrange
        var config = new MySqlConfiguration
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
    public void Validate_WithInMemoryCheckpointStrategy_ShouldNotThrow()
    {
        // Arrange
        var config = new MySqlConfiguration
        {
            CheckpointStrategy = CheckpointStrategy.InMemory,
        };

        // Act
        var action = () => config.Validate();

        // Assert
        action.Should().NotThrow();
    }

    [Fact]
    public void OnDuplicateKeyAction_ShouldHaveDefaultValue()
    {
        // Arrange & Act
        var config = new MySqlConfiguration();

        // Assert
        config.OnDuplicateKeyAction.Should().Be(OnDuplicateKeyAction.Update);
    }

    [Fact]
    public void RowErrorHandler_ShouldBeNullByDefault()
    {
        // Arrange & Act
        var config = new MySqlConfiguration();

        // Assert
        config.RowErrorHandler.Should().BeNull();
    }

    [Fact]
    public void DefaultDatabase_ShouldBeNullByDefault()
    {
        // Arrange & Act
        var config = new MySqlConfiguration();

        // Assert
        config.DefaultDatabase.Should().BeNull();
    }

    [Fact]
    public void ValidateConnectionSettings_WithValidSettings_ShouldNotThrow()
    {
        // Arrange
        var config = new MySqlConfiguration();

        // Act & Assert
        var exception = Record.Exception(() => config.ValidateConnectionSettings());
        exception.Should().BeNull();
    }
}

using AwesomeAssertions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.SqlServer.Configuration;

namespace NPipeline.Connectors.SqlServer.Tests.Configuration;

public sealed class SqlServerConfigurationTests
{
    [Fact]
    public void DefaultConfiguration_ShouldHaveValidDefaults()
    {
        // Arrange & Act
        var config = new SqlServerConfiguration();

        // Assert
        config.ConnectionString.Should().BeEmpty();
        config.Schema.Should().Be("dbo");
        config.CommandTimeout.Should().Be(30);
        config.BulkCopyTimeout.Should().Be(300);
        config.ConnectionTimeout.Should().Be(15);
        config.ConnectTimeout.Should().Be(15);
        config.MinPoolSize.Should().Be(1);
        config.MaxPoolSize.Should().Be(100);
        config.WriteStrategy.Should().Be(SqlServerWriteStrategy.Batch);
        config.BatchSize.Should().Be(100);
        config.MaxBatchSize.Should().Be(1000);
        config.UseTransaction.Should().BeTrue();
        config.UsePreparedStatements.Should().BeTrue();
        config.UseUpsert.Should().BeFalse();
        config.ContinueOnError.Should().BeFalse();
        config.ValidateIdentifiers.Should().BeTrue();
        config.MaxRetryAttempts.Should().Be(3);
        config.RetryDelay.Should().Be(TimeSpan.FromSeconds(1));
        config.CaseInsensitiveMapping.Should().BeTrue();
        config.CacheMappingMetadata.Should().BeTrue();
        config.StreamResults.Should().BeTrue();
        config.FetchSize.Should().Be(1000);
        config.ThrowOnMappingError.Should().BeTrue();
        config.EnableMARS.Should().BeFalse();
        config.EnableStreaming.Should().BeTrue();
        config.BulkCopyBatchSize.Should().Be(5000);
        config.BulkCopyNotifyAfter.Should().Be(1000);
        config.DeliverySemantic.Should().Be(DeliverySemantic.AtLeastOnce);
        config.CheckpointStrategy.Should().Be(CheckpointStrategy.None);
    }

    [Fact]
    public void Validate_WithValidConfiguration_ShouldNotThrow()
    {
        // Arrange
        var config = new SqlServerConfiguration();

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithEmptySchema_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { Schema = "" };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithWhitespaceSchema_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { Schema = "   " };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNullSchema_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { Schema = null! };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroCommandTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { CommandTimeout = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeCommandTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { CommandTimeout = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroBulkCopyTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { BulkCopyTimeout = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeBulkCopyTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { BulkCopyTimeout = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroConnectionTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { ConnectionTimeout = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeConnectionTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { ConnectionTimeout = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroConnectTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { ConnectTimeout = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeConnectTimeout_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { ConnectTimeout = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeMinPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { MinPoolSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroMaxPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { MaxPoolSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeMaxPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { MaxPoolSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithMinPoolSizeGreaterThanMaxPoolSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration
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
        var config = new SqlServerConfiguration { BatchSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { BatchSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroMaxBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { MaxBatchSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeMaxBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { MaxBatchSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithBatchSizeGreaterThanMaxBatchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration
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
        var config = new SqlServerConfiguration { MaxRetryAttempts = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeRetryDelay_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { RetryDelay = TimeSpan.FromSeconds(-1) };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithZeroFetchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { FetchSize = 0 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithNegativeFetchSize_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration { FetchSize = -1 };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithUseUpsertEnabled_ShouldThrowNotSupportedException()
    {
        // Arrange
        var config = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { "Id" },
        };

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithBulkCopyWriteStrategy_ShouldThrowNotSupportedException()
    {
        // Arrange
        var config = new SqlServerConfiguration
        {
            WriteStrategy = SqlServerWriteStrategy.BulkCopy,
        };

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithExactlyOnceDeliverySemantic_ShouldThrowNotSupportedException()
    {
        // Arrange
        var config = new SqlServerConfiguration
        {
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
        };

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithOffsetCheckpointStrategy_ShouldThrowNotSupportedException()
    {
        // Arrange
        var config = new SqlServerConfiguration
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
        var config = new SqlServerConfiguration
        {
            CheckpointStrategy = CheckpointStrategy.InMemory,
        };

        // Act
        var action = () => config.Validate();

        // Assert
        action.Should().NotThrow();
    }

    [Fact]
    public void Validate_WithUseUpsertEnabledButNoKeyColumns_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration
        {
            UseUpsert = true,
        };

        // Act & Assert
        _ = Assert.Throws<NotSupportedException>(() => config.Validate());
    }

    [Fact]
    public void Validate_WithUseUpsertEnabledButEmptyKeyColumns_ShouldThrowArgumentException()
    {
        // Arrange
        var config = new SqlServerConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = [],
        };

        // Act & Assert
        _ = Assert.Throws<NotSupportedException>(() => config.Validate());
    }

    [Fact]
    public void OnMergeAction_ShouldHaveDefaultValue()
    {
        // Arrange & Act
        var config = new SqlServerConfiguration();

        // Assert
        config.OnMergeAction.Should().Be(OnMergeAction.Update);
    }

    [Fact]
    public void RowErrorHandler_ShouldBeNullByDefault()
    {
        // Arrange & Act
        var config = new SqlServerConfiguration();

        // Assert
        config.RowErrorHandler.Should().BeNull();
    }

    [Fact]
    public void ApplicationName_ShouldBeNullByDefault()
    {
        // Arrange & Act
        var config = new SqlServerConfiguration();

        // Assert
        config.ApplicationName.Should().BeNull();
    }

    [Fact]
    public void ValidateConnectionSettings_WithValidSettings_ShouldNotThrow()
    {
        // Arrange
        var config = new SqlServerConfiguration();

        // Act & Assert
        var exception = Record.Exception(() => config.ValidateConnectionSettings());
        exception.Should().BeNull();
    }
}

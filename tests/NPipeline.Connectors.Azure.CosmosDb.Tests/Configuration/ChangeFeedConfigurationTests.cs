using AwesomeAssertions;
using FakeItEasy;
using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.ChangeFeed;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using CosmosChangeFeedStartFrom = NPipeline.Connectors.Azure.CosmosDb.Configuration.ChangeFeedStartFrom;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Configuration;

public sealed class ChangeFeedConfigurationTests
{
    [Fact]
    public void DefaultConfiguration_ShouldHaveValidDefaults()
    {
        // Arrange & Act
        var config = new ChangeFeedConfiguration();

        // Assert
        config.StartFrom.Should().Be(CosmosChangeFeedStartFrom.Beginning);
        config.StartTime.Should().BeNull();
        config.PollingInterval.Should().Be(TimeSpan.FromSeconds(1));
        config.MaxItemCount.Should().Be(100);
        config.IncludeFullDocuments.Should().BeTrue();
        config.PartitionKey.Should().BeNull();
        config.CheckpointStore.Should().BeNull();
        config.HandleRateLimiting.Should().BeTrue();
        config.MaxRateLimitWaitTime.Should().Be(TimeSpan.FromSeconds(30));
        config.ContinueOnError.Should().BeFalse();
    }

    [Fact]
    public void Validate_WithValidConfiguration_ShouldNotThrow()
    {
        // Arrange
        var config = new ChangeFeedConfiguration();

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithPointInTimeAndNoStartTime_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new ChangeFeedConfiguration
        {
            StartFrom = CosmosChangeFeedStartFrom.PointInTime,
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("StartTime is required when StartFrom is PointInTime");
    }

    [Fact]
    public void Validate_WithPointInTimeAndStartTime_ShouldNotThrow()
    {
        // Arrange
        var config = new ChangeFeedConfiguration
        {
            StartFrom = CosmosChangeFeedStartFrom.PointInTime,
            StartTime = DateTime.UtcNow.AddHours(-1),
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithZeroMaxItemCount_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new ChangeFeedConfiguration
        {
            MaxItemCount = 0,
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("MaxItemCount must be greater than 0");
    }

    [Fact]
    public void Validate_WithNegativeMaxItemCount_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new ChangeFeedConfiguration
        {
            MaxItemCount = -1,
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("MaxItemCount must be greater than 0");
    }

    [Fact]
    public void Validate_WithNegativePollingInterval_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new ChangeFeedConfiguration
        {
            PollingInterval = TimeSpan.FromSeconds(-1),
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("PollingInterval must be non-negative");
    }

    [Fact]
    public void Validate_WithZeroPollingInterval_ShouldNotThrow()
    {
        // Arrange
        var config = new ChangeFeedConfiguration
        {
            PollingInterval = TimeSpan.Zero,
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithNegativeMaxRateLimitWaitTime_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new ChangeFeedConfiguration
        {
            MaxRateLimitWaitTime = TimeSpan.FromSeconds(-1),
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("MaxRateLimitWaitTime must be non-negative");
    }

    [Fact]
    public void Validate_WithZeroMaxRateLimitWaitTime_ShouldNotThrow()
    {
        // Arrange
        var config = new ChangeFeedConfiguration
        {
            MaxRateLimitWaitTime = TimeSpan.Zero,
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Clone_ShouldCreateDeepCopy()
    {
        // Arrange
        var original = new ChangeFeedConfiguration
        {
            StartFrom = CosmosChangeFeedStartFrom.PointInTime,
            StartTime = DateTime.UtcNow.AddHours(-1),
            PollingInterval = TimeSpan.FromSeconds(5),
            MaxItemCount = 200,
            IncludeFullDocuments = false,
            PartitionKey = new PartitionKey("test"),
            HandleRateLimiting = false,
            MaxRateLimitWaitTime = TimeSpan.FromSeconds(60),
            ContinueOnError = true,
        };

        // Act
        var clone = original.Clone();

        // Assert
        clone.Should().NotBeSameAs(original);
        clone.StartFrom.Should().Be(original.StartFrom);
        clone.StartTime.Should().Be(original.StartTime);
        clone.PollingInterval.Should().Be(original.PollingInterval);
        clone.MaxItemCount.Should().Be(original.MaxItemCount);
        clone.IncludeFullDocuments.Should().Be(original.IncludeFullDocuments);
        clone.PartitionKey.Should().Be(original.PartitionKey);
        clone.CheckpointStore.Should().Be(original.CheckpointStore);
        clone.HandleRateLimiting.Should().Be(original.HandleRateLimiting);
        clone.MaxRateLimitWaitTime.Should().Be(original.MaxRateLimitWaitTime);
        clone.ContinueOnError.Should().Be(original.ContinueOnError);
    }

    [Fact]
    public void Clone_WhenModifyingClone_ShouldNotAffectOriginal()
    {
        // Arrange
        var original = new ChangeFeedConfiguration
        {
            StartFrom = CosmosChangeFeedStartFrom.Beginning,
            MaxItemCount = 100,
            PollingInterval = TimeSpan.FromSeconds(1),
        };

        // Act
        var clone = original.Clone();
        clone.StartFrom = CosmosChangeFeedStartFrom.Now;
        clone.MaxItemCount = 500;
        clone.PollingInterval = TimeSpan.FromSeconds(10);

        // Assert
        original.StartFrom.Should().Be(CosmosChangeFeedStartFrom.Beginning);
        original.MaxItemCount.Should().Be(100);
        original.PollingInterval.Should().Be(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Clone_WithCheckpointStore_ShouldShareSameInstance()
    {
        // Arrange
        var checkpointStore = A.Fake<IChangeFeedCheckpointStore>();

        var original = new ChangeFeedConfiguration
        {
            CheckpointStore = checkpointStore,
        };

        // Act
        var clone = original.Clone();

        // Assert
        clone.CheckpointStore.Should().BeSameAs(original.CheckpointStore);
    }

    [Fact]
    public void StartFrom_ShouldSupportAllEnumValues()
    {
        // Arrange
        var config = new ChangeFeedConfiguration();

        // Act & Assert - Beginning
        config.StartFrom = CosmosChangeFeedStartFrom.Beginning;
        config.StartFrom.Should().Be(CosmosChangeFeedStartFrom.Beginning);

        // Act & Assert - Now
        config.StartFrom = CosmosChangeFeedStartFrom.Now;
        config.StartFrom.Should().Be(CosmosChangeFeedStartFrom.Now);

        // Act & Assert - PointInTime
        config.StartFrom = CosmosChangeFeedStartFrom.PointInTime;
        config.StartFrom.Should().Be(CosmosChangeFeedStartFrom.PointInTime);

        // Act & Assert - ContinuationToken
        config.StartFrom = CosmosChangeFeedStartFrom.ContinuationToken;
        config.StartFrom.Should().Be(CosmosChangeFeedStartFrom.ContinuationToken);
    }

    [Fact]
    public void StartTime_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var config = new ChangeFeedConfiguration();
        var startTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        // Act
        config.StartTime = startTime;

        // Assert
        config.StartTime.Should().Be(startTime);
    }

    [Fact]
    public void PartitionKey_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var config = new ChangeFeedConfiguration();
        var partitionKey = new PartitionKey("partitionValue");

        // Act
        config.PartitionKey = partitionKey;

        // Assert
        config.PartitionKey.Should().Be(partitionKey);
    }

    [Fact]
    public void CheckpointStore_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var config = new ChangeFeedConfiguration();
        var checkpointStore = A.Fake<IChangeFeedCheckpointStore>();

        // Act
        config.CheckpointStore = checkpointStore;

        // Assert
        config.CheckpointStore.Should().Be(checkpointStore);
    }

    [Fact]
    public void Validate_WithNowStartFrom_ShouldNotRequireStartTime()
    {
        // Arrange
        var config = new ChangeFeedConfiguration
        {
            StartFrom = CosmosChangeFeedStartFrom.Now,
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithContinuationTokenStartFrom_ShouldNotRequireStartTime()
    {
        // Arrange
        var config = new ChangeFeedConfiguration
        {
            StartFrom = CosmosChangeFeedStartFrom.ContinuationToken,
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithBeginningStartFrom_ShouldNotRequireStartTime()
    {
        // Arrange
        var config = new ChangeFeedConfiguration
        {
            StartFrom = CosmosChangeFeedStartFrom.Beginning,
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }
}

using NPipeline.Connectors.MongoDB.Configuration;

namespace NPipeline.Connectors.MongoDB.Tests.Configuration;

/// <summary>
///     Unit tests for <see cref="MongoConfiguration" /> defaults and validation.
/// </summary>
public sealed class MongoConfigurationTests
{
    [Fact]
    public void Defaults_AreCorrectPerSpec()
    {
        var config = new MongoConfiguration();

        config.BatchSize.Should().Be(1_000);
        config.WriteBatchSize.Should().Be(1_000);
        config.OrderedWrites.Should().BeFalse(); // unordered = faster
        config.WriteStrategy.Should().Be(MongoWriteStrategy.BulkWrite); // plan default
        config.OnDuplicate.Should().Be(OnDuplicateAction.Fail);
        config.UpsertKeyFields.Should().BeEquivalentTo("_id");
        config.MaxRetryAttempts.Should().Be(3);
        config.RetryDelay.TotalSeconds.Should().Be(1);
        config.ContinueOnError.Should().BeFalse();
        config.NoCursorTimeout.Should().BeFalse();
        config.CommandTimeoutSeconds.Should().Be(30);
    }

    [Fact]
    public void Validate_ThrowsWhenDatabaseNameIsEmpty()
    {
        var config = new MongoConfiguration { CollectionName = "col" };
        var act = () => config.Validate();
        act.Should().Throw<InvalidOperationException>().WithMessage("*DatabaseName*");
    }

    [Fact]
    public void Validate_ThrowsWhenCollectionNameIsEmpty()
    {
        var config = new MongoConfiguration { DatabaseName = "db" };
        var act = () => config.Validate();
        act.Should().Throw<InvalidOperationException>().WithMessage("*CollectionName*");
    }

    [Fact]
    public void Validate_ThrowsWhenBatchSizeIsZero()
    {
        var config = ValidConfig();
        config.BatchSize = 0;
        var act = () => config.Validate();
        act.Should().Throw<ArgumentException>().WithMessage("*BatchSize*");
    }

    [Fact]
    public void Validate_ThrowsWhenWriteBatchSizeIsNegative()
    {
        var config = ValidConfig();
        config.WriteBatchSize = -1;
        var act = () => config.Validate();
        act.Should().Throw<ArgumentException>().WithMessage("*WriteBatchSize*");
    }

    [Fact]
    public void Validate_ThrowsWhenRetryAttemptsIsNegative()
    {
        var config = ValidConfig();
        config.MaxRetryAttempts = -1;
        var act = () => config.Validate();
        act.Should().Throw<ArgumentException>().WithMessage("*MaxRetryAttempts*");
    }

    [Fact]
    public void Validate_ThrowsWhenRetryDelayIsNegative()
    {
        var config = ValidConfig();
        config.RetryDelay = TimeSpan.FromSeconds(-1);
        var act = () => config.Validate();
        act.Should().Throw<ArgumentException>().WithMessage("*RetryDelay*");
    }

    [Fact]
    public void Validate_ThrowsWhenUpsertStrategyHasEmptyKeyFields()
    {
        var config = ValidConfig();
        config.WriteStrategy = MongoWriteStrategy.Upsert;
        config.UpsertKeyFields = [];
        var act = () => config.Validate();
        act.Should().Throw<ArgumentException>().WithMessage("*UpsertKeyFields*");
    }

    [Fact]
    public void Validate_SucceedsWithMinimalValidConfig()
    {
        var config = ValidConfig();
        var act = () => config.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_SucceedsWithUpsertStrategyAndKeyFields()
    {
        var config = ValidConfig();
        config.WriteStrategy = MongoWriteStrategy.Upsert;
        config.UpsertKeyFields = ["_id"];
        var act = () => config.Validate();
        act.Should().NotThrow();
    }

    private static MongoConfiguration ValidConfig()
    {
        return new MongoConfiguration { DatabaseName = "testdb", CollectionName = "testcol" };
    }
}

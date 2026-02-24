using AwesomeAssertions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Configuration;

public class CosmosWriteStrategyTests
{
    [Fact]
    public void Insert_ShouldHaveCorrectValue()
    {
        // Act
        var strategy = CosmosWriteStrategy.Insert;

        // Assert
        _ = strategy.Should().Be(CosmosWriteStrategy.Insert);
        ((int)strategy).Should().Be(0);
    }

    [Fact]
    public void PerRow_ShouldBeAliasForInsert()
    {
        // Act
        var strategy = CosmosWriteStrategy.PerRow;

        // Assert
        _ = strategy.Should().Be(CosmosWriteStrategy.Insert);
        ((int)strategy).Should().Be(0);
    }

    [Fact]
    public void Upsert_ShouldHaveCorrectValue()
    {
        // Act
        var strategy = CosmosWriteStrategy.Upsert;

        // Assert
        _ = strategy.Should().Be(CosmosWriteStrategy.Upsert);
        ((int)strategy).Should().Be(1);
    }

    [Fact]
    public void Batch_ShouldHaveCorrectValue()
    {
        // Act
        var strategy = CosmosWriteStrategy.Batch;

        // Assert
        _ = strategy.Should().Be(CosmosWriteStrategy.Batch);
        ((int)strategy).Should().Be(2);
    }

    [Fact]
    public void TransactionalBatch_ShouldHaveCorrectValue()
    {
        // Act
        var strategy = CosmosWriteStrategy.TransactionalBatch;

        // Assert
        _ = strategy.Should().Be(CosmosWriteStrategy.TransactionalBatch);
        ((int)strategy).Should().Be(3);
    }

    [Fact]
    public void Bulk_ShouldHaveCorrectValue()
    {
        // Act
        var strategy = CosmosWriteStrategy.Bulk;

        // Assert
        _ = strategy.Should().Be(CosmosWriteStrategy.Bulk);
        ((int)strategy).Should().Be(4);
    }

    [Fact]
    public void Default_ShouldBeInsert()
    {
        // Act
        var defaultStrategy = default(CosmosWriteStrategy);

        // Assert
        _ = defaultStrategy.Should().Be(CosmosWriteStrategy.Insert);
    }

    [Fact]
    public void AllEnumValues_ShouldBeDefined()
    {
        // Arrange
        var expectedValues = new[]
        {
            CosmosWriteStrategy.Insert,
            CosmosWriteStrategy.Upsert,
            CosmosWriteStrategy.Batch,
            CosmosWriteStrategy.TransactionalBatch,
            CosmosWriteStrategy.Bulk,
        };

        // Act
        // Use Distinct() because Enum.GetValues returns duplicates for aliased values (e.g., PerRow = Insert)
        var actualValues = Enum.GetValues<CosmosWriteStrategy>().Distinct();

        // Assert
        actualValues.Should().BeEquivalentTo(expectedValues);
    }

    [Theory]
    [InlineData(CosmosWriteStrategy.Insert, 0)]
    [InlineData(CosmosWriteStrategy.Upsert, 1)]
    [InlineData(CosmosWriteStrategy.Batch, 2)]
    [InlineData(CosmosWriteStrategy.TransactionalBatch, 3)]
    [InlineData(CosmosWriteStrategy.Bulk, 4)]
    public void EnumValue_ShouldHaveExpectedIntValue(CosmosWriteStrategy strategy, int expectedValue)
    {
        // Act & Assert
        ((int)strategy).Should().Be(expectedValue);
    }
}

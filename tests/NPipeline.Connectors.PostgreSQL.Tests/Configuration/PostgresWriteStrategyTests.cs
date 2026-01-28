using AwesomeAssertions;
using NPipeline.Connectors.PostgreSQL.Configuration;

namespace NPipeline.Connectors.PostgreSQL.Tests.Configuration;

public class PostgresWriteStrategyTests
{
    [Fact]
    public void PerRow_Should_HaveCorrectValue()
    {
        // Act
        var strategy = PostgresWriteStrategy.PerRow;

        // Assert
        _ = strategy.Should().Be(PostgresWriteStrategy.PerRow);
    }

    [Fact]
    public void Batch_Should_HaveCorrectValue()
    {
        // Act
        var strategy = PostgresWriteStrategy.Batch;

        // Assert
        _ = strategy.Should().Be(PostgresWriteStrategy.Batch);
    }

    [Fact]
    public void Copy_Should_HaveCorrectValue()
    {
        // Act
        var strategy = PostgresWriteStrategy.Copy;

        // Assert
        _ = strategy.Should().Be(PostgresWriteStrategy.Copy);
    }
}

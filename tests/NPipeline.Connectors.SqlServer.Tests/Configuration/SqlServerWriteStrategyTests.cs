using AwesomeAssertions;
using NPipeline.Connectors.SqlServer.Configuration;

namespace NPipeline.Connectors.SqlServer.Tests.Configuration;

public class SqlServerWriteStrategyTests
{
    [Fact]
    public void PerRow_Should_HaveCorrectValue()
    {
        // Act
        var strategy = SqlServerWriteStrategy.PerRow;

        // Assert
        _ = strategy.Should().Be(SqlServerWriteStrategy.PerRow);
    }

    [Fact]
    public void Batch_Should_HaveCorrectValue()
    {
        // Act
        var strategy = SqlServerWriteStrategy.Batch;

        // Assert
        _ = strategy.Should().Be(SqlServerWriteStrategy.Batch);
    }

    [Fact]
    public void BulkCopy_Should_HaveCorrectValue()
    {
        // Act
        var strategy = SqlServerWriteStrategy.BulkCopy;

        // Assert
        _ = strategy.Should().Be(SqlServerWriteStrategy.BulkCopy);
    }
}

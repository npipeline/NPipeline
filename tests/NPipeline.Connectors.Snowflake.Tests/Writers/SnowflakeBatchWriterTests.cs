using FakeItEasy;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Writers;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Snowflake.Tests.Writers;

public sealed class SnowflakeBatchWriterTests
{
    [Fact]
    public async Task FlushAsync_WithEmptyBuffer_ShouldBeNoOp()
    {
        // Arrange
        var command = A.Fake<IDatabaseCommand>();
        var connection = A.Fake<IDatabaseConnection>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(Task.FromResult(command));

        var writer = new SnowflakeBatchWriter<BatchEntity>(
            connection,
            "PUBLIC",
            "ORDERS",
            null,
            new SnowflakeConfiguration { BatchSize = 2, MaxBatchSize = 10 });

        // Act
        await writer.FlushAsync();

        // Assert
        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_WithInvalidCustomMapperCount_ShouldThrow()
    {
        // Arrange
        var command = A.Fake<IDatabaseCommand>();
        var connection = A.Fake<IDatabaseConnection>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(Task.FromResult(command));

        var writer = new SnowflakeBatchWriter<BatchEntity>(
            connection,
            "PUBLIC",
            "ORDERS",
            _ => [],
            new SnowflakeConfiguration { BatchSize = 10, MaxBatchSize = 100 });

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.WriteAsync(new BatchEntity { FirstName = "Ada" }));
    }

    private sealed class BatchEntity
    {
        public string FirstName { get; set; } = string.Empty;
    }
}

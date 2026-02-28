using FakeItEasy;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Mapping;
using NPipeline.Connectors.Snowflake.Writers;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Snowflake.Tests.Writers;

public sealed class SnowflakeBatchWriterUpsertTests
{
    [Fact]
    public async Task WriteAsync_WithUpsertEnabled_ShouldGenerateMergeStatement()
    {
        // Arrange
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._)).Returns(1);

        var connection = A.Fake<IDatabaseConnection>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(Task.FromResult(command));

        var configuration = new SnowflakeConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = ["ID"],
            OnMergeAction = OnMergeAction.Update,
            BatchSize = 1,
            MaxBatchSize = 10,
        };

        var writer = new SnowflakeBatchWriter<UpsertEntity>(
            connection,
            "PUBLIC",
            "CUSTOMERS",
            null,
            configuration);

        // Act
        await writer.WriteAsync(new UpsertEntity { Id = 1, FirstName = "Ada" });

        // Assert
        A.CallToSet(() => command.CommandText).WhenArgumentsMatch(args =>
                (args.Get<string>(0) ?? string.Empty).StartsWith("MERGE INTO", StringComparison.OrdinalIgnoreCase))
            .MustHaveHappened();
    }

    private sealed class UpsertEntity
    {
        [SnowflakeColumn("ID", PrimaryKey = true)]
        public int Id { get; set; }

        public string FirstName { get; set; } = string.Empty;
    }
}

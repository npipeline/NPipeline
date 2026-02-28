using FakeItEasy;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Writers;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Snowflake.Tests.Writers;

public sealed class SnowflakePerRowWriterTests
{
    [Fact]
    public async Task WriteAsync_WithConventionModel_ShouldUseUpperSnakeCaseColumns()
    {
        // Arrange
        var command = A.Fake<IDatabaseCommand>();
        A.CallTo(() => command.ExecuteNonQueryAsync(A<CancellationToken>._)).Returns(1);

        var connection = A.Fake<IDatabaseConnection>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(Task.FromResult(command));

        var writer = new SnowflakePerRowWriter<ConventionEntity>(
            connection,
            "PUBLIC",
            "CUSTOMERS",
            null,
            new SnowflakeConfiguration());

        // Act
        await writer.WriteAsync(new ConventionEntity { FirstName = "Ada" });

        // Assert
        A.CallToSet(() => command.CommandText).WhenArgumentsMatch(args =>
                (args.Get<string>(0) ?? string.Empty).Contains("\"FIRST_NAME\"", StringComparison.Ordinal))
            .MustHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_WithInvalidCustomMapperCount_ShouldThrow()
    {
        // Arrange
        var command = A.Fake<IDatabaseCommand>();
        var connection = A.Fake<IDatabaseConnection>();
        A.CallTo(() => connection.CreateCommandAsync(A<CancellationToken>._)).Returns(Task.FromResult(command));

        var writer = new SnowflakePerRowWriter<ConventionEntity>(
            connection,
            "PUBLIC",
            "CUSTOMERS",
            _ => [],
            new SnowflakeConfiguration());

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.WriteAsync(new ConventionEntity { FirstName = "Ada" }));
    }

    private sealed class ConventionEntity
    {
        public string FirstName { get; set; } = string.Empty;
    }
}

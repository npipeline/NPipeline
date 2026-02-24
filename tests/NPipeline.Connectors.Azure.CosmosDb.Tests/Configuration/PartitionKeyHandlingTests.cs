using AwesomeAssertions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Configuration;

public class PartitionKeyHandlingTests
{
    [Fact]
    public void Auto_ShouldHaveCorrectValue()
    {
        // Act
        var handling = PartitionKeyHandling.Auto;

        // Assert
        _ = handling.Should().Be(PartitionKeyHandling.Auto);
        ((int)handling).Should().Be(0);
    }

    [Fact]
    public void Explicit_ShouldHaveCorrectValue()
    {
        // Act
        var handling = PartitionKeyHandling.Explicit;

        // Assert
        _ = handling.Should().Be(PartitionKeyHandling.Explicit);
        ((int)handling).Should().Be(1);
    }

    [Fact]
    public void None_ShouldHaveCorrectValue()
    {
        // Act
        var handling = PartitionKeyHandling.None;

        // Assert
        _ = handling.Should().Be(PartitionKeyHandling.None);
        ((int)handling).Should().Be(2);
    }

    [Fact]
    public void Default_ShouldBeAuto()
    {
        // Act
        var defaultHandling = default(PartitionKeyHandling);

        // Assert
        _ = defaultHandling.Should().Be(PartitionKeyHandling.Auto);
    }

    [Fact]
    public void AllEnumValues_ShouldBeDefined()
    {
        // Arrange
        var expectedValues = new[]
        {
            PartitionKeyHandling.Auto,
            PartitionKeyHandling.Explicit,
            PartitionKeyHandling.None,
        };

        // Act
        var actualValues = Enum.GetValues<PartitionKeyHandling>();

        // Assert
        actualValues.Should().BeEquivalentTo(expectedValues);
    }

    [Theory]
    [InlineData(PartitionKeyHandling.Auto, 0)]
    [InlineData(PartitionKeyHandling.Explicit, 1)]
    [InlineData(PartitionKeyHandling.None, 2)]
    public void EnumValue_ShouldHaveExpectedIntValue(PartitionKeyHandling handling, int expectedValue)
    {
        // Act & Assert
        ((int)handling).Should().Be(expectedValue);
    }
}

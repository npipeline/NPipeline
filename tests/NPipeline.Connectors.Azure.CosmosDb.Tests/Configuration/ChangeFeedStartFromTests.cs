using AwesomeAssertions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Configuration;

public class ChangeFeedStartFromTests
{
    [Fact]
    public void Beginning_ShouldHaveCorrectValue()
    {
        // Act
        var startFrom = ChangeFeedStartFrom.Beginning;

        // Assert
        _ = startFrom.Should().Be(ChangeFeedStartFrom.Beginning);
        ((int)startFrom).Should().Be(0);
    }

    [Fact]
    public void Now_ShouldHaveCorrectValue()
    {
        // Act
        var startFrom = ChangeFeedStartFrom.Now;

        // Assert
        _ = startFrom.Should().Be(ChangeFeedStartFrom.Now);
        ((int)startFrom).Should().Be(1);
    }

    [Fact]
    public void PointInTime_ShouldHaveCorrectValue()
    {
        // Act
        var startFrom = ChangeFeedStartFrom.PointInTime;

        // Assert
        _ = startFrom.Should().Be(ChangeFeedStartFrom.PointInTime);
        ((int)startFrom).Should().Be(2);
    }

    [Fact]
    public void Time_ShouldBeAliasForPointInTime()
    {
        // Act
        var startFrom = ChangeFeedStartFrom.Time;

        // Assert
        _ = startFrom.Should().Be(ChangeFeedStartFrom.PointInTime);
        ((int)startFrom).Should().Be(2);
    }

    [Fact]
    public void ContinuationToken_ShouldHaveCorrectValue()
    {
        // Act
        var startFrom = ChangeFeedStartFrom.ContinuationToken;

        // Assert
        _ = startFrom.Should().Be(ChangeFeedStartFrom.ContinuationToken);
        ((int)startFrom).Should().Be(3);
    }

    [Fact]
    public void Default_ShouldBeBeginning()
    {
        // Act
        var defaultStartFrom = default(ChangeFeedStartFrom);

        // Assert
        _ = defaultStartFrom.Should().Be(ChangeFeedStartFrom.Beginning);
    }

    [Fact]
    public void AllEnumValues_ShouldBeDefined()
    {
        // Arrange
        var expectedValues = new[]
        {
            ChangeFeedStartFrom.Beginning,
            ChangeFeedStartFrom.Now,
            ChangeFeedStartFrom.PointInTime,
            ChangeFeedStartFrom.ContinuationToken,
        };

        // Act
        // Use Distinct() because Enum.GetValues returns duplicates for aliased values (e.g., Time = PointInTime)
        var actualValues = Enum.GetValues<ChangeFeedStartFrom>().Distinct();

        // Assert
        actualValues.Should().BeEquivalentTo(expectedValues);
    }

    [Theory]
    [InlineData(ChangeFeedStartFrom.Beginning, 0)]
    [InlineData(ChangeFeedStartFrom.Now, 1)]
    [InlineData(ChangeFeedStartFrom.PointInTime, 2)]
    [InlineData(ChangeFeedStartFrom.ContinuationToken, 3)]
    public void EnumValue_ShouldHaveExpectedIntValue(ChangeFeedStartFrom startFrom, int expectedValue)
    {
        // Act & Assert
        ((int)startFrom).Should().Be(expectedValue);
    }
}

using AwesomeAssertions;
using NPipeline.Connectors.PostgreSQL.Configuration;

namespace NPipeline.Connectors.PostgreSQL.Tests.Configuration;

public class OnConflictActionTests
{
    [Fact]
    public void Update_Should_HaveCorrectValue()
    {
        // Act
        var action = OnConflictAction.Update;

        // Assert
        _ = action.Should().Be(OnConflictAction.Update);
    }

    [Fact]
    public void Ignore_Should_HaveCorrectValue()
    {
        // Act
        var action = OnConflictAction.Ignore;

        // Assert
        _ = action.Should().Be(OnConflictAction.Ignore);
    }

    [Fact]
    public void Default_ShouldBeUpdate()
    {
        // Act
        var defaultAction = default(OnConflictAction);

        // Assert
        _ = defaultAction.Should().Be(OnConflictAction.Update);
    }
}

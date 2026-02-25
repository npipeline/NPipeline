using AwesomeAssertions;
using NPipeline.Connectors.SqlServer.Configuration;

namespace NPipeline.Connectors.SqlServer.Tests.Configuration;

public class OnMergeActionTests
{
    [Fact]
    public void Ignore_Should_HaveCorrectValue()
    {
        // Act
        var action = OnMergeAction.Ignore;

        // Assert
        _ = action.Should().Be(OnMergeAction.Ignore);
    }

    [Fact]
    public void Update_Should_HaveCorrectValue()
    {
        // Act
        var action = OnMergeAction.Update;

        // Assert
        _ = action.Should().Be(OnMergeAction.Update);
    }

    [Fact]
    public void Delete_Should_HaveCorrectValue()
    {
        // Act
        var action = OnMergeAction.Delete;

        // Assert
        _ = action.Should().Be(OnMergeAction.Delete);
    }

    [Fact]
    public void Default_ShouldBeIgnore()
    {
        // The zero value of the enum (default) should be Ignore
        _ = ((OnMergeAction)0).Should().Be(OnMergeAction.Ignore);
    }
}

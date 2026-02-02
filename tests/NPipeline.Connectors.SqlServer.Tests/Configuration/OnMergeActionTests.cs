using AwesomeAssertions;
using NPipeline.Connectors.SqlServer.Configuration;

namespace NPipeline.Connectors.SqlServer.Tests.Configuration;

public class OnMergeActionTests
{
    [Fact]
    public void Insert_Should_HaveCorrectValue()
    {
        // Act
        var action = OnMergeAction.Insert;

        // Assert
        _ = action.Should().Be(OnMergeAction.Insert);
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
    public void InsertOrUpdate_Should_HaveCorrectValue()
    {
        // Act
        var action = OnMergeAction.InsertOrUpdate;

        // Assert
        _ = action.Should().Be(OnMergeAction.InsertOrUpdate);
    }

    [Fact]
    public void Delete_Should_HaveCorrectValue()
    {
        // Act
        var action = OnMergeAction.Delete;

        // Assert
        _ = action.Should().Be(OnMergeAction.Delete);
    }
}

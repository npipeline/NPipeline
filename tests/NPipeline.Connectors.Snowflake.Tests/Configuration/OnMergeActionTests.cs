using NPipeline.Connectors.Snowflake.Configuration;

namespace NPipeline.Connectors.Snowflake.Tests.Configuration;

public sealed class OnMergeActionTests
{
    [Fact]
    public void OnMergeAction_Ignore_ShouldHaveCorrectValue()
    {
        Assert.Equal(0, (int)OnMergeAction.Ignore);
    }

    [Fact]
    public void OnMergeAction_Update_ShouldHaveCorrectValue()
    {
        Assert.Equal(1, (int)OnMergeAction.Update);
    }

    [Fact]
    public void OnMergeAction_Delete_ShouldHaveCorrectValue()
    {
        Assert.Equal(2, (int)OnMergeAction.Delete);
    }

    [Fact]
    public void OnMergeAction_ShouldHaveThreeValues()
    {
        var values = Enum.GetValues<OnMergeAction>();
        Assert.Equal(3, values.Length);
    }
}

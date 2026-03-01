using NPipeline.Connectors.Aws.Redshift.Configuration;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Configuration;

public class OnMergeActionTests
{
    [Fact]
    public void Update_ShouldHaveValueZero()
    {
        OnMergeAction.Update.Should().Be(0);
    }

    [Fact]
    public void Skip_ShouldHaveValueOne()
    {
        OnMergeAction.Skip.Should().Be((OnMergeAction)1);
    }

    [Fact]
    public void Enum_ShouldHaveTwoValues()
    {
        Enum.GetValues<OnMergeAction>().Should().HaveCount(2);
    }
}

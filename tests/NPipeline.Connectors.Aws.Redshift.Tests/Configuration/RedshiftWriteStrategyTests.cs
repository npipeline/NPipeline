using NPipeline.Connectors.Aws.Redshift.Configuration;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Configuration;

public class RedshiftWriteStrategyTests
{
    [Fact]
    public void PerRow_ShouldHaveValueZero()
    {
        RedshiftWriteStrategy.PerRow.Should().Be(0);
    }

    [Fact]
    public void Batch_ShouldHaveValueOne()
    {
        RedshiftWriteStrategy.Batch.Should().Be((RedshiftWriteStrategy)1);
    }

    [Fact]
    public void CopyFromS3_ShouldHaveValueTwo()
    {
        RedshiftWriteStrategy.CopyFromS3.Should().Be((RedshiftWriteStrategy)2);
    }

    [Fact]
    public void Enum_ShouldHaveThreeValues()
    {
        Enum.GetValues<RedshiftWriteStrategy>().Should().HaveCount(3);
    }
}

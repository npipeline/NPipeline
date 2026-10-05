using AwesomeAssertions;
using Xunit;

namespace NPipeline.StorageProviders.S3.Tests;

public class S3CoreOptionsTests
{
    [Fact]
    public void Defaults_Are8MiBPartsAndFourConcurrentUploads()
    {
        var options = new S3CoreOptions();

        options.PartSizeBytes.Should().Be(8 * 1024 * 1024);
        options.MaxConcurrency.Should().Be(4);
    }

    [Theory]
    [InlineData(5 * 1024 * 1024)]
    [InlineData(64 * 1024 * 1024)]
    [InlineData(int.MaxValue)]
    public void PartSizeBytes_AcceptsValuesFromTheS3Minimum(int size)
    {
        var options = new S3CoreOptions { PartSizeBytes = size };

        options.PartSizeBytes.Should().Be(size);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(5 * 1024 * 1024 - 1)]
    public void PartSizeBytes_BelowTheS3Minimum_Throws(int size)
    {
        var options = new S3CoreOptions();

        var act = () => options.PartSizeBytes = size;

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void MaxConcurrency_NotPositive_Throws(int value)
    {
        var options = new S3CoreOptions();

        var act = () => options.MaxConcurrency = value;

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void TwoInstances_HaveIndependentValues()
    {
        var options1 = new S3CoreOptions { MaxConcurrency = 9 };
        var options2 = new S3CoreOptions();

        options2.MaxConcurrency.Should().Be(4);
    }
}

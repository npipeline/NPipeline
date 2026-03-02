using AwesomeAssertions;
using Xunit;

namespace NPipeline.StorageProviders.S3.Tests;

public class S3CoreOptionsTests
{
    [Fact]
    public void Default_MultipartUploadThresholdBytes_Is64MB()
    {
        // Arrange
        var options = new S3CoreOptions();

        // Act & Assert
        options.MultipartUploadThresholdBytes.Should().Be(64 * 1024 * 1024);
    }

    [Fact]
    public void MultipartUploadThresholdBytes_CanBeChanged()
    {
        // Arrange
        var options = new S3CoreOptions();

        // Act
        options.MultipartUploadThresholdBytes = 128 * 1024 * 1024;

        // Assert
        options.MultipartUploadThresholdBytes.Should().Be(128 * 1024 * 1024);
    }

    [Fact]
    public void MultipartUploadThresholdBytes_CanBeSetToZero()
    {
        // Arrange
        var options = new S3CoreOptions();

        // Act
        options.MultipartUploadThresholdBytes = 0;

        // Assert
        options.MultipartUploadThresholdBytes.Should().Be(0);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5 * 1024 * 1024)]
    [InlineData(64 * 1024 * 1024)]
    [InlineData(256 * 1024 * 1024)]
    [InlineData(long.MaxValue)]
    public void MultipartUploadThresholdBytes_AcceptsVariousValues(long threshold)
    {
        // Arrange
        var options = new S3CoreOptions();

        // Act
        options.MultipartUploadThresholdBytes = threshold;

        // Assert
        options.MultipartUploadThresholdBytes.Should().Be(threshold);
    }

    [Fact]
    public void TwoInstances_HaveIndependentValues()
    {
        // Arrange
        var options1 = new S3CoreOptions();
        var options2 = new S3CoreOptions();

        // Act
        options1.MultipartUploadThresholdBytes = 10;

        // Assert
        options2.MultipartUploadThresholdBytes.Should().Be(64 * 1024 * 1024);
    }
}

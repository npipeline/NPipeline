using Amazon;
using Amazon.Runtime;
using AwesomeAssertions;
using Xunit;

namespace NPipeline.StorageProviders.S3.Aws.Tests;

public class AwsS3StorageProviderOptionsTests
{
    [Fact]
    public void DefaultValues_AreCorrect()
    {
        // Act
        var options = new AwsS3StorageProviderOptions();

        // Assert
        options.DefaultRegion.Should().BeNull();
        options.DefaultCredentials.Should().BeNull();
        options.UseDefaultCredentialChain.Should().BeTrue();
        options.ServiceUrl.Should().BeNull();
        options.ForcePathStyle.Should().BeFalse();
        options.PartSizeBytes.Should().Be(8 * 1024 * 1024);
    }

    [Fact]
    public void DefaultRegion_CanBeSet()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();
        var expectedRegion = RegionEndpoint.APSoutheast2;

        // Act
        options.DefaultRegion = expectedRegion;

        // Assert
        options.DefaultRegion.Should().Be(expectedRegion);
    }

    [Fact]
    public void DefaultCredentials_CanBeSet()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();
        var expectedCredentials = new BasicAWSCredentials("test-key", "test-secret");

        // Act
        options.DefaultCredentials = expectedCredentials;

        // Assert
        options.DefaultCredentials.Should().Be(expectedCredentials);
    }

    [Fact]
    public void UseDefaultCredentialChain_CanBeSet()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act
        options.UseDefaultCredentialChain = false;

        // Assert
        options.UseDefaultCredentialChain.Should().BeFalse();
    }

    [Fact]
    public void ServiceUrl_CanBeSet()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();
        var expectedUrl = new Uri("http://localhost:9000");

        // Act
        options.ServiceUrl = expectedUrl;

        // Assert
        options.ServiceUrl.Should().Be(expectedUrl);
    }

    [Fact]
    public void ForcePathStyle_CanBeSet()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act
        options.ForcePathStyle = true;

        // Assert
        options.ForcePathStyle.Should().BeTrue();
    }

    [Fact]
    public void PartSizeBytes_CanBeSet()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();
        var expectedThreshold = 16 * 1024 * 1024;

        // Act
        options.PartSizeBytes = expectedThreshold;

        // Assert
        options.PartSizeBytes.Should().Be(expectedThreshold);
    }

    [Fact]
    public void ConfigurationViaAction_UpdatesAllProperties()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act
        options.DefaultRegion = RegionEndpoint.EUWest1;
        options.DefaultCredentials = new BasicAWSCredentials("key", "secret");
        options.UseDefaultCredentialChain = false;
        options.ServiceUrl = new Uri("https://s3.example.com");
        options.ForcePathStyle = true;
        options.PartSizeBytes = 32 * 1024 * 1024;

        // Assert
        options.DefaultRegion.Should().Be(RegionEndpoint.EUWest1);
        options.DefaultCredentials.Should().NotBeNull();
        options.UseDefaultCredentialChain.Should().BeFalse();
        options.ServiceUrl.Should().Be(new Uri("https://s3.example.com"));
        options.ForcePathStyle.Should().BeTrue();
        options.PartSizeBytes.Should().Be(32 * 1024 * 1024);
    }

    [Fact]
    public void DefaultRegion_AcceptsUSEast1()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act
        options.DefaultRegion = RegionEndpoint.USEast1;

        // Assert
        options.DefaultRegion.Should().Be(RegionEndpoint.USEast1);
    }

    [Fact]
    public void DefaultRegion_AcceptsUSWest2()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act
        options.DefaultRegion = RegionEndpoint.USWest2;

        // Assert
        options.DefaultRegion.Should().Be(RegionEndpoint.USWest2);
    }

    [Fact]
    public void DefaultRegion_AcceptsEUWest1()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act
        options.DefaultRegion = RegionEndpoint.EUWest1;

        // Assert
        options.DefaultRegion.Should().Be(RegionEndpoint.EUWest1);
    }

    [Fact]
    public void DefaultRegion_AcceptsAPSoutheast2()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act
        options.DefaultRegion = RegionEndpoint.APSoutheast2;

        // Assert
        options.DefaultRegion.Should().Be(RegionEndpoint.APSoutheast2);
    }

    [Fact]
    public void DefaultRegion_AcceptsAPNortheast1()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act
        options.DefaultRegion = RegionEndpoint.APNortheast1;

        // Assert
        options.DefaultRegion.Should().Be(RegionEndpoint.APNortheast1);
    }

    [Theory]
    [InlineData(5242880)]
    [InlineData(67108864)]
    [InlineData(134217728)]
    [InlineData(1073741824)]
    public void PartSizeBytes_AcceptsVariousValues(int threshold)
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act
        options.PartSizeBytes = threshold;

        // Assert
        options.PartSizeBytes.Should().Be(threshold);
    }

    [Theory]
    [InlineData("http://localhost:9000")]
    [InlineData("https://s3.amazonaws.com")]
    [InlineData("https://minio.example.com:9000")]
    public void ServiceUrl_AcceptsVariousUrls(string urlString)
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();
        var url = new Uri(urlString);

        // Act
        options.ServiceUrl = url;

        // Assert
        options.ServiceUrl.Should().Be(url);
    }

    [Fact]
    public void DefaultCredentials_WithAWSCredentialsInterface_Works()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();
        AWSCredentials credentials = new BasicAWSCredentials("key", "secret");

        // Act
        options.DefaultCredentials = credentials;

        // Assert
        options.DefaultCredentials.Should().Be(credentials);
    }

    [Fact]
    public void MultipleOptionsInstances_AreIndependent()
    {
        // Arrange
        var options1 = new AwsS3StorageProviderOptions();
        var options2 = new AwsS3StorageProviderOptions();

        // Act
        options1.DefaultRegion = RegionEndpoint.USEast1;
        options1.ForcePathStyle = true;
        options2.DefaultRegion = RegionEndpoint.USWest2;
        options2.ForcePathStyle = false;

        // Assert
        options1.DefaultRegion.Should().Be(RegionEndpoint.USEast1);
        options1.ForcePathStyle.Should().BeTrue();
        options2.DefaultRegion.Should().Be(RegionEndpoint.USWest2);
        options2.ForcePathStyle.Should().BeFalse();
    }

    [Fact]
    public void DefaultPartSizeBytes_Is8MiB()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act & Assert
        options.PartSizeBytes.Should().Be(8 * 1024 * 1024);
    }

    [Fact]
    public void DefaultUseDefaultCredentialChain_IsTrue()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act & Assert
        options.UseDefaultCredentialChain.Should().BeTrue();
    }

    [Fact]
    public void DefaultForcePathStyle_IsFalse()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act & Assert
        options.ForcePathStyle.Should().BeFalse();
    }

    [Fact]
    public void DefaultServiceUrl_IsNull()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act & Assert
        options.ServiceUrl.Should().BeNull();
    }

    [Fact]
    public void DefaultDefaultRegion_IsNull()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act & Assert
        options.DefaultRegion.Should().BeNull();
    }

    [Fact]
    public void DefaultDefaultCredentials_IsNull()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions();

        // Act & Assert
        options.DefaultCredentials.Should().BeNull();
    }

    [Fact]
    public void SettingDefaultRegionToNull_Works()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions
        {
            DefaultRegion = RegionEndpoint.USEast1,
        };

        // Act
        options.DefaultRegion = null;

        // Assert
        options.DefaultRegion.Should().BeNull();
    }

    [Fact]
    public void SettingDefaultCredentialsToNull_Works()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions
        {
            DefaultCredentials = new BasicAWSCredentials("key", "secret"),
        };

        // Act
        options.DefaultCredentials = null;

        // Assert
        options.DefaultCredentials.Should().BeNull();
    }

    [Fact]
    public void SettingServiceUrlToNull_Works()
    {
        // Arrange
        var options = new AwsS3StorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
        };

        // Act
        options.ServiceUrl = null;

        // Assert
        options.ServiceUrl.Should().BeNull();
    }
}

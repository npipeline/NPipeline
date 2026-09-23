using AwesomeAssertions;
using Google.Apis.Auth.OAuth2;
using NPipeline.StorageProviders.Gcp.Reliability;

namespace NPipeline.StorageProviders.Gcp.Tests;

public class GcsStorageProviderOptionsTests
{
    [Fact]
    public void DefaultValues_AreCorrect()
    {
        // Act
        var options = new GcsStorageProviderOptions();

        // Assert
        options.DefaultProjectId.Should().BeNull();
        options.DefaultCredentials.Should().BeNull();
        options.UseDefaultCredentials.Should().BeTrue();
        options.ServiceUrl.Should().BeNull();
        options.UploadChunkSizeBytes.Should().Be(16 * 1024 * 1024); // 16 MB
        options.UploadBufferThresholdBytes.Should().Be(64 * 1024 * 1024); // 64 MB
        options.ClientCacheSizeLimit.Should().Be(100);
        options.Resilience.Should().BeSameAs(GcsStorageResilience.Default);
    }

    [Fact]
    public void DefaultProjectId_CanBeSet()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();
        const string expectedProjectId = "my-project-id";

        // Act
        options.DefaultProjectId = expectedProjectId;

        // Assert
        options.DefaultProjectId.Should().Be(expectedProjectId);
    }

    [Fact]
    public void UseDefaultCredentials_CanBeSet()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();

        // Act
        options.UseDefaultCredentials = false;

        // Assert
        options.UseDefaultCredentials.Should().BeFalse();
    }

    [Fact]
    public void ServiceUrl_CanBeSet()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();
        var expectedUrl = new Uri("http://localhost:4443");

        // Act
        options.ServiceUrl = expectedUrl;

        // Assert
        options.ServiceUrl.Should().Be(expectedUrl);
    }

    [Fact]
    public void UploadChunkSizeBytes_CanBeSet()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();
        const int expectedChunkSize = 32 * 1024 * 1024; // 32 MB

        // Act
        options.UploadChunkSizeBytes = expectedChunkSize;

        // Assert
        options.UploadChunkSizeBytes.Should().Be(expectedChunkSize);
    }

    [Fact]
    public void UploadBufferThresholdBytes_CanBeSet()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();
        const long expectedThreshold = 128 * 1024 * 1024; // 128 MB

        // Act
        options.UploadBufferThresholdBytes = expectedThreshold;

        // Assert
        options.UploadBufferThresholdBytes.Should().Be(expectedThreshold);
    }

    [Fact]
    public void ClientCacheSizeLimit_CanBeSet()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();
        const int expectedLimit = 50;

        // Act
        options.ClientCacheSizeLimit = expectedLimit;

        // Assert
        options.ClientCacheSizeLimit.Should().Be(expectedLimit);
    }

    [Fact]
    public void Resilience_CanBeSet()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();

        // Act
        options.Resilience = NResilience.Resilience.None;

        // Assert
        options.Resilience.Should().BeSameAs(NResilience.Resilience.None);
    }

    [Fact]
    public void Validate_WithNullResilience_Throws()
    {
        var options = new GcsStorageProviderOptions { Resilience = null! };

        _ = Assert.Throws<ArgumentNullException>(options.Validate);
    }

    [Fact]
    public void Validate_WithImpossibleResilience_Throws()
    {
        var options = new GcsStorageProviderOptions
        {
            Resilience = GcsStorageResilience.Default with { Attempts = 0 },
        };

        _ = Assert.Throws<NResilience.ResilienceConfigurationException>(options.Validate);
    }

    [Fact]
    public void Validate_WithValidOptions_DoesNotThrow()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();

        // Act & Assert
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Fact]
    public void Validate_WithZeroUploadChunkSizeBytes_ThrowsInvalidOperationException()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            UploadChunkSizeBytes = 0,
        };

        // Act & Assert
        var exception = options.Invoking(o => o.Validate())
            .Should().Throw<InvalidOperationException>()
            .Which;

        exception.Message.Should().Contain("UploadChunkSizeBytes must be positive");
    }

    [Fact]
    public void Validate_WithNegativeUploadChunkSizeBytes_ThrowsInvalidOperationException()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            UploadChunkSizeBytes = -1,
        };

        // Act & Assert
        var exception = options.Invoking(o => o.Validate())
            .Should().Throw<InvalidOperationException>()
            .Which;

        exception.Message.Should().Contain("UploadChunkSizeBytes must be positive");
    }

    [Theory]
    [InlineData(1)] // Not a multiple of 256 KiB
    [InlineData(1000)]
    [InlineData(262143)] // One less than 256 KiB
    [InlineData(262145)] // One more than 256 KiB
    [InlineData(524287)] // One less than 512 KiB
    public void Validate_WithNonMultipleOf256KiBUploadChunkSizeBytes_ThrowsInvalidOperationException(int chunkSize)
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            UploadChunkSizeBytes = chunkSize,
        };

        // Act & Assert
        var exception = options.Invoking(o => o.Validate())
            .Should().Throw<InvalidOperationException>()
            .Which;

        exception.Message.Should().Contain("UploadChunkSizeBytes must be a multiple of 256 KiB");
        exception.Message.Should().Contain("262144");
    }

    [Theory]
    [InlineData(262144)] // Exactly 256 KiB
    [InlineData(524288)] // 512 KiB
    [InlineData(1048576)] // 1 MiB
    [InlineData(16777216)] // 16 MiB (default)
    [InlineData(33554432)] // 32 MiB
    public void Validate_WithValidUploadChunkSizeBytes_DoesNotThrow(int chunkSize)
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            UploadChunkSizeBytes = chunkSize,
        };

        // Act & Assert
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Fact]
    public void Validate_WithZeroClientCacheSizeLimit_ThrowsInvalidOperationException()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            ClientCacheSizeLimit = 0,
        };

        // Act & Assert
        var exception = options.Invoking(o => o.Validate())
            .Should().Throw<InvalidOperationException>()
            .Which;

        exception.Message.Should().Contain("ClientCacheSizeLimit must be positive");
    }

    [Fact]
    public void Validate_WithNegativeClientCacheSizeLimit_ThrowsInvalidOperationException()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            ClientCacheSizeLimit = -1,
        };

        // Act & Assert
        var exception = options.Invoking(o => o.Validate())
            .Should().Throw<InvalidOperationException>()
            .Which;

        exception.Message.Should().Contain("ClientCacheSizeLimit must be positive");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    public void Validate_WithPositiveClientCacheSizeLimit_DoesNotThrow(int cacheSize)
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            ClientCacheSizeLimit = cacheSize,
        };

        // Act & Assert
        options.Invoking(o => o.Validate()).Should().NotThrow();
    }

    [Fact]
    public void ConfigurationViaAction_UpdatesAllProperties()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();

        // Act
        options.DefaultProjectId = "my-project";
        options.UseDefaultCredentials = false;
        options.ServiceUrl = new Uri("http://localhost:4443");
        options.UploadChunkSizeBytes = 32 * 1024 * 1024;
        options.UploadBufferThresholdBytes = 128 * 1024 * 1024;
        options.ClientCacheSizeLimit = 50;

        // Assert
        options.DefaultProjectId.Should().Be("my-project");
        options.UseDefaultCredentials.Should().BeFalse();
        options.ServiceUrl.Should().Be(new Uri("http://localhost:4443"));
        options.UploadChunkSizeBytes.Should().Be(32 * 1024 * 1024);
        options.UploadBufferThresholdBytes.Should().Be(128 * 1024 * 1024);
        options.ClientCacheSizeLimit.Should().Be(50);
    }

    [Theory]
    [InlineData("http://localhost:4443")]
    [InlineData("https://storage.googleapis.com")]
    [InlineData("http://fake-gcs-server:4443")]
    public void ServiceUrl_AcceptsVariousUrls(string urlString)
    {
        // Arrange
        var options = new GcsStorageProviderOptions();
        var url = new Uri(urlString);

        // Act
        options.ServiceUrl = url;

        // Assert
        options.ServiceUrl.Should().Be(url);
    }

    [Fact]
    public void MultipleOptionsInstances_AreIndependent()
    {
        // Arrange
        var options1 = new GcsStorageProviderOptions();
        var options2 = new GcsStorageProviderOptions();

        // Act
        options1.DefaultProjectId = "project1";
        options1.UploadChunkSizeBytes = 32 * 1024 * 1024;
        options2.DefaultProjectId = "project2";
        options2.UploadChunkSizeBytes = 8 * 1024 * 1024;

        // Assert
        options1.DefaultProjectId.Should().Be("project1");
        options1.UploadChunkSizeBytes.Should().Be(32 * 1024 * 1024);
        options2.DefaultProjectId.Should().Be("project2");
        options2.UploadChunkSizeBytes.Should().Be(8 * 1024 * 1024);
    }

    [Fact]
    public void SettingDefaultProjectIdToNull_Works()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            DefaultProjectId = "my-project",
        };

        // Act
        options.DefaultProjectId = null;

        // Assert
        options.DefaultProjectId.Should().BeNull();
    }

    [Fact]
    public void SettingServiceUrlToNull_Works()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:4443"),
        };

        // Act
        options.ServiceUrl = null;

        // Assert
        options.ServiceUrl.Should().BeNull();
    }

    [Fact]
    public void SettingDefaultCredentialsToNull_Works()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            DefaultCredentials = GoogleCredential.FromAccessToken("test-token"),
        };

        // Act
        options.DefaultCredentials = null;

        // Assert
        options.DefaultCredentials.Should().BeNull();
    }

    [Fact]
    public void DefaultUploadChunkSizeBytes_Is16MB()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();

        // Act & Assert
        options.UploadChunkSizeBytes.Should().Be(16 * 1024 * 1024);
    }

    [Fact]
    public void DefaultUploadBufferThresholdBytes_Is64MB()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();

        // Act & Assert
        options.UploadBufferThresholdBytes.Should().Be(64 * 1024 * 1024);
    }

    [Fact]
    public void DefaultClientCacheSizeLimit_Is100()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();

        // Act & Assert
        options.ClientCacheSizeLimit.Should().Be(100);
    }

    [Fact]
    public void DefaultUseDefaultCredentials_IsTrue()
    {
        // Arrange
        var options = new GcsStorageProviderOptions();

        // Act & Assert
        options.UseDefaultCredentials.Should().BeTrue();
    }
}

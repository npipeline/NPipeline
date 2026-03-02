using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Sftp.Tests;

/// <summary>
///     Unit tests for <see cref="SftpStorageProvider" />.
/// </summary>
public class SftpStorageProviderTests
{
    [Fact]
    public void Scheme_ShouldReturnSftp()
    {
        // Arrange
        var options = new SftpStorageProviderOptions();
        var factory = new SftpClientFactory(options);
        var provider = new SftpStorageProvider(factory, options);

        // Act & Assert
        provider.Scheme.Should().Be(StorageScheme.Sftp);
    }

    [Fact]
    public void CanHandle_WithSftpScheme_ShouldReturnTrue()
    {
        // Arrange
        var options = new SftpStorageProviderOptions();
        var factory = new SftpClientFactory(options);
        var provider = new SftpStorageProvider(factory, options);
        var uri = StorageUri.Parse("sftp://example.com/path/file.txt");

        // Act
        var result = provider.CanHandle(uri);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void CanHandle_WithDifferentScheme_ShouldReturnFalse()
    {
        // Arrange
        var options = new SftpStorageProviderOptions();
        var factory = new SftpClientFactory(options);
        var provider = new SftpStorageProvider(factory, options);
        var uri = StorageUri.Parse("file:///path/file.txt");

        // Act
        var result = provider.CanHandle(uri);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void CanHandle_WithNullUri_ShouldThrowArgumentNullException()
    {
        // Arrange
        var options = new SftpStorageProviderOptions();
        var factory = new SftpClientFactory(options);
        var provider = new SftpStorageProvider(factory, options);

        // Act
        var act = () => provider.CanHandle(null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetMetadata_ShouldReturnCorrectMetadata()
    {
        // Arrange
        var options = new SftpStorageProviderOptions
        {
            MaxPoolSize = 20,
            ConnectionIdleTimeout = TimeSpan.FromMinutes(10),
            KeepAliveInterval = TimeSpan.FromSeconds(15),
            ConnectionTimeout = TimeSpan.FromSeconds(60),
        };

        var factory = new SftpClientFactory(options);
        var provider = new SftpStorageProvider(factory, options);

        // Act
        var metadata = provider.GetMetadata();

        // Assert
        metadata.Name.Should().Be("SFTP");
        metadata.SupportedSchemes.Should().Contain("sftp");
        metadata.SupportsRead.Should().BeTrue();
        metadata.SupportsWrite.Should().BeTrue();
        metadata.SupportsListing.Should().BeTrue();
        metadata.SupportsMetadata.Should().BeTrue();
        metadata.SupportsHierarchy.Should().BeTrue();
        metadata.Capabilities.Should().ContainKey("maxPoolSize");
        metadata.Capabilities["maxPoolSize"].Should().Be(20);
    }

    [Fact]
    public void OpenReadAsync_WithNullUri_ShouldThrowArgumentNullException()
    {
        // Arrange
        var options = new SftpStorageProviderOptions();
        var factory = new SftpClientFactory(options);
        var provider = new SftpStorageProvider(factory, options);

        // Act
        var act = async () => await provider.OpenReadAsync(null!);

        // Assert
        act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public void OpenWriteAsync_WithNullUri_ShouldThrowArgumentNullException()
    {
        // Arrange
        var options = new SftpStorageProviderOptions();
        var factory = new SftpClientFactory(options);
        var provider = new SftpStorageProvider(factory, options);

        // Act
        var act = async () => await provider.OpenWriteAsync(null!);

        // Assert
        act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public void ExistsAsync_WithNullUri_ShouldThrowArgumentNullException()
    {
        // Arrange
        var options = new SftpStorageProviderOptions();
        var factory = new SftpClientFactory(options);
        var provider = new SftpStorageProvider(factory, options);

        // Act
        var act = async () => await provider.ExistsAsync(null!);

        // Assert
        act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public void GetMetadataAsync_WithNullUri_ShouldThrowArgumentNullException()
    {
        // Arrange
        var options = new SftpStorageProviderOptions();
        var factory = new SftpClientFactory(options);
        var provider = new SftpStorageProvider(factory, options);

        // Act
        var act = async () => await provider.GetMetadataAsync(null!);

        // Assert
        act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public void ListAsync_WithNullUri_ShouldThrowArgumentNullException()
    {
        // Arrange
        var options = new SftpStorageProviderOptions();
        var factory = new SftpClientFactory(options);
        var provider = new SftpStorageProvider(factory, options);

        // Act
        var act = async () =>
        {
            await foreach (var _ in provider.ListAsync(null!))
            {
            }
        };

        // Assert
        act.Should().ThrowAsync<ArgumentNullException>();
    }
}

using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Sftp.Tests;

/// <summary>
///     Unit tests for <see cref="SftpStorageProvider" />.
/// </summary>
public class SftpStorageProviderTests
{
    private static SftpStorageProvider CreateProvider()
    {
        var options = new SftpStorageProviderOptions();
        return new SftpStorageProvider(new SftpClientFactory(options), options);
    }

    [Fact]
    public void Schemes_ShouldContainOnlySftp()
    {
        CreateProvider().Schemes.Should().Equal(StorageScheme.Sftp);
    }

    [Fact]
    public void Capabilities_DeclareHierarchyAndAtomicMoveButNotConditionalWrite()
    {
        CreateProvider().Capabilities.Should().Be(
            StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete
            | StorageCapabilities.Move | StorageCapabilities.AtomicMove | StorageCapabilities.Hierarchy);
    }

    [Fact]
    public async Task DeleteAsync_WithNullUri_ShouldThrowArgumentNullException()
    {
        var act = async () => await CreateProvider().DeleteAsync(null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task MoveAsync_WithNullDestination_ShouldThrowArgumentNullException()
    {
        var act = async () => await CreateProvider().MoveAsync(StorageUri.Parse("sftp://h/a"), null!);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task OpenWriteAsync_WithConditionalOptions_ShouldThrowUnsupportedCapability()
    {
        var act = async () => await CreateProvider().OpenWriteAsync(StorageUri.Parse("sftp://h/a"), new StorageWriteOptions { Overwrite = false });

        await act.Should().ThrowAsync<UnsupportedStorageCapabilityException>();
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

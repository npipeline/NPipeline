using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Sftp.Tests;

/// <summary>
///     Unit tests for <see cref="SftpStorageProviderOptions" />.
/// </summary>
public class SftpStorageProviderOptionsTests
{
    [Fact]
    public void Constructor_ShouldSetDefaultValues()
    {
        // Arrange & Act
        var options = new SftpStorageProviderOptions();

        // Assert
        options.DefaultPort.Should().Be(22);
        options.MaxPoolSize.Should().Be(10);
        options.ConnectionIdleTimeout.Should().Be(TimeSpan.FromMinutes(5));
        options.KeepAliveInterval.Should().Be(TimeSpan.FromSeconds(30));
        options.ConnectionTimeout.Should().Be(TimeSpan.FromSeconds(30));
        options.AcceptAnyHostKey.Should().BeFalse();
        options.ValidateOnAcquire.Should().BeTrue();
        options.DefaultHost.Should().BeNull();
        options.DefaultUsername.Should().BeNull();
        options.DefaultPassword.Should().BeNull();
        options.DefaultKeyPath.Should().BeNull();
        options.DefaultKeyPassphrase.Should().BeNull();
        options.HostKeyFingerprints.Should().BeEmpty();
    }

    [Fact]
    public void Properties_ShouldBeSettable()
    {
        // Arrange
        var options = new SftpStorageProviderOptions();

        // Act
        options.DefaultHost = "sftp.example.com";
        options.DefaultPort = 2222;
        options.DefaultUsername = "testuser";
        options.DefaultPassword = "testpass";
        options.DefaultKeyPath = "/path/to/key";
        options.DefaultKeyPassphrase = "passphrase";
        options.MaxPoolSize = 20;
        options.ConnectionIdleTimeout = TimeSpan.FromMinutes(10);
        options.KeepAliveInterval = TimeSpan.FromSeconds(15);
        options.ConnectionTimeout = TimeSpan.FromSeconds(60);
        options.AcceptAnyHostKey = true;
        options.HostKeyFingerprints = ["SHA256:abc123"];
        options.ValidateOnAcquire = false;

        // Assert
        options.DefaultHost.Should().Be("sftp.example.com");
        options.DefaultPort.Should().Be(2222);
        options.DefaultUsername.Should().Be("testuser");
        options.DefaultPassword.Should().Be("testpass");
        options.DefaultKeyPath.Should().Be("/path/to/key");
        options.DefaultKeyPassphrase.Should().Be("passphrase");
        options.MaxPoolSize.Should().Be(20);
        options.ConnectionIdleTimeout.Should().Be(TimeSpan.FromMinutes(10));
        options.KeepAliveInterval.Should().Be(TimeSpan.FromSeconds(15));
        options.ConnectionTimeout.Should().Be(TimeSpan.FromSeconds(60));
        options.AcceptAnyHostKey.Should().BeTrue();
        options.HostKeyFingerprints.Should().Equal("SHA256:abc123");
        options.ValidateOnAcquire.Should().BeFalse();
    }

    [Fact]
    public async Task Connect_NoFingerprintAndNotAcceptAny_Throws()
    {
        using var factory = new SftpClientFactory(new SftpStorageProviderOptions());

        var act = () => factory.CreateClientAsync(StorageUri.Parse("sftp://user:pass@127.0.0.1:2/file"), CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*HostKeyFingerprints*");
    }

    [Theory]
    [InlineData("password=x", "DefaultPassword")]
    [InlineData("keyPassphrase=x&keyPath=/k", "DefaultKeyPassphrase")]
    public async Task Connect_SecretInUriParameter_ThrowsAndNamesTheOption(string query, string option)
    {
        using var factory = new SftpClientFactory(new SftpStorageProviderOptions { AcceptAnyHostKey = true });

        var act = () => factory.CreateClientAsync(StorageUri.Parse($"sftp://user@127.0.0.1:2/file?{query}"), CancellationToken.None);

        await act.Should().ThrowAsync<ArgumentException>().WithMessage($"*{option}*");
    }

    [Theory]
    [InlineData("ohD8VZEXGWo6Ez8GSEJQ9WpafgLFsOfLOtGGQCQo6Og", true)]
    [InlineData("SHA256:ohD8VZEXGWo6Ez8GSEJQ9WpafgLFsOfLOtGGQCQo6Og", true)]
    [InlineData("sha256:ohD8VZEXGWo6Ez8GSEJQ9WpafgLFsOfLOtGGQCQo6Og=", true)]
    [InlineData("OHD8VZEXGWO6EZ8GSEJQ9WPAFGLFSOFLOTGGQCQO6OG", false)] // base64 is case-sensitive
    [InlineData("SHA256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false)]
    public void HostKeyVerifier_ComparesFingerprints(string configured, bool trusted)
    {
        HostKeyVerifier.IsTrusted("ohD8VZEXGWo6Ez8GSEJQ9WpafgLFsOfLOtGGQCQo6Og", [configured]).Should().Be(trusted);
    }

    [Fact]
    public void HostKeyVerifier_NoPresentedFingerprint_IsNotTrusted()
    {
        HostKeyVerifier.IsTrusted(null, ["SHA256:anything"]).Should().BeFalse();
    }
}

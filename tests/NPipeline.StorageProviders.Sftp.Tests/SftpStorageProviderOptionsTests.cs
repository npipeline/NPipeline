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
        options.ValidateServerFingerprint.Should().BeTrue();
        options.ValidateOnAcquire.Should().BeTrue();
        options.DefaultHost.Should().BeNull();
        options.DefaultUsername.Should().BeNull();
        options.DefaultPassword.Should().BeNull();
        options.DefaultKeyPath.Should().BeNull();
        options.DefaultKeyPassphrase.Should().BeNull();
        options.ExpectedFingerprint.Should().BeNull();
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
        options.ValidateServerFingerprint = false;
        options.ExpectedFingerprint = "abc123";
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
        options.ValidateServerFingerprint.Should().BeFalse();
        options.ExpectedFingerprint.Should().Be("abc123");
        options.ValidateOnAcquire.Should().BeFalse();
    }
}

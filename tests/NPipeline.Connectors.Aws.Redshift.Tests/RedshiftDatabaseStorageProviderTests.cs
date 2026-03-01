using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Aws.Redshift.Tests;

public class RedshiftDatabaseStorageProviderTests
{
    private readonly RedshiftDatabaseStorageProvider _provider = new();

    [Fact]
    public void Scheme_ShouldReturnRedshift()
    {
        // Act
        var scheme = _provider.Scheme;

        // Assert
        scheme.ToString().Should().Be("redshift");
    }

    [Fact]
    public void CanHandle_WhenRedshiftScheme_ShouldReturnTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://cluster.example.com/mydb");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void CanHandle_WhenOtherScheme_ShouldReturnFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://cluster.example.com/mydb");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void CanHandle_WhenNullUri_ShouldThrowArgumentNullException()
    {
        // Act
        var act = () => _provider.CanHandle(null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetConnectionString_WithMinimalUri_ShouldContainRequiredComponents()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://my-cluster.redshift.amazonaws.com/analytics");

        // Act
        var connectionString = _provider.GetConnectionString(uri);

        // Assert
        connectionString.Should().Contain("Host=my-cluster.redshift.amazonaws.com");
        connectionString.Should().Contain("Database=analytics");
        connectionString.Should().Contain("Port=5439");
        connectionString.Should().Contain("SSL Mode=Require");
    }

    [Fact]
    public void GetConnectionString_WithCredentials_ShouldContainUsernameAndPassword()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://admin:secret123@my-cluster.redshift.amazonaws.com/analytics");

        // Act
        var connectionString = _provider.GetConnectionString(uri);

        // Assert
        connectionString.Should().Contain("Username=admin");
        connectionString.Should().Contain("Password=secret123");
    }

    [Fact]
    public void GetConnectionString_WithCustomPort_ShouldUseSpecifiedPort()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://my-cluster.redshift.amazonaws.com:5440/analytics");

        // Act
        var connectionString = _provider.GetConnectionString(uri);

        // Assert
        connectionString.Should().Contain("Port=5440");
    }

    [Fact]
    public void GetConnectionString_WithQueryParameters_ShouldIncludeAdditionalParameters()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://my-cluster.redshift.amazonaws.com/analytics?Timeout=60&Pooling=true");

        // Act
        var connectionString = _provider.GetConnectionString(uri);

        // Assert
        connectionString.Should().Contain("Timeout=60");

        // NpgsqlConnectionStringBuilder normalises boolean values to title-case
        connectionString.Should().Contain("Pooling=True");
    }

    [Fact]
    public void GetConnectionString_WhenNullUri_ShouldThrowArgumentNullException()
    {
        // Act
        var act = () => _provider.GetConnectionString(null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GetConnectionString_WhenMissingHost_ShouldThrowArgumentException()
    {
        // Arrange - using file scheme which has no host
        var uri = StorageUri.FromFilePath("/some/path");

        // Act
        var act = () => _provider.GetConnectionString(uri);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*host*");
    }

    [Fact]
    public void GetConnectionString_WhenMissingDatabase_ShouldThrowArgumentException()
    {
        // Arrange - path without database
        var uri = StorageUri.Parse("redshift://my-cluster.redshift.amazonaws.com/");

        // Act
        var act = () => _provider.GetConnectionString(uri);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*database*");
    }

    [Fact]
    public void CreateConfiguration_WithMinimalUri_ShouldSetRequiredProperties()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://my-cluster.redshift.amazonaws.com/analytics");

        // Act
        var config = _provider.CreateConfiguration(uri);

        // Assert
        config.Host.Should().Be("my-cluster.redshift.amazonaws.com");
        config.Database.Should().Be("analytics");
        config.Port.Should().Be(5439);
    }

    [Fact]
    public void CreateConfiguration_WithCredentials_ShouldSetUsernameAndPassword()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://admin:secret123@my-cluster.redshift.amazonaws.com/analytics");

        // Act
        var config = _provider.CreateConfiguration(uri);

        // Assert
        config.Username.Should().Be("admin");
        config.Password.Should().Be("secret123");
    }

    [Fact]
    public void CreateConfiguration_WithSchemaParameter_ShouldSetSchema()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://my-cluster.redshift.amazonaws.com/analytics?schema=etl");

        // Act
        var config = _provider.CreateConfiguration(uri);

        // Assert
        config.Schema.Should().Be("etl");
    }

    [Fact]
    public void CreateConfiguration_WithTimeoutParameter_ShouldSetCommandTimeout()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://my-cluster.redshift.amazonaws.com/analytics?timeout=600");

        // Act
        var config = _provider.CreateConfiguration(uri);

        // Assert
        config.CommandTimeout.Should().Be(600);
    }

    [Fact]
    public void CreateConfiguration_WithFetchSizeParameter_ShouldSetFetchSize()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://my-cluster.redshift.amazonaws.com/analytics?fetchSize=5000");

        // Act
        var config = _provider.CreateConfiguration(uri);

        // Assert
        config.FetchSize.Should().Be(5000);
    }

    [Fact]
    public void OpenReadAsync_ShouldThrowNotSupportedException()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://my-cluster.redshift.amazonaws.com/analytics");

        // Act - The method throws synchronously so we need to catch it
        NotSupportedException? exception = null;

        try
        {
            _ = _provider.OpenReadAsync(uri);
        }
        catch (NotSupportedException ex)
        {
            exception = ex;
        }

        // Assert
        Assert.NotNull(exception);
        Assert.Contains("OpenReadAsync", exception.Message);
    }

    [Fact]
    public void OpenWriteAsync_ShouldThrowNotSupportedException()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://my-cluster.redshift.amazonaws.com/analytics");

        // Act - The method throws synchronously so we need to catch it
        NotSupportedException? exception = null;

        try
        {
            _ = _provider.OpenWriteAsync(uri);
        }
        catch (NotSupportedException ex)
        {
            exception = ex;
        }

        // Assert
        Assert.NotNull(exception);
        Assert.Contains("OpenWriteAsync", exception.Message);
    }

    [Fact]
    public void ExistsAsync_ShouldThrowNotSupportedException()
    {
        // Arrange
        var uri = StorageUri.Parse("redshift://my-cluster.redshift.amazonaws.com/analytics");

        // Act - The method throws synchronously so we need to catch it
        NotSupportedException? exception = null;

        try
        {
            _ = _provider.ExistsAsync(uri);
        }
        catch (NotSupportedException ex)
        {
            exception = ex;
        }

        // Assert
        Assert.NotNull(exception);
        Assert.Contains("ExistsAsync", exception.Message);
    }

    [Fact]
    public void GetMetadata_ShouldReturnCorrectMetadata()
    {
        // Act
        var metadata = _provider.GetMetadata();

        // Assert
        metadata.Name.Should().Be("AWS Redshift");
        metadata.SupportedSchemes.Should().Contain("redshift");
        metadata.SupportsRead.Should().BeFalse();
        metadata.SupportsWrite.Should().BeFalse();
        metadata.SupportsListing.Should().BeFalse();
        metadata.SupportsMetadata.Should().BeFalse();
        metadata.SupportsHierarchy.Should().BeFalse();
    }
}

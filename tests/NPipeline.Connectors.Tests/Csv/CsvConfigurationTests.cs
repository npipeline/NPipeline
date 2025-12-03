using System.Globalization;
using AwesomeAssertions;
using NPipeline.Connectors.Csv;

namespace NPipeline.Connectors.Tests.Csv;

public sealed class CsvConfigurationTests
{
    [Fact]
    public void BufferSize_ShouldDefaultTo1024()
    {
        // Arrange & Act
        var config = new CsvConfiguration();

        // Assert
        config.BufferSize.Should().Be(1024);
    }

    [Fact]
    public void BufferSize_ShouldUseCustomValue()
    {
        // Arrange
        var customBufferSize = 2048;

        // Act
        var config = new CsvConfiguration
        {
            BufferSize = customBufferSize,
        };

        // Assert
        config.BufferSize.Should().Be(customBufferSize);
    }

    [Fact]
    public void BufferSize_ShouldUseCustomValueWithCultureInfo()
    {
        // Arrange
        var cultureInfo = new CultureInfo("en-US");
        var customBufferSize = 512;

        // Act
        var config = new CsvConfiguration(cultureInfo)
        {
            BufferSize = customBufferSize,
        };

        // Assert
        config.BufferSize.Should().Be(customBufferSize);
        config.HelperConfiguration.CultureInfo.Should().Be(cultureInfo);
    }

    [Fact]
    public void Constructor_WithCultureInfo_ShouldSetHelperConfiguration()
    {
        // Arrange
        var cultureInfo = new CultureInfo("fr-FR");

        // Act
        var config = new CsvConfiguration(cultureInfo);

        // Assert
        config.HelperConfiguration.Should().NotBeNull();
        config.HelperConfiguration.CultureInfo.Should().Be(cultureInfo);
        config.BufferSize.Should().Be(1024); // Default value
    }

    [Fact]
    public void ImplicitConversion_ShouldReturnHelperConfiguration()
    {
        // Arrange
        var config = new CsvConfiguration();

        // Act
        CsvHelper.Configuration.CsvConfiguration helperConfig = config;

        // Assert
        helperConfig.Should().Be(config.HelperConfiguration);
    }
}

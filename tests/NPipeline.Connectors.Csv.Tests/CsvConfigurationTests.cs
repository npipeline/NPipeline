using System.Globalization;
using AwesomeAssertions;
using NPipeline.Connectors.Csv;

namespace NPipeline.Connectors.Csv.Tests;

public sealed class CsvConfigurationTests
{
    [Fact]
    public void BufferSize_ShouldDefaultTo4096()
    {
        // Arrange & Act
        var config = new CsvConfiguration();

        // Assert
        config.BufferSize.Should().Be(4096);
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
        config.BufferSize.Should().Be(4096); // Default value
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

    [Fact]
    public void HasHeaderRecord_ShouldMirrorHelperConfiguration()
    {
        // Arrange
        var config = new CsvConfiguration();

        // Act
        config.HasHeaderRecord = false;

        // Assert
        config.HasHeaderRecord.Should().BeFalse();
        config.HelperConfiguration.HasHeaderRecord.Should().BeFalse();
    }

    [Fact]
    public void HeaderValidated_ShouldHaveDefaultValue()
    {
        // Arrange & Act
        var config = new CsvConfiguration();

        // Assert
        // HeaderValidated has a default delegate from CsvHelper
        config.HeaderValidated.Should().NotBeNull();
        config.HelperConfiguration.HeaderValidated.Should().NotBeNull();
        config.HeaderValidated.Should().Be(config.HelperConfiguration.HeaderValidated);
    }

    [Fact]
    public void RowErrorHandler_ShouldBeConfigurable()
    {
        // Arrange
        var config = new CsvConfiguration();
        Func<Exception, CsvRow, bool> handler = (_, _) => true;

        // Act
        config.RowErrorHandler = handler;

        // Assert
        config.RowErrorHandler.Should().Be(handler);
    }
}

using System.Text;
using AwesomeAssertions;
using NPipeline.Connectors.Excel;

namespace NPipeline.Connectors.Tests.Excel;

public sealed class ExcelConfigurationTests
{
    [Fact]
    public void BufferSize_ShouldDefaultTo4096()
    {
        // Arrange & Act
        var config = new ExcelConfiguration();

        // Assert
        config.BufferSize.Should().Be(4096);
    }

    [Fact]
    public void BufferSize_ShouldUseCustomValue()
    {
        // Arrange
        var customBufferSize = 2048;

        // Act
        var config = new ExcelConfiguration
        {
            BufferSize = customBufferSize,
        };

        // Assert
        config.BufferSize.Should().Be(customBufferSize);
    }

    [Fact]
    public void SheetName_ShouldDefaultToNull()
    {
        // Arrange & Act
        var config = new ExcelConfiguration();

        // Assert
        config.SheetName.Should().BeNull();
    }

    [Fact]
    public void SheetName_ShouldUseCustomValue()
    {
        // Arrange
        var customSheetName = "CustomSheet";

        // Act
        var config = new ExcelConfiguration
        {
            SheetName = customSheetName,
        };

        // Assert
        config.SheetName.Should().Be(customSheetName);
    }

    [Fact]
    public void FirstRowIsHeader_ShouldDefaultToTrue()
    {
        // Arrange & Act
        var config = new ExcelConfiguration();

        // Assert
        config.FirstRowIsHeader.Should().BeTrue();
    }

    [Fact]
    public void FirstRowIsHeader_ShouldUseCustomValue()
    {
        // Arrange & Act
        var config = new ExcelConfiguration
        {
            FirstRowIsHeader = false,
        };

        // Assert
        config.FirstRowIsHeader.Should().BeFalse();
    }

    [Fact]
    public void Encoding_ShouldDefaultToNull()
    {
        // Arrange & Act
        var config = new ExcelConfiguration();

        // Assert
        config.Encoding.Should().BeNull();
    }

    [Fact]
    public void Encoding_ShouldUseCustomValue()
    {
        // Arrange
        var customEncoding = Encoding.UTF8;

        // Act
        var config = new ExcelConfiguration
        {
            Encoding = customEncoding,
        };

        // Assert
        config.Encoding.Should().Be(customEncoding);
    }

    [Fact]
    public void AutodetectSeparators_ShouldDefaultToTrue()
    {
        // Arrange & Act
        var config = new ExcelConfiguration();

        // Assert
        config.AutodetectSeparators.Should().BeTrue();
    }

    [Fact]
    public void AutodetectSeparators_ShouldUseCustomValue()
    {
        // Arrange & Act
        var config = new ExcelConfiguration
        {
            AutodetectSeparators = false,
        };

        // Assert
        config.AutodetectSeparators.Should().BeFalse();
    }

    [Fact]
    public void AnalyzeAllColumns_ShouldDefaultToFalse()
    {
        // Arrange & Act
        var config = new ExcelConfiguration();

        // Assert
        config.AnalyzeAllColumns.Should().BeFalse();
    }

    [Fact]
    public void AnalyzeAllColumns_ShouldUseCustomValue()
    {
        // Arrange & Act
        var config = new ExcelConfiguration
        {
            AnalyzeAllColumns = true,
        };

        // Assert
        config.AnalyzeAllColumns.Should().BeTrue();
    }

    [Fact]
    public void AnalyzeInitialRowCount_ShouldDefaultTo30()
    {
        // Arrange & Act
        var config = new ExcelConfiguration();

        // Assert
        config.AnalyzeInitialRowCount.Should().Be(30);
    }

    [Fact]
    public void AnalyzeInitialRowCount_ShouldUseCustomValue()
    {
        // Arrange
        var customRowCount = 50;

        // Act
        var config = new ExcelConfiguration
        {
            AnalyzeInitialRowCount = customRowCount,
        };

        // Assert
        config.AnalyzeInitialRowCount.Should().Be(customRowCount);
    }

    [Fact]
    public void HasHeaderRow_ShouldReturnFirstRowIsHeaderValue()
    {
        // Arrange
        var config = new ExcelConfiguration
        {
            FirstRowIsHeader = false,
        };

        // Act
        var hasHeaderRow = config.HasHeaderRow;

        // Assert
        hasHeaderRow.Should().BeFalse();
    }

    [Fact]
    public void HasHeaderRow_ShouldSetFirstRowIsHeaderValue()
    {
        // Arrange
        var config = new ExcelConfiguration();

        // Act
        config.HasHeaderRow = false;

        // Assert
        config.FirstRowIsHeader.Should().BeFalse();
    }

    [Fact]
    public void Constructor_ShouldSetAllDefaultValues()
    {
        // Arrange & Act
        var config = new ExcelConfiguration();

        // Assert
        config.BufferSize.Should().Be(4096);
        config.SheetName.Should().BeNull();
        config.FirstRowIsHeader.Should().BeTrue();
        config.Encoding.Should().BeNull();
        config.AutodetectSeparators.Should().BeTrue();
        config.AnalyzeAllColumns.Should().BeFalse();
        config.AnalyzeInitialRowCount.Should().Be(30);
    }

    [Fact]
    public void Configuration_ShouldAllowSettingAllProperties()
    {
        // Arrange
        var encoding = Encoding.ASCII;

        // Act
        var config = new ExcelConfiguration
        {
            BufferSize = 8192,
            SheetName = "TestSheet",
            FirstRowIsHeader = false,
            Encoding = encoding,
            AutodetectSeparators = false,
            AnalyzeAllColumns = true,
            AnalyzeInitialRowCount = 100,
        };

        // Assert
        config.BufferSize.Should().Be(8192);
        config.SheetName.Should().Be("TestSheet");
        config.FirstRowIsHeader.Should().BeFalse();
        config.Encoding.Should().Be(encoding);
        config.AutodetectSeparators.Should().BeFalse();
        config.AnalyzeAllColumns.Should().BeTrue();
        config.AnalyzeInitialRowCount.Should().Be(100);
    }
}

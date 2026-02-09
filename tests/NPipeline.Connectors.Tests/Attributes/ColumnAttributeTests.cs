using AwesomeAssertions;
using NPipeline.Connectors.Attributes;
using Xunit;

namespace NPipeline.Connectors.Tests.Attributes;

public class ColumnAttributeTests
{
    [Fact]
    public void Constructor_WithValidName_SetsNameProperty()
    {
        // Arrange
        var columnName = "TestColumn";

        // Act
        var attribute = new ColumnAttribute(columnName);

        // Assert
        attribute.Name.Should().Be(columnName);
    }

    [Fact]
    public void Constructor_WithEmptyName_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => new ColumnAttribute(string.Empty));
        exception.ParamName.Should().Be("name");
        exception.Message.Should().Contain("Column name cannot be empty");
    }

    [Fact]
    public void Constructor_WithWhitespaceName_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => new ColumnAttribute("   "));
        exception.ParamName.Should().Be("name");
        exception.Message.Should().Contain("Column name cannot be empty");
    }

    [Fact]
    public void Constructor_WithNullName_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => new ColumnAttribute(null!));
        exception.ParamName.Should().Be("name");
        exception.Message.Should().Contain("Column name cannot be empty");
    }

    [Fact]
    public void IgnoreProperty_DefaultValue_IsFalse()
    {
        // Arrange
        var attribute = new ColumnAttribute("TestColumn");

        // Act & Assert
        attribute.Ignore.Should().BeFalse();
    }

    [Fact]
    public void IgnoreProperty_CanBeSetToTrue()
    {
        // Arrange
        var attribute = new ColumnAttribute("TestColumn");

        // Act
        attribute.Ignore = true;

        // Assert
        attribute.Ignore.Should().BeTrue();
    }

    [Fact]
    public void IgnoreProperty_CanBeSetToFalse()
    {
        // Arrange
        var attribute = new ColumnAttribute("TestColumn")
        {
            Ignore = true,
        };

        // Act
        attribute.Ignore = false;

        // Assert
        attribute.Ignore.Should().BeFalse();
    }

    [Fact]
    public void AttributeUsage_AllowsPropertyTarget()
    {
        // Arrange
        var attributeType = typeof(ColumnAttribute);
        var attributeUsage = (AttributeUsageAttribute)attributeType.GetCustomAttributes(typeof(AttributeUsageAttribute), false).First();

        // Act & Assert
        attributeUsage.ValidOn.Should().HaveFlag(AttributeTargets.Property);
    }

    [Fact]
    public void AttributeUsage_AllowsFieldTarget()
    {
        // Arrange
        var attributeType = typeof(ColumnAttribute);
        var attributeUsage = (AttributeUsageAttribute)attributeType.GetCustomAttributes(typeof(AttributeUsageAttribute), false).First();

        // Act & Assert
        attributeUsage.ValidOn.Should().HaveFlag(AttributeTargets.Field);
    }

    [Theory]
    [InlineData("ColumnName")]
    [InlineData("column_name")]
    [InlineData("COLUMN_NAME")]
    [InlineData("Column123")]
    [InlineData("Column_Name_123")]
    public void Constructor_WithVariousValidNames_SetsNameProperty(string columnName)
    {
        // Act
        var attribute = new ColumnAttribute(columnName);

        // Assert
        attribute.Name.Should().Be(columnName);
    }

    [Fact]
    public void MultipleProperties_CanHaveDifferentColumnNames()
    {
        // Arrange
        var attr1 = new ColumnAttribute("Column1");
        var attr2 = new ColumnAttribute("Column2");

        // Act & Assert
        attr1.Name.Should().Be("Column1");
        attr2.Name.Should().Be("Column2");
        attr1.Name.Should().NotBe(attr2.Name);
    }
}

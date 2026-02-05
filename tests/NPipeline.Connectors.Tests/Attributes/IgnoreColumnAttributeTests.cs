using FluentAssertions;
using NPipeline.Connectors.Attributes;
using Xunit;

namespace NPipeline.Connectors.Tests.Attributes;

public class IgnoreColumnAttributeTests
{
    [Fact]
    public void Constructor_CreatesInstanceSuccessfully()
    {
        // Act
        var attribute = new IgnoreColumnAttribute();

        // Assert
        attribute.Should().NotBeNull();
    }

    [Fact]
    public void AttributeUsage_AllowsPropertyTarget()
    {
        // Arrange
        var attributeType = typeof(IgnoreColumnAttribute);
        var attributeUsage = (AttributeUsageAttribute)attributeType.GetCustomAttributes(typeof(AttributeUsageAttribute), false).First();

        // Act & Assert
        attributeUsage.ValidOn.Should().HaveFlag(AttributeTargets.Property);
    }

    [Fact]
    public void AttributeUsage_DoesNotAllowFieldTarget()
    {
        // Arrange
        var attributeType = typeof(IgnoreColumnAttribute);
        var attributeUsage = (AttributeUsageAttribute)attributeType.GetCustomAttributes(typeof(AttributeUsageAttribute), false).First();

        // Act & Assert
        attributeUsage.ValidOn.Should().NotHaveFlag(AttributeTargets.Field);
    }

    [Fact]
    public void AttributeUsage_DoesNotAllowClassTarget()
    {
        // Arrange
        var attributeType = typeof(IgnoreColumnAttribute);
        var attributeUsage = (AttributeUsageAttribute)attributeType.GetCustomAttributes(typeof(AttributeUsageAttribute), false).First();

        // Act & Assert
        attributeUsage.ValidOn.Should().NotHaveFlag(AttributeTargets.Class);
    }

    [Fact]
    public void MultipleInstances_CanBeCreated()
    {
        // Arrange & Act
        var attr1 = new IgnoreColumnAttribute();
        var attr2 = new IgnoreColumnAttribute();

        // Assert
        attr1.Should().NotBeNull();
        attr2.Should().NotBeNull();
        attr1.Should().NotBeSameAs(attr2);
    }

    [Fact]
    public void AttributeUsage_AllowMultiple_IsFalse()
    {
        // Arrange
        var attributeType = typeof(IgnoreColumnAttribute);
        var attributeUsage = (AttributeUsageAttribute)attributeType.GetCustomAttributes(typeof(AttributeUsageAttribute), false).First();

        // Act & Assert
        attributeUsage.AllowMultiple.Should().BeFalse();
    }

    [Fact]
    public void AttributeUsage_Inherited_IsTrue()
    {
        // Arrange
        var attributeType = typeof(IgnoreColumnAttribute);
        var attributeUsage = (AttributeUsageAttribute)attributeType.GetCustomAttributes(typeof(AttributeUsageAttribute), false).First();

        // Act & Assert
        attributeUsage.Inherited.Should().BeTrue();
    }
}

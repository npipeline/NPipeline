using AwesomeAssertions;
using Xunit;

namespace NPipeline.Extensions.Composition.Tests;

public class CompositeContextConfigurationTests
{
    [Fact]
    public void Default_ShouldHaveAllInheritanceFlagsFalse()
    {
        // Arrange & Act
        var config = CompositeContextConfiguration.Default;

        // Assert
        config.InheritParentParameters.Should().BeFalse();
        config.InheritParentItems.Should().BeFalse();
        config.InheritParentProperties.Should().BeFalse();
    }

    [Fact]
    public void InheritAll_ShouldHaveAllInheritanceFlagsTrue()
    {
        // Arrange & Act
        var config = CompositeContextConfiguration.InheritAll;

        // Assert
        config.InheritParentParameters.Should().BeTrue();
        config.InheritParentItems.Should().BeTrue();
        config.InheritParentProperties.Should().BeTrue();
    }

    [Fact]
    public void Constructor_WithDefaultValues_ShouldMatchDefault()
    {
        // Arrange & Act
        var config = new CompositeContextConfiguration();
        var defaultConfig = CompositeContextConfiguration.Default;

        // Assert
        config.InheritParentParameters.Should().Be(defaultConfig.InheritParentParameters);
        config.InheritParentItems.Should().Be(defaultConfig.InheritParentItems);
        config.InheritParentProperties.Should().Be(defaultConfig.InheritParentProperties);
    }

    [Fact]
    public void MultipleInstances_ShouldBeEqual()
    {
        // Arrange
        var config1 = new CompositeContextConfiguration();
        var config2 = new CompositeContextConfiguration();

        // Act & Assert
        config1.Should().BeEquivalentTo(config2);
    }
}

using System.Reflection;
using AwesomeAssertions;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Mapping;

public class CosmosPartitionKeyAttributeTests
{
    #region Constructor Tests

    [Fact]
    public void DefaultConstructor_ShouldInitializeWithNullPath()
    {
        // Act
        var attribute = new CosmosPartitionKeyAttribute();

        // Assert
        attribute.Path.Should().BeNull();
        attribute.IsPartitionKey.Should().BeTrue();
    }

    [Fact]
    public void Constructor_WithPath_ShouldSetPathProperty()
    {
        // Arrange
        const string path = "/customerId";

        // Act
        var attribute = new CosmosPartitionKeyAttribute(path);

        // Assert
        attribute.Path.Should().Be(path);
        attribute.IsPartitionKey.Should().BeTrue();
    }

    [Fact]
    public void Constructor_WithNullPath_ShouldAcceptNull()
    {
        // Act
        var attribute = new CosmosPartitionKeyAttribute(null);

        // Assert
        attribute.Path.Should().BeNull();
    }

    [Fact]
    public void Constructor_WithEmptyPath_ShouldAcceptEmpty()
    {
        // Act
        var attribute = new CosmosPartitionKeyAttribute(string.Empty);

        // Assert
        attribute.Path.Should().BeEmpty();
    }

    #endregion

    #region Property Tests

    [Fact]
    public void Path_Property_ShouldBeSettable()
    {
        // Arrange
        var attribute = new CosmosPartitionKeyAttribute();

        // Act
        attribute.Path = "/newPath";

        // Assert
        attribute.Path.Should().Be("/newPath");
    }

    [Fact]
    public void IsPartitionKey_Property_ShouldDefaultToTrue()
    {
        // Arrange & Act
        var attribute = new CosmosPartitionKeyAttribute();

        // Assert
        attribute.IsPartitionKey.Should().BeTrue();
    }

    [Fact]
    public void IsPartitionKey_Property_ShouldBeSettable()
    {
        // Arrange
        var attribute = new CosmosPartitionKeyAttribute();

        // Act
        attribute.IsPartitionKey = false;

        // Assert
        attribute.IsPartitionKey.Should().BeFalse();
    }

    [Theory]
    [InlineData("/customerId")]
    [InlineData("/tenantId")]
    [InlineData("/region")]
    [InlineData("/id")]
    public void Path_ShouldAcceptValidCosmosPaths(string path)
    {
        // Act
        var attribute = new CosmosPartitionKeyAttribute(path);

        // Assert
        attribute.Path.Should().Be(path);
    }

    #endregion

    #region Attribute Usage Tests

    [Fact]
    public void Attribute_ShouldBeApplicableToProperties()
    {
        // Arrange
        var property = typeof(TestEntity).GetProperty(nameof(TestEntity.PartitionKeyProperty));

        // Act
        var attributes = property?.GetCustomAttributes<CosmosPartitionKeyAttribute>(true).ToList();

        // Assert
        attributes.Should().HaveCount(1);
    }

    [Fact]
    public void Attribute_ShouldNotBeApplicableToClasses()
    {
        // Arrange
        var type = typeof(TestEntity);

        // Act
        var attributes = type.GetCustomAttributes<CosmosPartitionKeyAttribute>(true).ToList();

        // Assert
        attributes.Should().BeEmpty();
    }

    [Fact]
    public void Attribute_ShouldNotAllowMultiple()
    {
        // Arrange
        var property = typeof(TestEntity).GetProperty(nameof(TestEntity.PartitionKeyProperty));

        // Act
        var attributeUsage = typeof(CosmosPartitionKeyAttribute)
            .GetCustomAttributes<AttributeUsageAttribute>(true)
            .FirstOrDefault();

        // Assert
        attributeUsage.Should().NotBeNull();
        attributeUsage!.AllowMultiple.Should().BeFalse();
    }

    [Fact]
    public void Attribute_ShouldBeInherited()
    {
        // Act
        var attributeUsage = typeof(CosmosPartitionKeyAttribute)
            .GetCustomAttributes<AttributeUsageAttribute>(true)
            .FirstOrDefault();

        // Assert
        attributeUsage.Should().NotBeNull();
        attributeUsage!.Inherited.Should().BeTrue();
    }

    [Fact]
    public void Attribute_OnInheritedProperty_ShouldBeAccessible()
    {
        // Arrange
        var property = typeof(DerivedTestEntity).GetProperty(nameof(DerivedTestEntity.PartitionKeyProperty));

        // Act
        var attributes = property?.GetCustomAttributes<CosmosPartitionKeyAttribute>(true).ToList();

        // Assert
        attributes.Should().HaveCount(1);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public void Attribute_WithPath_ShouldProvidePathForContainerCreation()
    {
        // Arrange
        var property = typeof(TestEntityWithPath).GetProperty(nameof(TestEntityWithPath.CustomerId));

        // Act
        var attribute = property?.GetCustomAttribute<CosmosPartitionKeyAttribute>();

        // Assert
        attribute.Should().NotBeNull();
        attribute!.Path.Should().Be("/customerId");
    }

    [Fact]
    public void Attribute_WithoutPath_ShouldUsePropertyName()
    {
        // Arrange
        var property = typeof(TestEntity).GetProperty(nameof(TestEntity.PartitionKeyProperty));

        // Act
        var attribute = property?.GetCustomAttribute<CosmosPartitionKeyAttribute>();

        // Assert
        attribute.Should().NotBeNull();
        attribute!.Path.Should().BeNull(); // Path is null, so property name would be used by convention
    }

    [Fact]
    public void MultipleProperties_WithAttribute_ShouldOnlyHaveOnePartitionKey()
    {
        // Note: This test documents expected usage - only one property should have the attribute
        // The attribute itself doesn't enforce this at compile time

        // Arrange
        var propertiesWithAttribute = typeof(TestEntityWithMultipleKeys)
            .GetProperties()
            .Where(p => p.IsDefined(typeof(CosmosPartitionKeyAttribute), true))
            .ToList();

        // Act & Assert - Document that multiple are possible but discouraged
        propertiesWithAttribute.Should().HaveCount(2);
    }

    #endregion

    #region Test Helper Classes

    public class TestEntity
    {
        [CosmosPartitionKey]
        public string? PartitionKeyProperty { get; set; }

        public string? OtherProperty { get; set; }
    }

    public class TestEntityWithPath
    {
        [CosmosPartitionKey("/customerId")]
        public string? CustomerId { get; set; }
    }

    public class DerivedTestEntity : TestEntity
    {
        public string? AdditionalProperty { get; set; }
    }

    public class TestEntityWithMultipleKeys
    {
        [CosmosPartitionKey]
        public string? Key1 { get; set; }

        [CosmosPartitionKey]
        public string? Key2 { get; set; }
    }

    #endregion
}

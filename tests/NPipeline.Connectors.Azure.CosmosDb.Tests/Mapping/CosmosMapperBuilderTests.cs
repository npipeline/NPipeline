using System.Text.Json;
using AwesomeAssertions;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Mapping;

public class CosmosMapperBuilderTests
{
    #region Build<T> Tests - Error Handling

    [Fact]
    public void Build_WithTypeWithoutParameterlessConstructor_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            CosmosMapperBuilder.Build<TestItemWithoutParameterlessConstructor>());

        exception.Message.Should().Contain("does not have a parameterless constructor");
    }

    #endregion

    #region Build<T> Tests - Value Types

    [Fact]
    public void Build_WithStringType_ShouldReturnMapperForFirstColumn()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "TestValue",
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<string>();
        var result = mapper(row);

        // Assert
        result.Should().Be("TestValue");
    }

    [Fact]
    public void Build_WithIntType_ShouldReturnMapperForFirstColumn()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["count"] = 42,
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<int>();
        var result = mapper(row);

        // Assert
        result.Should().Be(42);
    }

    [Fact]
    public void Build_WithBoolType_ShouldReturnMapperForFirstColumn()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["active"] = true,
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<bool>();
        var result = mapper(row);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void Build_WithDoubleType_ShouldReturnMapperForFirstColumn()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["value"] = 3.14,
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<double>();
        var result = mapper(row);

        // Assert
        result.Should().BeApproximately(3.14, 0.001);
    }

    [Fact]
    public void Build_WithLongType_ShouldReturnMapperForFirstColumn()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["bigNumber"] = 9876543210L,
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<long>();
        var result = mapper(row);

        // Assert
        result.Should().Be(9876543210L);
    }

    [Fact]
    public void Build_WithGuidType_ShouldReturnMapperForFirstColumn()
    {
        // Arrange
        var guid = Guid.NewGuid();

        var data = new Dictionary<string, object?>
        {
            ["id"] = guid,
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<Guid>();
        var result = mapper(row);

        // Assert
        result.Should().Be(guid);
    }

    [Fact]
    public void Build_WithDateTimeType_ShouldReturnMapperForFirstColumn()
    {
        // Arrange
        var date = new DateTime(2024, 6, 15, 10, 30, 0, DateTimeKind.Utc);

        var data = new Dictionary<string, object?>
        {
            ["created"] = date,
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<DateTime>();
        var result = mapper(row);

        // Assert
        result.Should().Be(date);
    }

    #endregion

    #region Build<T> Tests - Complex Types

    [Fact]
    public void Build_WithSimplePoco_ShouldMapAllProperties()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["Id"] = "123",
            ["Name"] = "Test Item",
            ["Count"] = 42,
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<TestItem>();
        var result = mapper(row);

        // Assert
        result.Should().NotBeNull();
        result.Id.Should().Be("123");
        result.Name.Should().Be("Test Item");
        result.Count.Should().Be(42);
    }

    [Fact]
    public void Build_WithNullableProperties_ShouldMapCorrectly()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["Id"] = 1,
            ["OptionalValue"] = 100,
            ["OptionalDate"] = new DateTime(2024, 1, 1),
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<TestItemWithNullables>();
        var result = mapper(row);

        // Assert
        result.Id.Should().Be(1);
        result.OptionalValue.Should().Be(100);
        result.OptionalDate.Should().Be(new DateTime(2024, 1, 1));
    }

    [Fact]
    public void Build_WithNullNullableProperties_ShouldMapCorrectly()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["Id"] = 1,
            ["OptionalValue"] = null,
            ["OptionalDate"] = null,
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<TestItemWithNullables>();
        var result = mapper(row);

        // Assert
        result.Id.Should().Be(1);
        result.OptionalValue.Should().BeNull();
        result.OptionalDate.Should().BeNull();
    }

    [Fact]
    public void Build_WithMissingProperties_ShouldUseDefaults()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["Id"] = "123",

            // Name and Count are missing
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<TestItem>();
        var result = mapper(row);

        // Assert
        result.Id.Should().Be("123");
        result.Name.Should().BeNull();
        result.Count.Should().Be(0);
    }

    [Fact]
    public void Build_WithReadOnlyProperty_ShouldSkipProperty()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["Id"] = "123",
            ["ReadOnlyValue"] = "should not be set",
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<TestItemWithReadOnlyProperty>();
        var result = mapper(row);

        // Assert
        result.Id.Should().Be("123");
        result.ReadOnlyValue.Should().Be("Default"); // Should remain default
    }

    [Fact]
    public void Build_WithIgnoredProperty_ShouldSkipProperty()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["Id"] = "123",
            ["IgnoredProperty"] = "should be ignored",
        };

        var row = new CosmosRow(data);

        // Act
        var mapper = CosmosMapperBuilder.Build<TestItemWithIgnoredProperty>();
        var result = mapper(row);

        // Assert
        result.Id.Should().Be("123");
        result.IgnoredProperty.Should().BeNull(); // Should not be mapped
    }

    #endregion

    #region Build<T> Tests - JsonElement Source

    [Fact]
    public void Build_WithJsonElementSource_ShouldMapCorrectly()
    {
        // Arrange
        var json = """{"Id": "456", "Name": "Json Item", "Count": 99}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var mapper = CosmosMapperBuilder.Build<TestItem>();
        var result = mapper(row);

        // Assert
        result.Id.Should().Be("456");
        result.Name.Should().Be("Json Item");
        result.Count.Should().Be(99);
    }

    [Fact]
    public void Build_WithJsonElementSourceAndNestedObject_ShouldMapSimpleProperties()
    {
        // Arrange
        var json = """{"Id": "789", "Name": "Item", "Nested": {"Key": "Value"}}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var mapper = CosmosMapperBuilder.Build<TestItem>();
        var result = mapper(row);

        // Assert
        result.Id.Should().Be("789");
        result.Name.Should().Be("Item");
    }

    #endregion

    #region Test Helper Classes

    public class TestItem
    {
        public string? Id { get; set; }
        public string? Name { get; set; }
        public int Count { get; set; }
    }

    public class TestItemWithNullables
    {
        public int Id { get; set; }
        public int? OptionalValue { get; set; }
        public DateTime? OptionalDate { get; set; }
    }

    public class TestItemWithReadOnlyProperty
    {
        public string? Id { get; set; }
        public string ReadOnlyValue { get; } = "Default";
    }

    public class TestItemWithIgnoredProperty
    {
        public string? Id { get; set; }

        [IgnoreColumn]
        public string? IgnoredProperty { get; set; }
    }

    public class TestItemWithoutParameterlessConstructor
    {
        public TestItemWithoutParameterlessConstructor(string id)
        {
            Id = id;
        }

        public string? Id { get; set; }
    }

    #endregion
}

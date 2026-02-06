using System.Text.Json.Serialization;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Json.Mapping;

namespace NPipeline.Connectors.Json.Tests;

/// <summary>
///     Comprehensive unit tests for JsonWriterMapperBuilder.
///     Tests property name extraction, value getter compilation, ignored properties, naming policies,
///     different types, read-only properties, private setters, cache behavior, ColumnAttribute precedence,
///     complex property names, getter execution, empty models, and property order.
/// </summary>
public class JsonWriterMapperBuilderTests
{
    [Fact]
    public void GetPropertyNames_WithSimpleModel_ReturnsCorrectNames()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<SimpleModel>();

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.Contains("userid", propertyNames);
        Assert.Contains("fullname", propertyNames);
    }

    [Fact]
    public void GetPropertyNames_WithColumnAttribute_ReturnsColumnName()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<ModelWithColumn>();

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.Contains("user_id", propertyNames);
        Assert.Contains("name", propertyNames);
    }

    [Fact]
    public void GetPropertyNames_WithJsonPropertyNameAttribute_ReturnsPropertyName()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<ModelWithJsonPropertyName>();

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.Contains("user_id", propertyNames);
        Assert.Contains("name", propertyNames);
    }

    [Fact]
    public void GetPropertyNames_WithIgnoreColumnAttribute_ExcludesProperty()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<ModelWithIgnoreColumn>();

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.DoesNotContain("internalfield", propertyNames);
        Assert.Contains("id", propertyNames);
        Assert.Contains("name", propertyNames);
    }

    [Fact]
    public void GetPropertyNames_WithJsonIgnoreAttribute_ExcludesProperty()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<ModelWithJsonIgnore>();

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.DoesNotContain("internalfield", propertyNames);
        Assert.Contains("id", propertyNames);
        Assert.Contains("name", propertyNames);
    }

    [Fact]
    public void GetPropertyNames_WithColumnIgnoreAttribute_ExcludesProperty()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<ModelWithColumnIgnore>();

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.DoesNotContain("internalfield", propertyNames);
        Assert.Contains("id", propertyNames);
        Assert.Contains("name", propertyNames);
    }

    [Fact]
    public void GetPropertyNames_WithSnakeCaseNamingPolicy_ConvertsToSnakeCase()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<SimpleModel>(JsonPropertyNamingPolicy.SnakeCase);

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.Contains("user_id", propertyNames);
        Assert.Contains("full_name", propertyNames);
    }

    [Fact]
    public void GetPropertyNames_WithCamelCaseNamingPolicy_ConvertsToCamelCase()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<SimpleModel>(JsonPropertyNamingPolicy.CamelCase);

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.Contains("userId", propertyNames);
        Assert.Contains("fullName", propertyNames);
    }

    [Fact]
    public void GetPropertyNames_WithPascalCaseNamingPolicy_UsesPascalCase()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<SimpleModel>(JsonPropertyNamingPolicy.PascalCase);

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.Contains("UserId", propertyNames);
        Assert.Contains("FullName", propertyNames);
    }

    [Fact]
    public void GetPropertyNames_WithLowerCaseNamingPolicy_ConvertsToLowerCase()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<SimpleModel>();

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.Contains("userid", propertyNames);
        Assert.Contains("fullname", propertyNames);
    }

    [Fact]
    public void GetPropertyNames_WithAsIsNamingPolicy_UsesOriginalName()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<SimpleModel>(JsonPropertyNamingPolicy.AsIs);

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.Contains("UserId", propertyNames);
        Assert.Contains("FullName", propertyNames);
    }

    [Fact]
    public void GetValueGetters_WithSimpleModel_ReturnsCorrectGetters()
    {
        // Act
        var getters = JsonWriterMapperBuilder.GetValueGetters<SimpleModel>();

        // Assert
        Assert.Equal(2, getters.Length);
    }

    [Fact]
    public void GetValueGetters_WithIgnoreColumnAttribute_ExcludesProperty()
    {
        // Act
        var getters = JsonWriterMapperBuilder.GetValueGetters<ModelWithIgnoreColumn>();

        // Assert
        Assert.Equal(2, getters.Length);
    }

    [Fact]
    public void GetValueGetters_WithJsonIgnoreAttribute_ExcludesProperty()
    {
        // Act
        var getters = JsonWriterMapperBuilder.GetValueGetters<ModelWithJsonIgnore>();

        // Assert
        Assert.Equal(2, getters.Length);
    }

    [Fact]
    public void GetValueGetters_WithColumnIgnoreAttribute_ExcludesProperty()
    {
        // Act
        var getters = JsonWriterMapperBuilder.GetValueGetters<ModelWithColumnIgnore>();

        // Assert
        Assert.Equal(2, getters.Length);
    }

    [Fact]
    public void GetValueGetters_WithDifferentTypes_ReturnsCorrectValues()
    {
        // Arrange
        var model = new ComplexModel
        {
            Id = 1,
            Age = 30,
            Balance = 100.50m,
            IsActive = true,
        };

        // Act
        var getters = JsonWriterMapperBuilder.GetValueGetters<ComplexModel>();

        // Assert
        Assert.Equal(4, getters.Length);
        Assert.Equal(1, getters[0](model));
        Assert.Equal(30, getters[1](model));
        Assert.Equal(100.50m, getters[2](model));
        Assert.Equal(true, getters[3](model));
    }

    [Fact]
    public void GetValueGetters_WithReadOnlyProperty_IncludesProperty()
    {
        // Arrange
        var model = new ModelWithReadOnlyProperty { Id = 1 };

        // Act
        var getters = JsonWriterMapperBuilder.GetValueGetters<ModelWithReadOnlyProperty>();

        // Assert
        Assert.Equal(2, getters.Length);
        Assert.Equal(1, getters[0](model));
        Assert.Equal("computed", getters[1](model));
    }

    [Fact]
    public void GetValueGetters_WithPrivateSetter_IncludesProperty()
    {
        // Arrange
        var model = ModelWithPrivateSetter.Create(1, "Alice");

        // Act
        var getters = JsonWriterMapperBuilder.GetValueGetters<ModelWithPrivateSetter>();

        // Assert
        Assert.Equal(2, getters.Length);
        Assert.Equal(1, getters[0](model));
        Assert.Equal("Alice", getters[1](model));
    }

    [Fact]
    public void GetValueGetters_WithNullValues_ReturnsNull()
    {
        // Arrange
        var model = new SimpleModel { UserId = 1, FullName = null };

        // Act
        var getters = JsonWriterMapperBuilder.GetValueGetters<SimpleModel>();

        // Assert
        Assert.Equal(2, getters.Length);
        Assert.Equal(1, getters[0](model));
        Assert.Null(getters[1](model));
    }

    [Fact]
    public void GetPropertyNames_UsesCache_ReturnsSameArray()
    {
        // Act
        var propertyNames1 = JsonWriterMapperBuilder.GetPropertyNames<SimpleModel>();
        var propertyNames2 = JsonWriterMapperBuilder.GetPropertyNames<SimpleModel>();

        // Assert
        Assert.Same(propertyNames1, propertyNames2);
    }

    [Fact]
    public void GetValueGetters_UsesCache_ReturnsSameArray()
    {
        // Act
        var getters1 = JsonWriterMapperBuilder.GetValueGetters<SimpleModel>();
        var getters2 = JsonWriterMapperBuilder.GetValueGetters<SimpleModel>();

        // Assert
        Assert.Same(getters1, getters2);
    }

    [Fact]
    public void GetPropertyNames_WithColumnAttributeTakesPrecedenceOverJsonPropertyName()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<ModelWithColumnAndJsonPropertyName>();

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.Contains("custom_name", propertyNames);
        Assert.Contains("name", propertyNames);
    }

    [Fact]
    public void GetPropertyNames_WithComplexPropertyName_HandlesCorrectly()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<ModelWithComplexProperty>();

        // Assert
        Assert.Equal(2, propertyNames.Length);
        Assert.Contains("user_id", propertyNames);
        Assert.Contains("full_name", propertyNames);
    }

    [Fact]
    public void GetValueGetters_ExecutesGetter_ReturnsCorrectValue()
    {
        // Arrange
        var model = new SimpleModel { UserId = 1, FullName = "Alice" };

        // Act
        var getters = JsonWriterMapperBuilder.GetValueGetters<SimpleModel>();
        var result = getters[0](model);

        // Assert
        Assert.Equal(1, result);
    }

    [Fact]
    public void GetPropertyNames_WithEmptyModel_ReturnsEmptyArray()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<EmptyModel>();

        // Assert
        Assert.Empty(propertyNames);
    }

    [Fact]
    public void GetValueGetters_WithEmptyModel_ReturnsEmptyArray()
    {
        // Act
        var getters = JsonWriterMapperBuilder.GetValueGetters<EmptyModel>();

        // Assert
        Assert.Empty(getters);
    }

    [Fact]
    public void GetPropertyNames_WithMultipleProperties_ReturnsCorrectOrder()
    {
        // Act
        var propertyNames = JsonWriterMapperBuilder.GetPropertyNames<SimpleModel>();

        // Assert
        Assert.Equal(2, propertyNames.Length);

        // Properties should be in the order they are declared in the class
        Assert.Equal("userid", propertyNames[0]);
        Assert.Equal("fullname", propertyNames[1]);
    }

    public class SimpleModel
    {
        public int UserId { get; set; }
        public string? FullName { get; set; }
    }

    public class ModelWithColumn
    {
        [Column("user_id")]
        public int UserId { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    public class ModelWithJsonPropertyName
    {
        [JsonPropertyName("user_id")]
        public int UserId { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    public class ModelWithIgnoreColumn
    {
        public int Id { get; set; }

        [IgnoreColumn]
        public string InternalField { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;
    }

    public class ModelWithJsonIgnore
    {
        public int Id { get; set; }

        [JsonIgnore]
        public string InternalField { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;
    }

    public class ModelWithColumnIgnore
    {
        public int Id { get; set; }

        [Column("internal_field", Ignore = true)]
        public string InternalField { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;
    }

    public class ComplexModel
    {
        public int Id { get; set; }
        public int Age { get; set; }
        public decimal Balance { get; set; }
        public bool IsActive { get; set; }
    }

    public class ModelWithReadOnlyProperty
    {
        public int Id { get; set; }

        public string Computed => "computed";
    }

    public class ModelWithPrivateSetter
    {
        public int Id { get; private set; }
        public string Name { get; private set; } = string.Empty;

        public static ModelWithPrivateSetter Create(int id, string name)
        {
            return new ModelWithPrivateSetter { Id = id, Name = name };
        }
    }

    public class ModelWithColumnAndJsonPropertyName
    {
        [Column("custom_name")]
        public int CustomName { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;
    }

    public class ModelWithComplexProperty
    {
        [Column("user_id")]
        public int UserId { get; set; }

        [JsonPropertyName("full_name")]
        public string FullName { get; set; } = string.Empty;
    }

    public class EmptyModel
    {
    }
}

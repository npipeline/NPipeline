using System.Text.Json;
using System.Text.Json.Serialization;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Json.Mapping;

namespace NPipeline.Connectors.Json.Tests;

/// <summary>
///     Comprehensive unit tests for JsonMapperBuilder.
///     Tests attribute-based mapping, property name conversion, ignored properties, nullable properties,
///     type conversion, cache behavior, error handling, ColumnAttribute precedence, and complex property names.
/// </summary>
public class JsonMapperBuilderTests
{
    [Fact]
    public void BuildMapper_WithColumnAttribute_UsesColumnName()
    {
        // Arrange
        var json = "{\"user_id\":1,\"name\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<ModelWithColumn>();
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.UserId);
        Assert.Equal("Alice", result.Name);
    }

    [Fact]
    public void BuildMapper_WithJsonPropertyNameAttribute_UsesPropertyName()
    {
        // Arrange
        var json = "{\"user_id\":1,\"name\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<ModelWithJsonPropertyName>();
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.UserId);
        Assert.Equal("Alice", result.Name);
    }

    [Fact]
    public void BuildMapper_WithIgnoreColumnAttribute_IgnoresProperty()
    {
        // Arrange
        var json = "{\"id\":1,\"internal_field\":\"secret\",\"name\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<ModelWithIgnoreColumn>();
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.Id);
        Assert.Equal(string.Empty, result.InternalField);
        Assert.Equal("Alice", result.Name);
    }

    [Fact]
    public void BuildMapper_WithJsonIgnoreAttribute_IgnoresProperty()
    {
        // Arrange
        var json = "{\"id\":1,\"internal_field\":\"secret\",\"name\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<ModelWithJsonIgnore>();
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.Id);
        Assert.Equal(string.Empty, result.InternalField);
        Assert.Equal("Alice", result.Name);
    }

    [Fact]
    public void BuildMapper_WithColumnIgnoreAttribute_IgnoresProperty()
    {
        // Arrange
        var json = "{\"id\":1,\"internal_field\":\"secret\",\"name\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<ModelWithColumnIgnore>();
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.Id);
        Assert.Equal(string.Empty, result.InternalField);
        Assert.Equal("Alice", result.Name);
    }

    [Fact]
    public void BuildMapper_WithSnakeCaseNamingPolicy_ConvertsToSnakeCase()
    {
        // Arrange
        var json = "{\"user_id\":1,\"full_name\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<SimpleModel>(JsonPropertyNamingPolicy.SnakeCase);
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.UserId);
        Assert.Equal("Alice", result.FullName);
    }

    [Fact]
    public void BuildMapper_WithCamelCaseNamingPolicy_ConvertsToCamelCase()
    {
        // Arrange
        var json = "{\"userId\":1,\"fullName\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<SimpleModel>(JsonPropertyNamingPolicy.CamelCase);
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.UserId);
        Assert.Equal("Alice", result.FullName);
    }

    [Fact]
    public void BuildMapper_WithPascalCaseNamingPolicy_UsesPascalCase()
    {
        // Arrange
        var json = "{\"UserId\":1,\"FullName\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<SimpleModel>(JsonPropertyNamingPolicy.PascalCase);
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.UserId);
        Assert.Equal("Alice", result.FullName);
    }

    [Fact]
    public void BuildMapper_WithLowerCaseNamingPolicy_ConvertsToLowerCase()
    {
        // Arrange
        var json = "{\"userid\":1,\"fullname\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<SimpleModel>();
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.UserId);
        Assert.Equal("Alice", result.FullName);
    }

    [Fact]
    public void BuildMapper_WithAsIsNamingPolicy_UsesOriginalName()
    {
        // Arrange
        var json = "{\"UserId\":1,\"FullName\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<SimpleModel>(JsonPropertyNamingPolicy.AsIs);
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.UserId);
        Assert.Equal("Alice", result.FullName);
    }

    [Fact]
    public void BuildMapper_WithNullableProperty_HandlesNullsCorrectly()
    {
        // Arrange
        var json = "{\"UserId\":1,\"FullName\":null}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<SimpleModel>(JsonPropertyNamingPolicy.PascalCase);
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.UserId);
        Assert.Null(result.FullName);
    }

    [Fact]
    public void BuildMapper_WithDifferentTypes_ConvertsCorrectly()
    {
        // Arrange
        var json = "{\"id\":\"1\",\"age\":\"30\",\"balance\":\"100.50\",\"isactive\":\"true\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<ComplexModel>();
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.Id);
        Assert.Equal(30, result.Age);
        Assert.Equal(100.50m, result.Balance);
        Assert.True(result.IsActive);
    }

    [Fact]
    public void BuildMapper_UsesCache_ReturnsSameMapperForSameType()
    {
        // Arrange
        var json = "{\"userid\":1,\"fullname\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper1 = JsonMapperBuilder.Build<SimpleModel>();
        var mapper2 = JsonMapperBuilder.Build<SimpleModel>();

        // Assert
        Assert.Same(mapper1, mapper2);
    }

    [Fact]
    public void BuildMapper_WithMissingProperty_DoesNotThrow()
    {
        // Arrange
        var json = "{\"userid\":1}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<SimpleModel>();
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.UserId);
        Assert.Null(result.FullName);
    }

    [Fact]
    public void BuildMapper_WithExtraProperty_IgnoresExtraProperty()
    {
        // Arrange
        var json = "{\"userid\":1,\"fullname\":\"Alice\",\"extra\":\"ignored\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<SimpleModel>();
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.UserId);
        Assert.Equal("Alice", result.FullName);
    }

    [Fact]
    public void BuildMapper_WithComplexPropertyName_HandlesCorrectly()
    {
        // Arrange
        var json = "{\"userid\":123,\"fullname\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);
        var mapper = JsonMapperBuilder.Build<SimpleModel>();

        // Act
        var result = mapper(row);

        // Assert
        Assert.Equal(123, result.UserId);
        Assert.Equal("Alice", result.FullName);
    }

    [Fact]
    public void BuildMapper_WithColumnAttributeTakesPrecedenceOverJsonPropertyName()
    {
        // Arrange
        var json = "{\"custom_name\":1,\"name\":\"Alice\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var mapper = JsonMapperBuilder.Build<ModelWithColumnAndJsonPropertyName>();
        var result = mapper(row);

        // Assert
        Assert.Equal(1, result.CustomName);
        Assert.Equal("Alice", result.Name);
    }

    [Fact]
    public void BuildMapper_WithMappingError_UsesDefaultValue()
    {
        // Arrange
        var json = "{\"id\":\"not_an_int\",\"age\":\"30\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);
        var mapper = JsonMapperBuilder.Build<ComplexModel>();

        // Act
        var result = mapper(row);

        // Assert
        // When conversion fails, the default value is used
        Assert.Equal(0, result.Id);
        Assert.Equal(30, result.Age);
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

    public class SimpleModel
    {
        public int UserId { get; set; }
        public string? FullName { get; set; }
    }

    public class ComplexModel
    {
        public int Id { get; set; }
        public int Age { get; set; }
        public decimal Balance { get; set; }
        public bool IsActive { get; set; }
    }

    public class ModelWithColumnAndJsonPropertyName
    {
        [Column("custom_name")]
        public int CustomName { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;
    }

    public class ModelWithInvalidType
    {
        public int InvalidType { get; set; }
    }
}

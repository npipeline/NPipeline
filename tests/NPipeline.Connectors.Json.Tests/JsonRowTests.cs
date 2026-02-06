using System.Text.Json;

namespace NPipeline.Connectors.Json.Tests;

/// <summary>
///     Unit tests for <see cref="JsonRow" />.
///     Tests TryGet methods, Get methods with defaults, HasProperty method,
///     nested property access, and type conversion.
/// </summary>
public class JsonRowTests
{
    #region TryGet Tests

    [Fact]
    public void TryGet_WithExistingProperty_ReturnsTrueAndValue()
    {
        // Arrange
        var json = """
                   {
                       "id": 1,
                       "name": "John Doe",
                       "age": 30
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("id", out int id));
        Assert.Equal(1, id);

        Assert.True(row.TryGet("name", out string? name));
        Assert.Equal("John Doe", name);

        Assert.True(row.TryGet("age", out int age));
        Assert.Equal(30, age);
    }

    [Fact]
    public void TryGet_WithMissingProperty_ReturnsFalseAndDefaultValue()
    {
        // Arrange
        var json = """
                   {
                       "id": 1,
                       "name": "John Doe"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.False(row.TryGet("age", out int age));
        Assert.Equal(0, age);

        Assert.False(row.TryGet("email", out var email, "default@example.com"));
        Assert.Equal("default@example.com", email);
    }

    [Fact]
    public void TryGet_WithNullValue_ReturnsFalseAndDefaultValue()
    {
        // Arrange
        var json = """
                   {
                       "id": 1,
                       "name": null
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.False(row.TryGet("name", out var name, "default"));
        Assert.Equal("default", name);
    }

    [Fact]
    public void TryGet_WithInvalidTypeConversion_ReturnsFalseAndDefaultValue()
    {
        // Arrange
        var json = """
                   {
                       "id": "not a number",
                       "name": "John Doe"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.False(row.TryGet("id", out int id));
        Assert.Equal(0, id);
    }

    [Fact]
    public void TryGet_WithCaseInsensitive_MatchesProperties()
    {
        // Arrange
        var json = """
                   {
                       "ID": 1,
                       "NAME": "John Doe",
                       "EMAIL": "john@example.com"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("id", out int id));
        Assert.Equal(1, id);

        Assert.True(row.TryGet("name", out string? name));
        Assert.Equal("John Doe", name);

        Assert.True(row.TryGet("email", out string? email));
        Assert.Equal("john@example.com", email);
    }

    [Fact]
    public void TryGet_WithCaseSensitive_DoesNotMatchProperties()
    {
        // Arrange
        var json = """
                   {
                       "ID": 1,
                       "NAME": "John Doe"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element, false);

        // Act & Assert
        Assert.False(row.TryGet("id", out int id));
        Assert.Equal(0, id);

        Assert.False(row.TryGet("name", out var name, "default"));
        Assert.Equal("default", name);
    }

    #endregion

    #region Get Tests

    [Fact]
    public void Get_WithExistingProperty_ReturnsValue()
    {
        // Arrange
        var json = """
                   {
                       "id": 1,
                       "name": "John Doe",
                       "age": 30
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.Equal(1, row.Get<int>("id"));
        Assert.Equal("John Doe", row.Get<string>("name"));
        Assert.Equal(30, row.Get<int>("age"));
    }

    [Fact]
    public void Get_WithMissingProperty_ReturnsDefaultValue()
    {
        // Arrange
        var json = """
                   {
                       "id": 1,
                       "name": "John Doe"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.Equal(0, row.Get<int>("age"));
        Assert.Equal("default", row.Get<string>("email", "default"));
        Assert.False(row.Get<bool>("is_active"));
    }

    [Fact]
    public void Get_WithNullValue_ReturnsDefaultValue()
    {
        // Arrange
        var json = """
                   {
                       "id": 1,
                       "name": null
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.Equal("default", row.Get<string>("name", "default"));
    }

    #endregion

    #region HasProperty Tests

    [Fact]
    public void HasProperty_WithExistingProperty_ReturnsTrue()
    {
        // Arrange
        var json = """
                   {
                       "id": 1,
                       "name": "John Doe",
                       "age": 30
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.HasProperty("id"));
        Assert.True(row.HasProperty("name"));
        Assert.True(row.HasProperty("age"));
    }

    [Fact]
    public void HasProperty_WithMissingProperty_ReturnsFalse()
    {
        // Arrange
        var json = """
                   {
                       "id": 1,
                       "name": "John Doe"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.False(row.HasProperty("age"));
        Assert.False(row.HasProperty("email"));
        Assert.False(row.HasProperty("is_active"));
    }

    [Fact]
    public void HasProperty_WithCaseInsensitive_MatchesProperties()
    {
        // Arrange
        var json = """
                   {
                       "ID": 1,
                       "NAME": "John Doe"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.HasProperty("id"));
        Assert.True(row.HasProperty("name"));
    }

    [Fact]
    public void HasProperty_WithCaseSensitive_DoesNotMatchProperties()
    {
        // Arrange
        var json = """
                   {
                       "ID": 1,
                       "NAME": "John Doe"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element, false);

        // Act & Assert
        Assert.False(row.HasProperty("id"));
        Assert.False(row.HasProperty("name"));
    }

    #endregion

    #region GetNested Tests

    [Fact]
    public void GetNested_WithValidPath_ReturnsValue()
    {
        // Arrange
        var json = """
                   {
                       "user": {
                           "id": 1,
                           "name": "John Doe",
                           "address": {
                               "city": "New York",
                               "state": "NY"
                           }
                       }
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.Equal(1, row.GetNested<int>("user.id"));
        Assert.Equal("John Doe", row.GetNested<string>("user.name"));
        Assert.Equal("New York", row.GetNested<string>("user.address.city"));
        Assert.Equal("NY", row.GetNested<string>("user.address.state"));
    }

    [Fact]
    public void GetNested_WithMissingPath_ReturnsDefaultValue()
    {
        // Arrange
        var json = """
                   {
                       "user": {
                           "id": 1,
                           "name": "John Doe"
                       }
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.Equal(0, row.GetNested<int>("user.age"));
        Assert.Equal("default", row.GetNested<string>("user.email", "default"));
        Assert.Equal("default", row.GetNested<string>("user.address.city", "default"));
    }

    [Fact]
    public void GetNested_WithEmptyPath_ReturnsDefaultValue()
    {
        // Arrange
        var json = """
                   {
                       "id": 1,
                       "name": "John Doe"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.Equal("default", row.GetNested<string>("", "default"));
        Assert.Equal(0, row.GetNested<int>(""));
    }

    [Fact]
    public void GetNested_WithNullValueInPath_ReturnsDefaultValue()
    {
        // Arrange
        var json = """
                   {
                       "user": {
                           "id": 1,
                           "name": "John Doe",
                           "address": null
                       }
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.Equal("default", row.GetNested<string>("user.address.city", "default"));
    }

    [Fact]
    public void GetNested_WithNonObjectInPath_ReturnsDefaultValue()
    {
        // Arrange
        var json = """
                   {
                       "user": {
                           "id": 1,
                           "name": "John Doe",
                           "address": "not an object"
                       }
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.Equal("default", row.GetNested<string>("user.address.city", "default"));
    }

    #endregion

    #region Type Conversion Tests

    [Fact]
    public void TryGet_WithIntValue_ConvertsCorrectly()
    {
        // Arrange
        var json = """
                   {
                       "int_value": 42,
                       "string_int": "42"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("int_value", out int intValue));
        Assert.Equal(42, intValue);

        Assert.True(row.TryGet("string_int", out int stringInt));
        Assert.Equal(42, stringInt);
    }

    [Fact]
    public void TryGet_WithLongValue_ConvertsCorrectly()
    {
        // Arrange
        var json = """
                   {
                       "long_value": 9223372036854775807,
                       "string_long": "9223372036854775807"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("long_value", out long longValue));
        Assert.Equal(9223372036854775807L, longValue);

        Assert.True(row.TryGet("string_long", out long stringLong));
        Assert.Equal(9223372036854775807L, stringLong);
    }

    [Fact]
    public void TryGet_WithShortValue_ConvertsCorrectly()
    {
        // Arrange
        var json = """
                   {
                       "short_value": 32767,
                       "string_short": "32767"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("short_value", out short shortValue));
        Assert.Equal((short)32767, shortValue);

        Assert.True(row.TryGet("string_short", out short stringShort));
        Assert.Equal((short)32767, stringShort);
    }

    [Fact]
    public void TryGet_WithFloatValue_ConvertsCorrectly()
    {
        // Arrange
        var json = """
                   {
                       "float_value": 3.14,
                       "string_float": "3.14"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("float_value", out float floatValue));
        Assert.Equal(3.14f, floatValue);

        Assert.True(row.TryGet("string_float", out float stringFloat));
        Assert.Equal(3.14f, stringFloat);
    }

    [Fact]
    public void TryGet_WithDoubleValue_ConvertsCorrectly()
    {
        // Arrange
        var json = """
                   {
                       "double_value": 3.14159265359,
                       "string_double": "3.14159265359"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("double_value", out double doubleValue));
        Assert.Equal(3.14159265359, doubleValue);

        Assert.True(row.TryGet("string_double", out double stringDouble));
        Assert.Equal(3.14159265359, stringDouble);
    }

    [Fact]
    public void TryGet_WithDecimalValue_ConvertsCorrectly()
    {
        // Arrange
        var json = """
                   {
                       "decimal_value": 123.456,
                       "string_decimal": "123.456"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("decimal_value", out decimal decimalValue));
        Assert.Equal(123.456m, decimalValue);

        Assert.True(row.TryGet("string_decimal", out decimal stringDecimal));
        Assert.Equal(123.456m, stringDecimal);
    }

    [Fact]
    public void TryGet_WithBoolValue_ConvertsCorrectly()
    {
        // Arrange
        var json = """
                   {
                       "bool_value": true,
                       "string_bool": "true",
                       "number_bool": 1,
                       "number_bool_false": 0
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("bool_value", out bool boolValue));
        Assert.True(boolValue);

        Assert.True(row.TryGet("string_bool", out bool stringBool));
        Assert.True(stringBool);

        Assert.True(row.TryGet("number_bool", out bool numberBool));
        Assert.True(numberBool);

        Assert.True(row.TryGet("number_bool_false", out bool numberBoolFalse));
        Assert.False(numberBoolFalse);
    }

    [Fact]
    public void TryGet_WithStringValue_ConvertsCorrectly()
    {
        // Arrange
        var json = """
                   {
                       "string_value": "hello",
                       "number_value": 42,
                       "bool_value": true
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("string_value", out string? stringValue));
        Assert.Equal("hello", stringValue);

        Assert.True(row.TryGet("number_value", out string? numberValue));
        Assert.Equal("42", numberValue);

        Assert.True(row.TryGet("bool_value", out string? boolValue));
        Assert.Equal("True", boolValue);
    }

    [Fact]
    public void TryGet_WithDateTimeValue_ConvertsCorrectly()
    {
        // Arrange
        var json = "{\"timestamp\":\"2024-01-15T10:30:00Z\"}";
        var row = new JsonRow(JsonDocument.Parse(json).RootElement);

        // Act
        var result = row.Get<DateTime>("timestamp");

        // Assert
        Assert.Equal(2024, result.Year);
        Assert.Equal(1, result.Month);
        Assert.Equal(15, result.Day);
        Assert.Equal(10, result.Hour);
        Assert.Equal(30, result.Minute);

        // The DateTime value should preserve the UTC kind from the JSON
        Assert.Equal(DateTimeKind.Utc, result.Kind);
    }

    [Fact]
    public void TryGet_WithGuidValue_ConvertsCorrectly()
    {
        // Arrange
        var json = """
                   {
                       "guid_value": "12345678-1234-1234-1234-123456789012"
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("guid_value", out Guid guidValue));
        Assert.Equal(Guid.Parse("12345678-1234-1234-1234-123456789012"), guidValue);
    }

    #endregion

    #region Non-Object Element Tests

    [Fact]
    public void TryGet_WithArrayElement_ReturnsFalse()
    {
        // Arrange
        var json = """
                   [1, 2, 3, 4, 5]
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.False(row.TryGet("id", out int id));
        Assert.Equal(0, id);
    }

    [Fact]
    public void TryGet_WithStringElement_ReturnsFalse()
    {
        // Arrange
        var json = "\"not an object\"";
        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.False(row.TryGet("id", out int id));
        Assert.Equal(0, id);
    }

    [Fact]
    public void TryGet_WithNumberElement_ReturnsFalse()
    {
        // Arrange
        var json = "42";
        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.False(row.TryGet("id", out int id));
        Assert.Equal(0, id);
    }

    [Fact]
    public void HasProperty_WithArrayElement_ReturnsFalse()
    {
        // Arrange
        var json = """
                   [1, 2, 3, 4, 5]
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.False(row.HasProperty("id"));
    }

    #endregion

    #region Complex Nested Tests

    [Fact]
    public void GetNested_WithDeepNesting_ReturnsValue()
    {
        // Arrange
        var json = """
                   {
                       "level1": {
                           "level2": {
                               "level3": {
                                   "level4": {
                                       "value": "deep value"
                                   }
                               }
                           }
                       }
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.Equal("deep value", row.GetNested<string>("level1.level2.level3.level4.value"));
    }

    [Fact]
    public void GetNested_WithMixedCase_ReturnsValue()
    {
        // Arrange
        var json = """
                   {
                       "User": {
                           "Name": "John Doe",
                           "Address": {
                               "City": "New York"
                           }
                       }
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.Equal("John Doe", row.GetNested<string>("user.name"));
        Assert.Equal("New York", row.GetNested<string>("user.address.city"));
    }

    #endregion

    #region Edge Cases Tests

    [Fact]
    public void TryGet_WithEmptyStringProperty_ReturnsTrueAndEmptyString()
    {
        // Arrange
        var json = """
                   {
                       "id": 1,
                       "name": ""
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("name", out string? name));
        Assert.Equal(string.Empty, name);
    }

    [Fact]
    public void TryGet_WithZeroValue_ReturnsTrueAndZero()
    {
        // Arrange
        var json = """
                   {
                       "id": 0,
                       "balance": 0.0
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("id", out int id));
        Assert.Equal(0, id);

        Assert.True(row.TryGet("balance", out decimal balance));
        Assert.Equal(0m, balance);
    }

    [Fact]
    public void TryGet_WithFalseValue_ReturnsTrueAndFalse()
    {
        // Arrange
        var json = """
                   {
                       "is_active": false
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.True(row.TryGet("is_active", out bool isActive));
        Assert.False(isActive);
    }

    [Fact]
    public void GetNested_WithWhitespaceInPath_ReturnsDefaultValue()
    {
        // Arrange
        var json = """
                   {
                       "user": {
                           "id": 1
                       }
                   }
                   """;

        var element = JsonDocument.Parse(json).RootElement;
        var row = new JsonRow(element);

        // Act & Assert
        Assert.Equal("default", row.GetNested<string>("user . id", "default"));
    }

    #endregion
}

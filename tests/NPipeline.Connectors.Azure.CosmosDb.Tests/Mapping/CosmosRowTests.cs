using System.Collections;
using System.Text.Json;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Mapping;

public class CosmosRowTests
{
    #region Constructor Tests - JsonElement

    [Fact]
    public void Constructor_WithJsonElement_ShouldInitializeCorrectly()
    {
        // Arrange
        var json = """{"id": "123", "name": "Test", "value": 42}""";
        var document = JsonDocument.Parse(json).RootElement;

        // Act
        var row = new CosmosRow(document);

        // Assert
        row.FieldCount.Should().Be(3);
        row.ColumnNames.Should().Contain(["id", "name", "value"]);
    }

    [Fact]
    public void Constructor_WithJsonElementAndColumnNames_ShouldUseProvidedNames()
    {
        // Arrange
        var json = """{"id": "123", "name": "Test"}""";
        var document = JsonDocument.Parse(json).RootElement;
        var columnNames = new List<string> { "id", "name", "extra" };

        // Act
        var row = new CosmosRow(document, columnNames);

        // Assert
        row.FieldCount.Should().Be(3);
        row.ColumnNames.Should().BeEquivalentTo(columnNames);
    }

    [Fact]
    public void Constructor_WithEmptyJsonElement_ShouldHaveZeroFields()
    {
        // Arrange
        var json = "{}";
        var document = JsonDocument.Parse(json).RootElement;

        // Act
        var row = new CosmosRow(document);

        // Assert
        row.FieldCount.Should().Be(0);
        row.ColumnNames.Should().BeEmpty();
    }

    [Fact]
    public void Constructor_WithNullJsonElement_ShouldHaveZeroFields()
    {
        // Arrange
        var json = "null";
        var document = JsonDocument.Parse(json).RootElement;

        // Act
        var row = new CosmosRow(document);

        // Assert
        row.FieldCount.Should().Be(0);
    }

    #endregion

    #region Constructor Tests - Dictionary

    [Fact]
    public void Constructor_WithDictionary_ShouldInitializeCorrectly()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["id"] = "123",
            ["name"] = "Test",
            ["count"] = 42,
        };

        // Act
        var row = new CosmosRow(data);

        // Assert
        row.FieldCount.Should().Be(3);
        row.ColumnNames.Should().Contain(["id", "name", "count"]);
    }

    [Fact]
    public void Constructor_WithDictionaryCaseInsensitive_ShouldMatchCaseInsensitive()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["ID"] = "123",
            ["Name"] = "Test",
        };

        // Act
        var row = new CosmosRow(data);

        // Assert
        row.HasColumn("id").Should().BeTrue();
        row.HasColumn("ID").Should().BeTrue();
        row.HasColumn("name").Should().BeTrue();
        row.HasColumn("NAME").Should().BeTrue();
    }

    [Fact]
    public void Constructor_WithDictionaryCaseSensitive_ShouldMatchExactCase()
    {
        // Arrange
        var data = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ID"] = "123",
        };

        // Act
        var row = new CosmosRow(data, false);

        // Assert
        row.HasColumn("ID").Should().BeTrue();
        row.HasColumn("id").Should().BeFalse();
    }

    [Fact]
    public void Constructor_WithEmptyDictionary_ShouldHaveZeroFields()
    {
        // Arrange
        var data = new Dictionary<string, object?>();

        // Act
        var row = new CosmosRow(data);

        // Assert
        row.FieldCount.Should().Be(0);
        row.ColumnNames.Should().BeEmpty();
    }

    #endregion

    #region Constructor Tests - IDatabaseReader

    [Fact]
    public void Constructor_WithDatabaseReader_ShouldExtractDataCorrectly()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(3);
        A.CallTo(() => reader.GetName(0)).Returns("id");
        A.CallTo(() => reader.GetName(1)).Returns("name");
        A.CallTo(() => reader.GetName(2)).Returns("value");
        A.CallTo(() => reader.GetFieldValue<object>(0)).Returns("123");
        A.CallTo(() => reader.GetFieldValue<object>(1)).Returns("Test");
        A.CallTo(() => reader.GetFieldValue<object>(2)).Returns(42);

        // Act
        var row = new CosmosRow(reader);

        // Assert
        row.FieldCount.Should().Be(3);
        row.ColumnNames.Should().Contain(["id", "name", "value"]);
    }

    [Fact]
    public void Constructor_WithDatabaseReaderThrowing_ShouldSetNullValue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(2);
        A.CallTo(() => reader.GetName(0)).Returns("id");
        A.CallTo(() => reader.GetName(1)).Returns("problematic");
        A.CallTo(() => reader.GetFieldValue<object>(0)).Returns("123");
        A.CallTo(() => reader.GetFieldValue<object>(1)).Throws(new InvalidOperationException());

        // Act
        var row = new CosmosRow(reader);

        // Assert
        row.GetValue("problematic").Should().BeNull();
    }

    #endregion

    #region Get<T> Tests

    [Fact]
    public void Get_WithValidColumnName_ShouldReturnValue()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "TestValue",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.Get<string>("name");

        // Assert
        result.Should().Be("TestValue");
    }

    [Fact]
    public void Get_WithMissingColumn_ShouldReturnDefault()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "Test",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.Get<string>("missing");

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void Get_WithDefaultValue_ShouldReturnDefaultWhenMissing()
    {
        // Arrange
        var data = new Dictionary<string, object?>();
        var row = new CosmosRow(data);

        // Act
        var result = row.Get("missing", "default_value");

        // Assert
        result.Should().Be("default_value");
    }

    [Fact]
    public void Get_WithTypeConversion_ShouldConvertValue()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["count"] = 42L, // Long value
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.Get<int>("count");

        // Assert
        result.Should().Be(42);
    }

    [Theory]
    [InlineData("intValue", 42, 42)]
    [InlineData("boolValue", true, true)]
    [InlineData("doubleValue", 3.14, 3.14)]
    public void Get_WithVariousTypes_ShouldReturnCorrectType<T>(string columnName, T value, T expected)
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            [columnName] = value,
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.Get<T>(columnName);

        // Assert
        result.Should().Be(expected);
    }

    [Fact]
    public void Get_WithOrdinal_ShouldReturnValueByIndex()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["first"] = "value1",
            ["second"] = "value2",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.Get<string>(0);

        // Assert
        result.Should().Be("value1");
    }

    [Fact]
    public void Get_WithInvalidOrdinal_ShouldThrowArgumentOutOfRangeException()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "test",
        };

        var row = new CosmosRow(data);

        // Act & Assert
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => row.Get<string>(5));
        exception.ParamName.Should().Be("ordinal");
    }

    [Fact]
    public void Get_WithNegativeOrdinal_ShouldThrowArgumentOutOfRangeException()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "test",
        };

        var row = new CosmosRow(data);

        // Act & Assert
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => row.Get<string>(-1));
        exception.ParamName.Should().Be("ordinal");
    }

    #endregion

    #region TryGet Tests

    [Fact]
    public void TryGet_WithValidColumn_ShouldReturnTrueAndSetValue()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "TestValue",
        };

        var row = new CosmosRow(data);

        // Act
        var success = row.TryGet<string>("name", out var value);

        // Assert
        success.Should().BeTrue();
        value.Should().Be("TestValue");
    }

    [Fact]
    public void TryGet_WithMissingColumn_ShouldReturnFalse()
    {
        // Arrange
        var data = new Dictionary<string, object?>();
        var row = new CosmosRow(data);

        // Act
        var success = row.TryGet<string>("missing", out var value);

        // Assert
        success.Should().BeFalse();
        value.Should().BeNull();
    }

    [Fact]
    public void TryGet_WithNullValue_ShouldReturnTrue()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = null,
        };

        var row = new CosmosRow(data);

        // Act
        var success = row.TryGet<string>("name", out var value);

        // Assert
        success.Should().BeTrue();
        value.Should().BeNull();
    }

    [Fact]
    public void TryGet_WithEmptyColumnName_ShouldReturnFalse()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "test",
        };

        var row = new CosmosRow(data);

        // Act
        var success = row.TryGet<string>("", out var value);

        // Assert
        success.Should().BeFalse();
    }

    [Fact]
    public void TryGet_WithNullColumnName_ShouldReturnFalse()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "test",
        };

        var row = new CosmosRow(data);

        // Act
        var success = row.TryGet<string>(null!, out var value);

        // Assert
        success.Should().BeFalse();
    }

    [Fact]
    public void TryGet_WithTypeConversionFailure_ShouldReturnFalse()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["text"] = "not_a_number",
        };

        var row = new CosmosRow(data);

        // Act
        var success = row.TryGet<int>("text", out var value);

        // Assert
        success.Should().BeFalse();
    }

    #endregion

    #region GetValue Tests

    [Fact]
    public void GetValue_WithValidColumn_ShouldReturnValue()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "TestValue",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.GetValue("name");

        // Assert
        result.Should().Be("TestValue");
    }

    [Fact]
    public void GetValue_WithMissingColumn_ShouldReturnNull()
    {
        // Arrange
        var data = new Dictionary<string, object?>();
        var row = new CosmosRow(data);

        // Act
        var result = row.GetValue("missing");

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void GetValue_WithNullValue_ShouldReturnNull()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = null,
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.GetValue("name");

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void GetValue_WithCaseInsensitiveMatch_ShouldReturnValue()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["Name"] = "TestValue",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.GetValue("name");

        // Assert
        result.Should().Be("TestValue");
    }

    #endregion

    #region HasColumn Tests

    [Fact]
    public void HasColumn_WithExistingColumn_ShouldReturnTrue()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "test",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.HasColumn("name");

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void HasColumn_WithMissingColumn_ShouldReturnFalse()
    {
        // Arrange
        var data = new Dictionary<string, object?>();
        var row = new CosmosRow(data);

        // Act
        var result = row.HasColumn("missing");

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void HasColumn_WithCaseInsensitiveMatch_ShouldReturnTrue()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["Name"] = "test",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.HasColumn("name");

        // Assert
        result.Should().BeTrue();
    }

    #endregion

    #region ToDictionary Tests

    [Fact]
    public void ToDictionary_WithData_ShouldReturnCorrectDictionary()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["id"] = "123",
            ["name"] = "Test",
            ["count"] = 42,
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.ToDictionary();

        // Assert
        result.Should().ContainKey("id");
        result["id"].Should().Be("123");
        result.Should().ContainKey("name");
        result["name"].Should().Be("Test");
        result.Should().ContainKey("count");
        result["count"].Should().Be(42);
    }

    [Fact]
    public void ToDictionary_WithEmptyRow_ShouldReturnEmptyDictionary()
    {
        // Arrange
        var data = new Dictionary<string, object?>();
        var row = new CosmosRow(data);

        // Act
        var result = row.ToDictionary();

        // Assert
        result.Should().BeEmpty();
    }

    [Fact]
    public void ToDictionary_ShouldReturnCopy()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "test",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.ToDictionary();
        result["newKey"] = "newValue";

        // Assert
        row.HasColumn("newKey").Should().BeFalse();
    }

    #endregion

    #region Indexer Tests

    [Fact]
    public void Indexer_Get_WithValidColumn_ShouldReturnValue()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "TestValue",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row["name"];

        // Assert
        result.Should().Be("TestValue");
    }

    [Fact]
    public void Indexer_Set_WithDictionaryBacking_ShouldSetValue()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = "old",
        };

        var row = new CosmosRow(data);

        // Act
        row["name"] = "new";

        // Assert
        row["name"].Should().Be("new");
    }

    #endregion

    #region JsonElement Tests

    [Fact]
    public void Get_FromJsonElement_ShouldReturnValue()
    {
        // Arrange
        var json = """{"id": "123", "name": "Test", "count": 42}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var id = row.Get<string>("id");
        var name = row.Get<string>("name");
        var count = row.Get<int>("count");

        // Assert
        id.Should().Be("123");
        name.Should().Be("Test");
        count.Should().Be(42);
    }

    [Fact]
    public void TryGet_FromJsonElement_ShouldReturnValue()
    {
        // Arrange
        var json = """{"id": "123", "active": true}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var success = row.TryGet<string>("id", out var value);

        // Assert
        success.Should().BeTrue();
        value.Should().Be("123");
    }

    [Fact]
    public void GetValue_FromJsonElement_ShouldReturnValue()
    {
        // Arrange
        var json = """{"name": "TestValue"}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var result = row.GetValue("name");

        // Assert
        result.Should().Be("TestValue");
    }

    [Fact]
    public void HasColumn_FromJsonElement_ShouldReturnTrue()
    {
        // Arrange
        var json = """{"name": "test"}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var result = row.HasColumn("name");

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void Get_FromJsonElementWithCamelCase_ShouldMatchOriginalCase()
    {
        // Arrange - JSON has PascalCase, query with PascalCase
        var json = """{"Name": "Test"}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var result = row.Get<string>("Name"); // Match the exact case in JSON

        // Assert
        result.Should().Be("Test");
    }

    [Fact]
    public void Get_FromJsonElementWithCamelCaseQuery_ShouldConvertToCamelCase()
    {
        // Arrange - JSON has camelCase, query with PascalCase should still work via camelCase conversion
        var json = """{"name": "Test"}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var result = row.Get<string>("Name"); // PascalCase query

        // Assert
        result.Should().Be("Test"); // Should find via camelCase conversion
    }

    [Fact]
    public void ToDictionary_FromJsonElement_ShouldReturnAllProperties()
    {
        // Arrange
        var json = """{"id": "123", "name": "Test"}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var result = row.ToDictionary();

        // Assert
        result.Should().ContainKey("id");
        result["id"].Should().Be("123");
        result.Should().ContainKey("name");
        result["name"].Should().Be("Test");
    }

    [Fact]
    public void GetDocument_ShouldReturnOriginalJsonElement()
    {
        // Arrange
        var json = """{"id": "123"}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var result = row.GetDocument();

        // Assert
        result.ValueKind.Should().Be(JsonValueKind.Object);
    }

    #endregion

    #region Special Property Access Tests

    [Fact]
    public void GetId_WithIdProperty_ShouldReturnId()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["id"] = "doc-123",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.GetId();

        // Assert
        result.Should().Be("doc-123");
    }

    [Fact]
    public void GetId_WithIdAsProperty_ShouldReturnId()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["Id"] = "doc-456",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.GetId();

        // Assert
        result.Should().Be("doc-456");
    }

    [Fact]
    public void GetId_WithUnderscoreId_ShouldReturnId()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["_id"] = "doc-789",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.GetId();

        // Assert
        result.Should().Be("doc-789");
    }

    [Fact]
    public void GetPartitionKey_WithPartitionKeyProperty_ShouldReturnValue()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["partitionKey"] = "pk-123",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.GetPartitionKey();

        // Assert
        result.Should().Be("pk-123");
    }

    [Fact]
    public void GetTimestamp_WithTsProperty_ShouldReturnDateTime()
    {
        // Arrange
        var expectedTimestamp = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var unixTimestamp = ((DateTimeOffset)expectedTimestamp).ToUnixTimeSeconds();

        var data = new Dictionary<string, object?>
        {
            ["_ts"] = unixTimestamp,
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.GetTimestamp();

        // Assert
        result.Should().BeCloseTo(expectedTimestamp, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void GetTimestamp_WithZeroTs_ShouldReturnNull()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["_ts"] = 0L,
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.GetTimestamp();

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void GetEtag_WithEtagProperty_ShouldReturnValue()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["_etag"] = "\"etag-value\"",
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.GetEtag();

        // Assert
        result.Should().Be("\"etag-value\"");
    }

    #endregion

    #region Null Value Handling Tests

    [Fact]
    public void Get_WithNullValueInDictionary_ShouldReturnDefault()
    {
        // Arrange
        var data = new Dictionary<string, object?>
        {
            ["name"] = null,
        };

        var row = new CosmosRow(data);

        // Act
        var result = row.Get<string>("name");

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void Get_WithNullJsonValue_ShouldReturnDefault()
    {
        // Arrange
        var json = """{"name": null}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var result = row.Get<string>("name");

        // Assert
        result.Should().BeNull();
    }

    #endregion

    #region Complex Type Tests

    [Fact]
    public void GetValue_WithNestedObject_ShouldReturnDictionary()
    {
        // Arrange
        var json = """{"nested": {"key": "value"}}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var result = row.GetValue("nested");

        // Assert
        result.Should().BeAssignableTo<Dictionary<string, object?>>();
    }

    [Fact]
    public void GetValue_WithArray_ShouldReturnList()
    {
        // Arrange
        var json = """{"items": [1, 2, 3]}""";
        var document = JsonDocument.Parse(json).RootElement;
        var row = new CosmosRow(document);

        // Act
        var result = row.GetValue("items");

        // Assert
        result.Should().BeAssignableTo<IList>();
    }

    #endregion
}

using FakeItEasy;
using NPipeline.Connectors.Aws.Redshift.Mapping;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Mapping;

public class RedshiftMapperBuilderTests : IDisposable
{
    public RedshiftMapperBuilderTests()
    {
        RedshiftMapperBuilder.ClearCache();
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        RedshiftMapperBuilder.ClearCache();
    }

    [Fact]
    public void Build_DefaultMapper_MapsAllProperties()
    {
        // Arrange
        var reader = CreateFakeReader(new Dictionary<string, object?>
        {
            ["id"] = 1,
            ["name"] = "Test",
            ["price"] = 99.99m,
            ["quantity"] = 10,
            ["created_at"] = new DateTime(2024, 1, 1),
            ["is_active"] = true,
        });

        var row = new RedshiftRow(reader);

        // Act
        var mapper = RedshiftMapperBuilder.Build<TestPoco>();
        var result = mapper(row);

        // Assert
        result.Id.Should().Be(1);
        result.Name.Should().Be("Test");
        result.Price.Should().Be(99.99m);
        result.Quantity.Should().Be(10);
        result.CreatedAt.Should().Be(new DateTime(2024, 1, 1));
        result.IsActive.Should().BeTrue();
    }

    [Fact]
    public void Build_WithColumnAttribute_UsesAttributeName()
    {
        // Arrange
        var reader = CreateFakeReader(new Dictionary<string, object?>
        {
            ["custom_id"] = 42,
        });

        var row = new RedshiftRow(reader);

        // Act
        var mapper = RedshiftMapperBuilder.Build<AttributeTestClass>();
        var result = mapper(row);

        // Assert
        result.CustomId.Should().Be(42);
    }

    [Fact]
    public void Build_WithIgnoreColumnAttribute_SkipsProperty()
    {
        // Arrange
        var reader = CreateFakeReader(new Dictionary<string, object?>
        {
            ["id"] = 1,
            ["ignored"] = "should not map",
        });

        var row = new RedshiftRow(reader);

        // Act
        var mapper = RedshiftMapperBuilder.Build<IgnoreTestClass>();
        var result = mapper(row);

        // Assert
        result.Id.Should().Be(1);
        result.Ignored.Should().BeNull(); // Should be default value
    }

    [Fact]
    public void Build_CachesMapper_SameTypeReturnsSameDelegate()
    {
        // Act
        var mapper1 = RedshiftMapperBuilder.Build<TestPoco>();
        var mapper2 = RedshiftMapperBuilder.Build<TestPoco>();

        // Assert
        mapper1.Should().BeSameAs(mapper2);
    }

    [Fact]
    public void Build_NullableValueType_HandlesNull()
    {
        // Arrange
        var reader = CreateFakeReader(new Dictionary<string, object?>
        {
            ["nullable_int"] = null,
        });

        var row = new RedshiftRow(reader);

        // Act
        var mapper = RedshiftMapperBuilder.Build<NullableTestClass>();
        var result = mapper(row);

        // Assert
        result.NullableInt.Should().BeNull();
    }

    [Fact]
    public void Build_NullableValueType_WithValue_ReturnsValue()
    {
        // Arrange
        var reader = CreateFakeReader(new Dictionary<string, object?>
        {
            ["nullable_int"] = 42,
        });

        var row = new RedshiftRow(reader);

        // Act
        var mapper = RedshiftMapperBuilder.Build<NullableTestClass>();
        var result = mapper(row);

        // Assert
        result.NullableInt.Should().Be(42);
    }

    [Fact]
    public void Build_MissingColumn_UsesDefaultValue()
    {
        // Arrange
        var reader = CreateFakeReader(new Dictionary<string, object?>
        {
            ["id"] = 1,

            // name column is missing
        });

        var row = new RedshiftRow(reader);

        // Act
        var mapper = RedshiftMapperBuilder.Build<TestPoco>();
        var result = mapper(row);

        // Assert
        result.Id.Should().Be(1);
        result.Name.Should().BeEmpty(); // Default value for string
    }

    private static IDatabaseReader CreateFakeReader(Dictionary<string, object?> values)
    {
        var reader = A.Fake<IDatabaseReader>();
        var columnNames = values.Keys.ToList();

        A.CallTo(() => reader.FieldCount).Returns(columnNames.Count);

        for (var i = 0; i < columnNames.Count; i++)
        {
            var index = i;
            var columnName = columnNames[i];
            var value = values[columnName];

            A.CallTo(() => reader.GetName(index)).Returns(columnName);
            A.CallTo(() => reader.IsDBNull(index)).Returns(value == null);

            // Set up GetFieldValue for the specific type
            if (value != null)
            {
                var valueType = value.GetType();
                var getFieldValueMethod = typeof(IDatabaseReader).GetMethod("GetFieldValue")!.MakeGenericMethod(valueType);
                A.CallTo(() => reader.GetFieldValue<object>(index)).Returns(value);

                // Handle specific types
                if (value is int intValue)
                    A.CallTo(() => reader.GetFieldValue<int>(index)).Returns(intValue);
                else if (value is string stringValue)
                    A.CallTo(() => reader.GetFieldValue<string>(index)).Returns(stringValue);
                else if (value is decimal decimalValue)
                    A.CallTo(() => reader.GetFieldValue<decimal>(index)).Returns(decimalValue);
                else if (value is DateTime dateTimeValue)
                    A.CallTo(() => reader.GetFieldValue<DateTime>(index)).Returns(dateTimeValue);
                else if (value is bool boolValue)
                    A.CallTo(() => reader.GetFieldValue<bool>(index)).Returns(boolValue);
            }
        }

        return reader;
    }

    private sealed class TestPoco
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public int Quantity { get; set; }
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
    }

    private sealed class AttributeTestClass
    {
        [RedshiftColumn("custom_id")]
        public int CustomId { get; set; }
    }

    private sealed class IgnoreTestClass
    {
        public int Id { get; set; }

        [RedshiftColumn("ignored", Ignore = true)]
        public string? Ignored { get; set; }
    }

    private sealed class NullableTestClass
    {
        public int? NullableInt { get; set; }
    }
}

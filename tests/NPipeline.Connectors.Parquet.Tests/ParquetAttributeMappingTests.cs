using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.Connectors.Parquet.Mapping;

namespace NPipeline.Connectors.Parquet.Tests;

public sealed class ParquetAttributeMappingTests
{
    #region Write Path Mapper Tests

    [Fact]
    public void GetColumnNames_ForSimpleRecord_ReturnsCorrectColumnNames()
    {
        // Arrange & Act
        var columnNames = ParquetWriterMapperBuilder.GetColumnNames<SimpleRecord>();

        // Assert
        columnNames.Should().BeEquivalentTo(["Id", "Name", "IsActive"]);
    }

    [Fact]
    public void GetColumnNames_ForRecordWithOverrides_ReturnsOverriddenNames()
    {
        // Arrange & Act
        var columnNames = ParquetWriterMapperBuilder.GetColumnNames<ColumnNameOverrideRecord>();

        // Assert
        columnNames.Should().BeEquivalentTo(["column_id", "column_name"]);
    }

    [Fact]
    public void GetValueGetters_ForSimpleRecord_ExtractsCorrectValues()
    {
        // Arrange
        var record = new SimpleRecord { Id = 789, Name = "Test Record", IsActive = true };
        var getters = ParquetWriterMapperBuilder.GetValueGetters<SimpleRecord>();

        // Act
        var idValue = getters[0](record);
        var nameValue = getters[1](record);
        var isActiveValue = getters[2](record);

        // Assert
        idValue.Should().Be(789);
        nameValue.Should().Be("Test Record");
        isActiveValue.Should().Be(true);
    }

    [Fact]
    public void GetValueGetters_ForGuidRecord_ConvertsToString()
    {
        // Arrange
        var guid = Guid.Parse("A1B2C3D4-E5F6-7890-ABCD-EF1234567890");
        var record = new GuidRecord { Id = guid };
        var getters = ParquetWriterMapperBuilder.GetValueGetters<GuidRecord>();

        // Act
        var value = getters[0](record);

        // Assert
        value.Should().Be(guid.ToString());
    }

    [Fact]
    public void GetValueGetters_ForEnumRecord_ConvertsToString()
    {
        // Arrange
        var record = new EnumRecord { Status = TestStatus.Active };
        var getters = ParquetWriterMapperBuilder.GetValueGetters<EnumRecord>();

        // Act
        var value = getters[0](record);

        // Assert
        value.Should().Be("Active");
    }

    [Fact]
    public void GetValueGetters_ForNullableGuidRecord_ConvertsToStringOrNull()
    {
        // Arrange
        var guid = Guid.Parse("A1B2C3D4-E5F6-7890-ABCD-EF1234567890");
        var recordWithValue = new NullableGuidRecord { Id = guid };
        var recordWithNull = new NullableGuidRecord { Id = null };
        var getters = ParquetWriterMapperBuilder.GetValueGetters<NullableGuidRecord>();

        // Act
        var valueWithGuid = getters[0](recordWithValue);
        var valueWithNull = getters[0](recordWithNull);

        // Assert
        valueWithGuid.Should().Be(guid.ToString());
        valueWithNull.Should().BeNull();
    }

    [Fact]
    public void GetProperties_ForSimpleRecord_ReturnsAllWritableProperties()
    {
        // Arrange & Act
        var properties = ParquetWriterMapperBuilder.GetProperties<SimpleRecord>();

        // Assert
        properties.Select(p => p.Name).Should().BeEquivalentTo(["Id", "Name", "IsActive"]);
    }

    [Fact]
    public void GetProperties_ForIgnoredPropertyRecord_ExcludesIgnoredProperties()
    {
        // Arrange & Act
        var properties = ParquetWriterMapperBuilder.GetProperties<IgnoredPropertyRecord>();

        // Assert
        properties.Select(p => p.Name).Should().BeEquivalentTo(["Included"]);
    }

    [Fact]
    public void GetColumnToPropertyMap_ForRecord_MapsCorrectly()
    {
        // Arrange & Act
        var map = ParquetWriterMapperBuilder.GetColumnToPropertyMap<SimpleRecord>();

        // Assert
        map.Should().HaveCount(3);
        map["Id"].Name.Should().Be("Id");
        map["Name"].Name.Should().Be("Name");
        map["IsActive"].Name.Should().Be("IsActive");
    }

    #endregion

    #region Schema Builder Tests for Attributes

    [Fact]
    public void SchemaBuilder_ForRecordWithColumnNameOverride_UsesOverriddenName()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<ColumnNameOverrideRecord>();

        // Assert
        schema.Fields.Should().HaveCount(2);
        schema.Fields[0].Name.Should().Be("column_id");
        schema.Fields[1].Name.Should().Be("column_name");
    }

    [Fact]
    public void SchemaBuilder_ForIgnoredPropertyRecord_ExcludesIgnoredProperties()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<IgnoredPropertyRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        schema.Fields[0].Name.Should().Be("Included");
    }

    [Fact]
    public void SchemaBuilder_ForRecordWithColumnAttributeFallback_UsesColumnName()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<ColumnAttributeRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        schema.Fields[0].Name.Should().Be("fallback_column_name");
    }

    #endregion

    #region Test Record Types

    private sealed class SimpleRecord
    {
        public long Id { get; set; }
        public string? Name { get; set; }
        public bool IsActive { get; set; }
    }

    private sealed class ColumnNameOverrideRecord
    {
        [ParquetColumn("column_id")]
        public long Id { get; set; }

        [ParquetColumn("column_name")]
        public string? Name { get; set; }
    }

    private sealed class NullableRecord
    {
        public int Id { get; set; }
        public int? OptionalValue { get; set; }
        public string? OptionalName { get; set; }
    }

    private sealed class IgnoredPropertyRecord
    {
        public string? Included { get; set; }

        [ParquetColumn(Ignore = true)]
        public string? Excluded { get; set; }
    }

    private sealed class GuidRecord
    {
        public Guid Id { get; set; }
    }

    private sealed class NullableGuidRecord
    {
        public Guid? Id { get; set; }
    }

    private sealed class EnumRecord
    {
        public TestStatus Status { get; set; }
    }

    private sealed class ColumnAttributeRecord
    {
        [NPipeline.Connectors.Attributes.Column("fallback_column_name")]
        public string? Value { get; set; }
    }

    private enum TestStatus
    {
        Active,
        Inactive,
        Pending
    }

    #endregion
}

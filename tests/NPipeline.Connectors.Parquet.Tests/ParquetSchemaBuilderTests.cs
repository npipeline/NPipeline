using System.Drawing;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.Connectors.Parquet.Mapping;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet.Tests;

public sealed class ParquetSchemaBuilderTests
{
    #region Multiple Properties

    [Fact]
    public void Build_WithMultipleProperties_CreatesAllFields()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<MultiPropertyRecord>();

        // Assert
        schema.Fields.Should().HaveCount(5);
        schema.Fields.Select(f => f.Name).Should().BeEquivalentTo("Id", "Name", "Amount", "CreatedDate", "IsActive");
    }

    #endregion

    #region Supported CLR Types

    [Fact]
    public void Build_WithStringProperty_CreatesStringDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<StringRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<string>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithIntProperty_CreatesIntDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<IntRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<int>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithLongProperty_CreatesLongDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<LongRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<long>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithShortProperty_CreatesShortDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<ShortRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<short>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithByteProperty_CreatesByteDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<ByteRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<byte>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithFloatProperty_CreatesFloatDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<FloatRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<float>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithDoubleProperty_CreatesDoubleDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<DoubleRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<double>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithBoolProperty_CreatesBoolDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<BoolRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<bool>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithDecimalProperty_CreatesDecimalDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<DecimalRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DecimalDataField;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
        field!.Precision.Should().Be(18);
        field!.Scale.Should().Be(4);
    }

    [Fact]
    public void Build_WithDateTimeProperty_CreatesDateTimeDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<DateTimeRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<DateTime>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithDateTimeOffsetProperty_CreatesDateTimeDataField()
    {
        // DateTimeOffset is stored as DateTime (UTC) in Parquet since Parquet.Net doesn't support DateTimeOffset directly.
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<DateTimeOffsetRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<DateTime>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithGuidProperty_CreatesStringDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<GuidRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<string>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithByteArrayProperty_CreatesByteArrayDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<ByteArrayRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<byte[]>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithEnumProperty_CreatesStringDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<EnumRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<string>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    #endregion

    #region Nullable Variants

    [Fact]
    public void Build_WithNullableIntProperty_CreatesNullableIntDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<NullableIntRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<int>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithNullableLongProperty_CreatesNullableLongDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<NullableLongRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<long>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithNullableDoubleProperty_CreatesNullableDoubleDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<NullableDoubleRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<double>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithNullableBoolProperty_CreatesNullableBoolDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<NullableBoolRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<bool>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithNullableDecimalProperty_CreatesNullableDecimalDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<NullableDecimalRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DecimalDataField;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithNullableDateTimeProperty_CreatesNullableDateTimeDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<NullableDateTimeRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<DateTime>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    [Fact]
    public void Build_WithNullableDateTimeOffsetProperty_CreatesNullableDateTimeDataField()
    {
        // Nullable DateTimeOffset is stored as nullable DateTime (UTC) in Parquet since Parquet.Net doesn't support DateTimeOffset directly.
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<NullableDateTimeOffsetRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<DateTime>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
        field.IsNullable.Should().BeTrue();
    }

    [Fact]
    public void Build_WithNullableGuidProperty_CreatesNullableStringDataField()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<NullableGuidRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DataField<string>;
        field.Should().NotBeNull();
        field!.Name.Should().Be("Value");
    }

    #endregion

    #region ParquetColumnAttribute Name Overrides

    [Fact]
    public void Build_WithParquetColumnNameOverride_UsesOverriddenName()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<ColumnNameOverrideRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        schema.Fields[0].Name.Should().Be("column_name_in_parquet");
    }

    [Fact]
    public void Build_WithParquetColumnAttributeNoName_UsesPropertyName()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<ParquetColumnNoNameRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        schema.Fields[0].Name.Should().Be("Value");
    }

    #endregion

    #region ColumnAttribute Fallback

    [Fact]
    public void Build_WithColumnAttributeFallback_UsesColumnName()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<ColumnAttributeRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        schema.Fields[0].Name.Should().Be("fallback_column_name");
    }

    [Fact]
    public void Build_WithParquetColumnAndColumnAttribute_PrefersParquetColumn()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<BothAttributesRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        schema.Fields[0].Name.Should().Be("parquet_name");
    }

    #endregion

    #region ParquetDecimalAttribute Required Enforcement

    [Fact]
    public void Build_DecimalWithoutParquetDecimalAttribute_ThrowsParquetSchemaException()
    {
        // Arrange & Act
        var act = () => ParquetSchemaBuilder.Build<DecimalWithoutAttributeRecord>();

        // Assert
        act.Should().Throw<ParquetSchemaException>()
            .WithMessage("*decimal*ParquetDecimalAttribute*");
    }

    [Fact]
    public void Build_WithParquetDecimalAttribute_UsesSpecifiedPrecisionAndScale()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<CustomDecimalRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        var field = schema.Fields[0] as DecimalDataField;
        field.Should().NotBeNull();
        field!.Precision.Should().Be(28);
        field!.Scale.Should().Be(8);
    }

    #endregion

    #region Unsupported Type Exceptions

    [Fact]
    public void Build_WithUnsupportedType_ThrowsParquetSchemaException()
    {
        // Arrange & Act
        var act = () => ParquetSchemaBuilder.Build<UnsupportedTypeRecord>();

        // Assert
        act.Should().Throw<ParquetSchemaException>()
            .WithMessage("*Unsupported type*");
    }

    [Fact]
    public void Build_WithUnsupportedComplexType_ThrowsParquetSchemaException()
    {
        // Arrange & Act
        var act = () => ParquetSchemaBuilder.Build<ComplexTypeRecord>();

        // Assert
        act.Should().Throw<ParquetSchemaException>()
            .WithMessage("*Unsupported type*");
    }

    #endregion

    #region Ignore Attribute

    [Fact]
    public void Build_WithIgnoredProperty_ExcludesPropertyFromSchema()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<IgnoredPropertyRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        schema.Fields[0].Name.Should().Be("Included");
    }

    [Fact]
    public void Build_WithIgnoreColumnAttribute_ExcludesPropertyFromSchema()
    {
        // Arrange & Act
        var schema = ParquetSchemaBuilder.Build<IgnoreColumnAttributeRecord>();

        // Assert
        schema.Fields.Should().HaveCount(1);
        schema.Fields[0].Name.Should().Be("Included");
    }

    #endregion

    #region Test Record Types

    private sealed class StringRecord
    {
        public string? Value { get; set; }
    }

    private sealed class IntRecord
    {
        public int Value { get; set; }
    }

    private sealed class LongRecord
    {
        public long Value { get; set; }
    }

    private sealed class ShortRecord
    {
        public short Value { get; set; }
    }

    private sealed class ByteRecord
    {
        public byte Value { get; set; }
    }

    private sealed class FloatRecord
    {
        public float Value { get; set; }
    }

    private sealed class DoubleRecord
    {
        public double Value { get; set; }
    }

    private sealed class BoolRecord
    {
        public bool Value { get; set; }
    }

    private sealed class DateTimeRecord
    {
        public DateTime Value { get; set; }
    }

    private sealed class DateTimeOffsetRecord
    {
        public DateTimeOffset Value { get; set; }
    }

    private sealed class GuidRecord
    {
        public Guid Value { get; set; }
    }

    private sealed class ByteArrayRecord
    {
        public byte[]? Value { get; set; }
    }

    private sealed class EnumRecord
    {
        public TestEnum Value { get; set; }
    }

    private sealed class DecimalRecord
    {
        [ParquetDecimal(18, 4)]
        public decimal Value { get; set; }
    }

    private sealed class NullableIntRecord
    {
        public int? Value { get; set; }
    }

    private sealed class NullableLongRecord
    {
        public long? Value { get; set; }
    }

    private sealed class NullableDoubleRecord
    {
        public double? Value { get; set; }
    }

    private sealed class NullableBoolRecord
    {
        public bool? Value { get; set; }
    }

    private sealed class NullableDateTimeRecord
    {
        public DateTime? Value { get; set; }
    }

    private sealed class NullableDateTimeOffsetRecord
    {
        public DateTimeOffset? Value { get; set; }
    }

    private sealed class NullableGuidRecord
    {
        public Guid? Value { get; set; }
    }

    private sealed class NullableDecimalRecord
    {
        [ParquetDecimal(18, 4)]
        public decimal? Value { get; set; }
    }

    private sealed class ColumnNameOverrideRecord
    {
        [ParquetColumn("column_name_in_parquet")]
        public string? Value { get; set; }
    }

    private sealed class ParquetColumnNoNameRecord
    {
        [ParquetColumn]
        public string? Value { get; set; }
    }

    private sealed class ColumnAttributeRecord
    {
        [Column("fallback_column_name")]
        public string? Value { get; set; }
    }

    private sealed class BothAttributesRecord
    {
        [ParquetColumn("parquet_name")]
        [Column("column_name")]
        public string? Value { get; set; }
    }

    private sealed class DecimalWithoutAttributeRecord
    {
        public decimal Value { get; set; }
    }

    private sealed class CustomDecimalRecord
    {
        [ParquetDecimal(28, 8)]
        public decimal Value { get; set; }
    }

    private sealed class UnsupportedTypeRecord
    {
        public Point Value { get; set; } = default!;
    }

    private sealed class ComplexTypeRecord
    {
        public NestedRecord? Nested { get; set; }
    }

    private sealed class NestedRecord
    {
        public string? Name { get; set; }
    }

    private sealed class IgnoredPropertyRecord
    {
        public string? Included { get; set; }

        [ParquetColumn(Ignore = true)]
        public string? Excluded { get; set; }
    }

    private sealed class IgnoreColumnAttributeRecord
    {
        public string? Included { get; set; }

        [IgnoreColumn]
        public string? Excluded { get; set; }
    }

    private sealed class MultiPropertyRecord
    {
        public int Id { get; set; }
        public string? Name { get; set; }

        [ParquetDecimal(10, 2)]
        public decimal Amount { get; set; }

        public DateTime CreatedDate { get; set; }
        public bool IsActive { get; set; }
    }

    private enum TestEnum
    {
        Value1,
        Value2,
        Value3,
    }

    #endregion
}

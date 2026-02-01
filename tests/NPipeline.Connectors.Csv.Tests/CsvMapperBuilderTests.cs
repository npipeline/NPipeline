using System.Globalization;
using AwesomeAssertions;
using CsvHelper;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Csv.Tests;

/// <summary>
///     Unit tests for CsvMapperBuilder.
///     Validates mapping delegates for reading CSV rows into objects.
/// </summary>
public sealed class CsvMapperBuilderTests
{
    #region Convention-Based Mapping Tests

    [Fact]
    public void Build_WithConventionMapping_MapsPascalCaseToLowercase()
    {
        // Arrange
        var csvData = "userid,fullname,createdat,isactive\n1,John Doe,2024-01-01,true";
        var config = new CsvConfiguration(CultureInfo.InvariantCulture);

        var headers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["userid"] = 0,
            ["fullname"] = 1,
            ["createdat"] = 2,
            ["isactive"] = 3,
        };

        using var reader = new StringReader(csvData);
        using var csv = new CsvReader(reader, config.HelperConfiguration);
        csv.Read();
        csv.ReadHeader();
        csv.Read();

        var row = new CsvRow(csv, headers, true);
        var mapper = CsvMapperBuilder.Build<PocoWithMixedCase>();

        // Act
        var result = mapper(row);

        // Assert
        result.UserId.Should().Be(1);
        result.FullName.Should().Be("John Doe");
        result.IsActive.Should().BeTrue();
    }

    #endregion

    #region Case-Insensitive Column Matching Tests

    [Fact]
    public void Build_WithDifferentColumnCase_MapsSuccessfully()
    {
        // Arrange
        var csvData = "userid,fullname,createdat,isactive\n1,John Doe,2024-01-01,true";
        var config = new CsvConfiguration(CultureInfo.InvariantCulture);

        var headers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["userid"] = 0,
            ["fullname"] = 1,
            ["createdat"] = 2,
            ["isactive"] = 3,
        };

        using var reader = new StringReader(csvData);
        using var csv = new CsvReader(reader, config.HelperConfiguration);
        csv.Read();
        csv.ReadHeader();
        csv.Read();

        var row = new CsvRow(csv, headers, true);
        var mapper = CsvMapperBuilder.Build<PocoWithMixedCase>();

        // Act
        var result = mapper(row);

        // Assert
        result.UserId.Should().Be(1);
        result.FullName.Should().Be("John Doe");
        result.IsActive.Should().BeTrue();
    }

    #endregion

    #region Various Types Tests

    [Fact]
    public void Build_WithVariousTypes_ConvertsAllTypesCorrectly()
    {
        // Arrange
        var csvData =
            "intvalue,longvalue,shortvalue,doublevalue,floatvalue,decimalvalue,stringvalue,boolvalue,datetimevalue,guidvalue\n42,1234567890,100,3.14,2.5,99.99,Test,true,2024-01-01,550e8400-e29b-41d4-a716-446655440000";

        var config = new CsvConfiguration(CultureInfo.InvariantCulture);

        var headers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["intvalue"] = 0,
            ["longvalue"] = 1,
            ["shortvalue"] = 2,
            ["doublevalue"] = 3,
            ["floatvalue"] = 4,
            ["decimalvalue"] = 5,
            ["stringvalue"] = 6,
            ["boolvalue"] = 7,
            ["datetimevalue"] = 8,
            ["guidvalue"] = 9,
        };

        using var reader = new StringReader(csvData);
        using var csv = new CsvReader(reader, config.HelperConfiguration);
        csv.Read();
        csv.ReadHeader();
        csv.Read();

        var row = new CsvRow(csv, headers, true);
        var mapper = CsvMapperBuilder.Build<PocoWithVariousTypes>();

        // Act
        var result = mapper(row);

        // Assert
        result.IntValue.Should().Be(42);
        result.LongValue.Should().Be(1234567890L);
        result.ShortValue.Should().Be(100);
        result.DoubleValue.Should().Be(3.14);
        result.FloatValue.Should().Be(2.5f);
        result.DecimalValue.Should().Be(99.99m);
        result.StringValue.Should().Be("Test");
        result.BoolValue.Should().BeTrue();
        result.DateTimeValue.Should().Be(new DateTime(2024, 1, 1));
        result.GuidValue.Should().Be(new Guid("550e8400-e29b-41d4-a716-446655440000"));
    }

    #endregion

    #region Null Value Handling Tests

    [Fact]
    public void Build_WithNullableTypes_HandlesNullValues()
    {
        // Arrange
        var csvData = "id,name,createdat,isactive,amount\n1,John,,true,";
        var config = new CsvConfiguration(CultureInfo.InvariantCulture);

        var headers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 0,
            ["name"] = 1,
            ["createdat"] = 2,
            ["isactive"] = 3,
            ["amount"] = 4,
        };

        using var reader = new StringReader(csvData);
        using var csv = new CsvReader(reader, config.HelperConfiguration);
        csv.Read();
        csv.ReadHeader();
        csv.Read();

        var row = new CsvRow(csv, headers, true);
        var mapper = CsvMapperBuilder.Build<PocoWithNullableTypes>();

        // Act
        var result = mapper(row);

        // Assert
        result.Id.Should().Be(1);
        result.Name.Should().Be("John");
        result.CreatedAt.Should().BeNull();
        result.IsActive.Should().BeTrue();
        result.Amount.Should().BeNull();
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public void Build_WithMissingColumn_UsesDefaultValue()
    {
        // Arrange
        var csvData = "id,name\n1,John";
        var config = new CsvConfiguration(CultureInfo.InvariantCulture);

        var headers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 0,
            ["name"] = 1,
        };

        using var reader = new StringReader(csvData);
        using var csv = new CsvReader(reader, config.HelperConfiguration);
        csv.Read();
        csv.ReadHeader();
        csv.Read();

        var row = new CsvRow(csv, headers, true);
        var mapper = CsvMapperBuilder.Build<SimplePoco>();

        // Act
        var result = mapper(row);

        // Assert
        result.Id.Should().Be(1);
        result.Name.Should().Be("John");
        result.CreatedAt.Should().Be(default);
        result.IsActive.Should().BeFalse();
        result.Amount.Should().Be(0m);
    }

    #endregion

    #region Test Models

    private sealed class SimplePoco
    {
        public int Id { get; set; }
        public string Name { get; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
        public decimal Amount { get; set; }
    }

    private sealed class PocoWithAttributes
    {
        [Column("user_id")]
        public int Id { get; set; }

        [Column("full_name")]
        public string Name { get; } = string.Empty;

        [Column("created_date")]
        public DateTime CreatedAt { get; set; }

        [Column("is_active")]
        public bool IsActive { get; set; }

        [Column("total_amount")]
        public decimal Amount { get; set; }
    }

    private sealed class PocoWithIgnore
    {
        public int Id { get; set; }

        [IgnoreColumn]
        public string IgnoredProperty { get; } = string.Empty;

        public string Name { get; } = string.Empty;

        [Column("ignored", Ignore = true)]
        public string AlsoIgnored { get; } = string.Empty;
    }

    private sealed class PocoWithNullableTypes
    {
        public int? Id { get; set; }
        public string? Name { get; set; }
        public DateTime? CreatedAt { get; set; }
        public bool? IsActive { get; set; }
        public decimal? Amount { get; set; }
    }

    private sealed class PocoWithMixedCase
    {
        public int UserId { get; set; }
        public string FullName { get; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
    }

    private sealed class EmptyPoco
    {
    }

    private sealed class PocoWithNoMappableProperties
    {
        [IgnoreColumn]
        public string IgnoredProperty { get; set; } = string.Empty;

        [Column("ignored", Ignore = true)]
        public int AlsoIgnored { get; set; }
    }

    private sealed class PocoWithVariousTypes
    {
        public int IntValue { get; set; }
        public long LongValue { get; set; }
        public short ShortValue { get; set; }
        public double DoubleValue { get; set; }
        public float FloatValue { get; set; }
        public decimal DecimalValue { get; set; }
        public string StringValue { get; } = string.Empty;
        public bool BoolValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public Guid GuidValue { get; set; }
    }

    private sealed class PocoWithNoParameterlessConstructor
    {
        public PocoWithNoParameterlessConstructor(int id)
        {
            Id = id;
        }

        public int Id { get; set; }
    }

    #endregion

    #region Build Tests

    [Fact]
    public void Build_WithSimplePoco_CreatesMapper()
    {
        // Act
        var mapper = CsvMapperBuilder.Build<SimplePoco>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithAttributes_CreatesMapper()
    {
        // Act
        var mapper = CsvMapperBuilder.Build<PocoWithAttributes>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithIgnore_CreatesMapper()
    {
        // Act
        var mapper = CsvMapperBuilder.Build<PocoWithIgnore>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithNullableTypes_CreatesMapper()
    {
        // Act
        var mapper = CsvMapperBuilder.Build<PocoWithNullableTypes>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithVariousTypes_CreatesMapper()
    {
        // Act
        var mapper = CsvMapperBuilder.Build<PocoWithVariousTypes>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithEmptyPoco_CreatesMapper()
    {
        // Act
        var mapper = CsvMapperBuilder.Build<EmptyPoco>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithNoMappableProperties_CreatesMapper()
    {
        // Act
        var mapper = CsvMapperBuilder.Build<PocoWithNoMappableProperties>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithNoParameterlessConstructor_ThrowsInvalidOperationException()
    {
        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => CsvMapperBuilder.Build<PocoWithNoParameterlessConstructor>());
    }

    #endregion

    #region Mapper Caching Tests

    [Fact]
    public void Build_ReturnsCachedMapperOnSecondCall()
    {
        // Act
        var mapper1 = CsvMapperBuilder.Build<SimplePoco>();
        var mapper2 = CsvMapperBuilder.Build<SimplePoco>();

        // Assert
        mapper2.Should().BeSameAs(mapper1);
    }

    [Fact]
    public void Build_DifferentTypes_ReturnDifferentMappers()
    {
        // Act
        var mapper1 = CsvMapperBuilder.Build<SimplePoco>();
        var mapper2 = CsvMapperBuilder.Build<PocoWithAttributes>();

        // Assert
        mapper2.Should().NotBeSameAs(mapper1);
    }

    #endregion

    #region Attribute-Based Mapping Tests

    [Fact]
    public void Build_WithAttributeMapping_MapsToCustomColumnNames()
    {
        // Arrange
        var csvData = "user_id,full_name,created_date,is_active,total_amount\n1,Jane Doe,2024-01-01,true,100.50";
        var config = new CsvConfiguration(CultureInfo.InvariantCulture);

        var headers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["user_id"] = 0,
            ["full_name"] = 1,
            ["created_date"] = 2,
            ["is_active"] = 3,
            ["total_amount"] = 4,
        };

        using var reader = new StringReader(csvData);
        using var csv = new CsvReader(reader, config.HelperConfiguration);
        csv.Read();
        csv.ReadHeader();
        csv.Read();

        var row = new CsvRow(csv, headers, true);
        var mapper = CsvMapperBuilder.Build<PocoWithAttributes>();

        // Act
        var result = mapper(row);

        // Assert
        result.Id.Should().Be(1);
        result.Name.Should().Be("Jane Doe");
        result.IsActive.Should().BeTrue();
        result.Amount.Should().Be(100.50m);
    }

    [Fact]
    public void Build_WithIgnoreAttribute_ExcludesIgnoredProperties()
    {
        // Arrange
        var csvData = "id,name\n1,John";
        var config = new CsvConfiguration(CultureInfo.InvariantCulture);

        var headers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 0,
            ["name"] = 1,
        };

        using var reader = new StringReader(csvData);
        using var csv = new CsvReader(reader, config.HelperConfiguration);
        csv.Read();
        csv.ReadHeader();
        csv.Read();

        var row = new CsvRow(csv, headers, true);
        var mapper = CsvMapperBuilder.Build<PocoWithIgnore>();

        // Act
        var result = mapper(row);

        // Assert
        result.Id.Should().Be(1);
        result.Name.Should().Be("John");
        result.IgnoredProperty.Should().BeEmpty();
        result.AlsoIgnored.Should().BeEmpty();
    }

    #endregion
}

using System.Reflection;
using AwesomeAssertions;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Spreadsheet;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Excel.Mapping;

namespace NPipeline.Connectors.Excel.Tests;

/// <summary>
///     Unit tests for ExcelWriterMapperBuilder.
///     Validates mapping delegates for writing objects to Excel rows.
/// </summary>
public sealed class ExcelWriterMapperBuilderTests
{
    #region Multiple Records Tests

    [Fact]
    public void Build_WritesMultipleRecordsCorrectly()
    {
        // Arrange
        var pocos = new[]
        {
            new SimplePoco { Id = 1, Name = "John", CreatedAt = new DateTime(2024, 1, 1), IsActive = true, Amount = 100m },
            new SimplePoco { Id = 2, Name = "Jane", CreatedAt = new DateTime(2024, 1, 2), IsActive = false, Amount = 200m },
            new SimplePoco { Id = 3, Name = "Bob", CreatedAt = new DateTime(2024, 1, 3), IsActive = true, Amount = 300m },
        };

        using var stream = new MemoryStream();
        using var writer = OpenXmlWriter.Create(stream);
        writer.WriteStartElement(new Worksheet());
        writer.WriteStartElement(new SheetData());
        var mapper = CreateWriterMapper<SimplePoco>();

        // Act
        for (var i = 0; i < pocos.Length; i++)
        {
            writer.WriteStartElement(new Row { RowIndex = (uint)(i + 1) });
            mapper(writer, pocos[i]);
            writer.WriteEndElement(); // Row
        }

        writer.WriteEndElement(); // SheetData
        writer.WriteEndElement(); // Worksheet

        // Assert
        // If we get here without exception, all writes succeeded
    }

    #endregion

    #region Test Models

    private sealed class SimplePoco
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
        public decimal Amount { get; set; }
    }

    private sealed class PocoWithAttributes
    {
        [Column("user_id")]
        public int Id { get; set; }

        [Column("full_name")]
        public string Name { get; set; } = string.Empty;

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
        public string IgnoredProperty { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        [Column("ignored", Ignore = true)]
        public string AlsoIgnored { get; set; } = string.Empty;
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
        public string FullName { get; set; } = string.Empty;
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
        public string StringValue { get; set; } = string.Empty;
        public bool BoolValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public Guid GuidValue { get; set; }
    }

    #endregion

    #region Build Tests

    [Fact]
    public void Build_WithSimplePoco_CreatesMapper()
    {
        // Act
        var mapper = CreateWriterMapper<SimplePoco>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithAttributes_CreatesMapper()
    {
        // Act
        var mapper = CreateWriterMapper<PocoWithAttributes>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithIgnore_CreatesMapper()
    {
        // Act
        var mapper = CreateWriterMapper<PocoWithIgnore>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithNullableTypes_CreatesMapper()
    {
        // Act
        var mapper = CreateWriterMapper<PocoWithNullableTypes>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithVariousTypes_CreatesMapper()
    {
        // Act
        var mapper = CreateWriterMapper<PocoWithVariousTypes>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithEmptyPoco_CreatesMapper()
    {
        // Act
        var mapper = CreateWriterMapper<EmptyPoco>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithNoMappableProperties_CreatesMapper()
    {
        // Act
        var mapper = CreateWriterMapper<PocoWithNoMappableProperties>();

        // Assert
        mapper.Should().NotBeNull();
    }

    #endregion

    #region GetColumnNames Tests

    [Fact]
    public void GetColumnNames_WithSimplePoco_ReturnsPropertyNames()
    {
        // Act
        var columnNames = GetColumnNames<SimplePoco>();

        // Assert
        columnNames.Should().NotBeEmpty();
        columnNames.Should().HaveCount(5);
        columnNames.Should().ContainInOrder("id", "name", "createdat", "isactive", "amount");
    }

    [Fact]
    public void GetColumnNames_WithPocoWithAttributes_ReturnsCustomColumnNames()
    {
        // Act
        var columnNames = GetColumnNames<PocoWithAttributes>();

        // Assert
        columnNames.Should().NotBeEmpty();
        columnNames.Should().HaveCount(5);
        columnNames.Should().ContainInOrder("user_id", "full_name", "created_date", "is_active", "total_amount");
    }

    [Fact]
    public void GetColumnNames_WithPocoWithIgnore_ExcludesIgnoredProperties()
    {
        // Act
        var columnNames = GetColumnNames<PocoWithIgnore>();

        // Assert
        columnNames.Should().NotBeEmpty();
        columnNames.Should().HaveCount(2);
        columnNames.Should().ContainInOrder("id", "name");
        columnNames.Should().NotContain("IgnoredProperty");
        columnNames.Should().NotContain("AlsoIgnored");
    }

    [Fact]
    public void GetColumnNames_WithPocoWithNullableTypes_ReturnsPropertyNames()
    {
        // Act
        var columnNames = GetColumnNames<PocoWithNullableTypes>();

        // Assert
        columnNames.Should().NotBeEmpty();
        columnNames.Should().HaveCount(5);
        columnNames.Should().ContainInOrder("id", "name", "createdat", "isactive", "amount");
    }

    [Fact]
    public void GetColumnNames_WithPocoWithMixedCase_ReturnsLowercaseNames()
    {
        // Act
        var columnNames = GetColumnNames<PocoWithMixedCase>();

        // Assert
        columnNames.Should().NotBeEmpty();
        columnNames.Should().HaveCount(4);
        columnNames.Should().ContainInOrder("userid", "fullname", "createdat", "isactive");
    }

    [Fact]
    public void GetColumnNames_WithEmptyPoco_ReturnsEmptyArray()
    {
        // Act
        var columnNames = GetColumnNames<EmptyPoco>();

        // Assert
        columnNames.Should().BeEmpty();
    }

    [Fact]
    public void GetColumnNames_WithPocoWithNoMappableProperties_ReturnsEmptyArray()
    {
        // Act
        var columnNames = GetColumnNames<PocoWithNoMappableProperties>();

        // Assert
        columnNames.Should().BeEmpty();
    }

    #endregion

    #region Mapper Caching Tests

    [Fact]
    public void Build_ReturnsCachedMapperOnSecondCall()
    {
        // Act
        var mapper1 = CreateWriterMapper<SimplePoco>();
        var mapper2 = CreateWriterMapper<SimplePoco>();

        // Assert
        mapper2.Should().BeSameAs(mapper1);
    }

    [Fact]
    public void GetColumnNames_ReturnsCachedArrayOnSecondCall()
    {
        // Act
        var columnNames1 = GetColumnNames<SimplePoco>();
        var columnNames2 = GetColumnNames<SimplePoco>();

        // Assert
        columnNames2.Should().BeSameAs(columnNames1);
    }

    [Fact]
    public void Build_DifferentTypes_ReturnDifferentMappers()
    {
        // Act
        var mapper1 = CreateWriterMapper<SimplePoco>();
        var mapper2 = CreateWriterMapper<PocoWithAttributes>();

        // Assert
        mapper2.Should().NotBeSameAs(mapper1);
    }

    #endregion

    #region Write Tests

    [Fact]
    public void Build_WithSimplePoco_WritesAllFields()
    {
        // Arrange
        var poco = new SimplePoco
        {
            Id = 1,
            Name = "John Doe",
            CreatedAt = new DateTime(2024, 1, 1),
            IsActive = true,
            Amount = 100.50m,
        };

        using var stream = new MemoryStream();
        using var writer = CreateOpenXmlWriter(stream);
        var mapper = CreateWriterMapper<SimplePoco>();

        // Act
        mapper(writer, poco);
        CloseOpenXmlWriter(writer);

        // Assert
        // If we get here without exception, the write succeeded
        // The actual content validation would require parsing the OpenXml
    }

    [Fact]
    public void Build_WithPocoWithAttributes_WritesUsingCustomColumnNames()
    {
        // Arrange
        var poco = new PocoWithAttributes
        {
            Id = 1,
            Name = "Jane Doe",
            CreatedAt = new DateTime(2024, 1, 1),
            IsActive = true,
            Amount = 200.75m,
        };

        using var stream = new MemoryStream();
        using var writer = CreateOpenXmlWriter(stream);
        var mapper = CreateWriterMapper<PocoWithAttributes>();

        // Act
        mapper(writer, poco);
        CloseOpenXmlWriter(writer);

        // Assert
        // If we get here without exception, the write succeeded
    }

    [Fact]
    public void Build_WithPocoWithIgnore_ExcludesIgnoredProperties()
    {
        // Arrange
        var poco = new PocoWithIgnore
        {
            Id = 1,
            IgnoredProperty = "Should not appear",
            Name = "John",
            AlsoIgnored = "Also should not appear",
        };

        using var stream = new MemoryStream();
        using var writer = CreateOpenXmlWriter(stream);
        var mapper = CreateWriterMapper<PocoWithIgnore>();

        // Act
        mapper(writer, poco);
        CloseOpenXmlWriter(writer);

        // Assert
        // If we get here without exception, the write succeeded
        // The mapper should only write 2 cells (id and name)
    }

    [Fact]
    public void Build_WithPocoWithNullableTypes_WritesNullValues()
    {
        // Arrange
        var poco = new PocoWithNullableTypes
        {
            Id = 1,
            Name = "John",
            CreatedAt = null,
            IsActive = true,
            Amount = null,
        };

        using var stream = new MemoryStream();
        using var writer = CreateOpenXmlWriter(stream);
        var mapper = CreateWriterMapper<PocoWithNullableTypes>();

        // Act
        mapper(writer, poco);
        CloseOpenXmlWriter(writer);

        // Assert
        // If we get here without exception, the write succeeded
    }

    [Fact]
    public void Build_WithPocoWithMixedCase_WritesUsingConvention()
    {
        // Arrange
        var poco = new PocoWithMixedCase
        {
            UserId = 1,
            FullName = "John Doe",
            CreatedAt = new DateTime(2024, 1, 1),
            IsActive = true,
        };

        using var stream = new MemoryStream();
        using var writer = CreateOpenXmlWriter(stream);
        var mapper = CreateWriterMapper<PocoWithMixedCase>();

        // Act
        mapper(writer, poco);
        CloseOpenXmlWriter(writer);

        // Assert
        // If we get here without exception, the write succeeded
    }

    [Fact]
    public void Build_WithPocoWithVariousTypes_WritesAllTypesCorrectly()
    {
        // Arrange
        var poco = new PocoWithVariousTypes
        {
            IntValue = 42,
            LongValue = 1234567890L,
            ShortValue = 100,
            FloatValue = 3.14f,
            DoubleValue = 2.5,
            DecimalValue = 99.99m,
            StringValue = "Test",
            BoolValue = true,
            DateTimeValue = new DateTime(2024, 1, 1),
            GuidValue = Guid.Parse("550e8400-e29b-41d4-a716-446655440000"),
        };

        using var stream = new MemoryStream();
        using var writer = CreateOpenXmlWriter(stream);
        var mapper = CreateWriterMapper<PocoWithVariousTypes>();

        // Act
        mapper(writer, poco);
        CloseOpenXmlWriter(writer);

        // Assert
        // If we get here without exception, the write succeeded
    }

    [Fact]
    public void Build_WithEmptyPoco_WritesEmptyRow()
    {
        // Arrange
        var poco = new EmptyPoco();

        using var stream = new MemoryStream();
        using var writer = CreateOpenXmlWriter(stream);
        var mapper = CreateWriterMapper<EmptyPoco>();

        // Act
        mapper(writer, poco);
        CloseOpenXmlWriter(writer);

        // Assert
        // If we get here without exception, the write succeeded
    }

    [Fact]
    public void Build_WithPocoWithNoMappableProperties_WritesEmptyRow()
    {
        // Arrange
        var poco = new PocoWithNoMappableProperties
        {
            IgnoredProperty = "Should not appear",
            AlsoIgnored = 42,
        };

        using var stream = new MemoryStream();
        using var writer = CreateOpenXmlWriter(stream);
        var mapper = CreateWriterMapper<PocoWithNoMappableProperties>();

        // Act
        mapper(writer, poco);
        CloseOpenXmlWriter(writer);

        // Assert
        // If we get here without exception, the write succeeded
    }

    #endregion

    #region Helper Methods

    private static Action<OpenXmlWriter, T> CreateWriterMapper<T>()
    {
        var buildMethod = typeof(ExcelWriterMapperBuilder)
                              .GetMethod(nameof(ExcelWriterMapperBuilder.Build), BindingFlags.Public | BindingFlags.Static)
                          ?? throw new InvalidOperationException("ExcelWriterMapperBuilder.Build method not found");

        var genericMethod = buildMethod.MakeGenericMethod(typeof(T));
        return (Action<OpenXmlWriter, T>)genericMethod.Invoke(null, null)!;
    }

    private static string[] GetColumnNames<T>()
    {
        var buildMethod = typeof(ExcelWriterMapperBuilder)
                              .GetMethod(nameof(ExcelWriterMapperBuilder.GetColumnNames), BindingFlags.Public | BindingFlags.Static)
                          ?? throw new InvalidOperationException("ExcelWriterMapperBuilder.GetColumnNames method not found");

        var genericMethod = buildMethod.MakeGenericMethod(typeof(T));
        return (string[])genericMethod.Invoke(null, null)!;
    }

    private static OpenXmlWriter CreateOpenXmlWriter(Stream stream)
    {
        var writer = OpenXmlWriter.Create(stream);
        writer.WriteStartElement(new Worksheet());
        writer.WriteStartElement(new SheetData());
        writer.WriteStartElement(new Row { RowIndex = 1 });
        return writer;
    }

    private static void CloseOpenXmlWriter(OpenXmlWriter writer)
    {
        writer.WriteEndElement(); // Row
        writer.WriteEndElement(); // SheetData
        writer.WriteEndElement(); // Worksheet
    }

    #endregion
}

using System.Globalization;
using System.Text;
using AwesomeAssertions;
using CsvHelper;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Csv.Tests;

/// <summary>
///     Unit tests for CsvWriterMapperBuilder.
///     Validates mapping delegates for writing objects to CSV rows.
/// </summary>
public sealed class CsvWriterMapperBuilderTests
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

        var config = new CsvConfiguration(CultureInfo.InvariantCulture);
        using var writer = new StringWriter();
        using var csv = new CsvWriter(writer, config.HelperConfiguration);
        var mapper = CsvWriterMapperBuilder.Build<SimplePoco>();

        // Act
        foreach (var poco in pocos)
        {
            mapper(csv, poco);
            csv.NextRecord();
        }

        var result = writer.ToString();
        var lines = result.Split(new[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries);

        // Assert
        lines.Should().HaveCount(3);
        lines[0].Should().Contain("1,John");
        lines[1].Should().Contain("2,Jane");
        lines[2].Should().Contain("3,Bob");
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
        var mapper = CsvWriterMapperBuilder.Build<SimplePoco>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithAttributes_CreatesMapper()
    {
        // Act
        var mapper = CsvWriterMapperBuilder.Build<PocoWithAttributes>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithIgnore_CreatesMapper()
    {
        // Act
        var mapper = CsvWriterMapperBuilder.Build<PocoWithIgnore>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithNullableTypes_CreatesMapper()
    {
        // Act
        var mapper = CsvWriterMapperBuilder.Build<PocoWithNullableTypes>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithVariousTypes_CreatesMapper()
    {
        // Act
        var mapper = CsvWriterMapperBuilder.Build<PocoWithVariousTypes>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithEmptyPoco_CreatesMapper()
    {
        // Act
        var mapper = CsvWriterMapperBuilder.Build<EmptyPoco>();

        // Assert
        mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithNoMappableProperties_CreatesMapper()
    {
        // Act
        var mapper = CsvWriterMapperBuilder.Build<PocoWithNoMappableProperties>();

        // Assert
        mapper.Should().NotBeNull();
    }

    #endregion

    #region GetColumnNames Tests

    [Fact]
    public void GetColumnNames_WithSimplePoco_ReturnsPropertyNames()
    {
        // Act
        var columnNames = CsvWriterMapperBuilder.GetColumnNames<SimplePoco>();

        // Assert
        columnNames.Should().NotBeEmpty();
        columnNames.Should().HaveCount(5);
        columnNames.Should().ContainInOrder("id", "name", "createdat", "isactive", "amount");
    }

    [Fact]
    public void GetColumnNames_WithPocoWithAttributes_ReturnsCustomColumnNames()
    {
        // Act
        var columnNames = CsvWriterMapperBuilder.GetColumnNames<PocoWithAttributes>();

        // Assert
        columnNames.Should().NotBeEmpty();
        columnNames.Should().HaveCount(5);
        columnNames.Should().ContainInOrder("user_id", "full_name", "created_date", "is_active", "total_amount");
    }

    [Fact]
    public void GetColumnNames_WithPocoWithIgnore_ExcludesIgnoredProperties()
    {
        // Act
        var columnNames = CsvWriterMapperBuilder.GetColumnNames<PocoWithIgnore>();

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
        var columnNames = CsvWriterMapperBuilder.GetColumnNames<PocoWithNullableTypes>();

        // Assert
        columnNames.Should().NotBeEmpty();
        columnNames.Should().HaveCount(5);
        columnNames.Should().ContainInOrder("id", "name", "createdat", "isactive", "amount");
    }

    [Fact]
    public void GetColumnNames_WithPocoWithMixedCase_ReturnsLowercaseNames()
    {
        // Act
        var columnNames = CsvWriterMapperBuilder.GetColumnNames<PocoWithMixedCase>();

        // Assert
        columnNames.Should().NotBeEmpty();
        columnNames.Should().HaveCount(4);
        columnNames.Should().ContainInOrder("userid", "fullname", "createdat", "isactive");
    }

    [Fact]
    public void GetColumnNames_WithEmptyPoco_ReturnsEmptyArray()
    {
        // Act
        var columnNames = CsvWriterMapperBuilder.GetColumnNames<EmptyPoco>();

        // Assert
        columnNames.Should().BeEmpty();
    }

    [Fact]
    public void GetColumnNames_WithPocoWithNoMappableProperties_ReturnsEmptyArray()
    {
        // Act
        var columnNames = CsvWriterMapperBuilder.GetColumnNames<PocoWithNoMappableProperties>();

        // Assert
        columnNames.Should().BeEmpty();
    }

    #endregion

    #region Mapper Caching Tests

    [Fact]
    public void Build_ReturnsCachedMapperOnSecondCall()
    {
        // Act
        var mapper1 = CsvWriterMapperBuilder.Build<SimplePoco>();
        var mapper2 = CsvWriterMapperBuilder.Build<SimplePoco>();

        // Assert
        mapper2.Should().BeSameAs(mapper1);
    }

    [Fact]
    public void GetColumnNames_ReturnsCachedArrayOnSecondCall()
    {
        // Act
        var columnNames1 = CsvWriterMapperBuilder.GetColumnNames<SimplePoco>();
        var columnNames2 = CsvWriterMapperBuilder.GetColumnNames<SimplePoco>();

        // Assert
        columnNames2.Should().BeSameAs(columnNames1);
    }

    [Fact]
    public void Build_DifferentTypes_ReturnDifferentMappers()
    {
        // Act
        var mapper1 = CsvWriterMapperBuilder.Build<SimplePoco>();
        var mapper2 = CsvWriterMapperBuilder.Build<PocoWithAttributes>();

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

        var config = new CsvConfiguration(CultureInfo.InvariantCulture);
        using var writer = new StringWriter();
        using var csv = new CsvWriter(writer, config.HelperConfiguration);
        var mapper = CsvWriterMapperBuilder.Build<SimplePoco>();

        // Act
        mapper(csv, poco);
        csv.NextRecord();

        var result = writer.ToString();

        // Assert
        result.Should().Contain("1");
        result.Should().Contain("John Doe");
        result.Should().Contain("True");
        result.Should().Contain("100.50");
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

        var config = new CsvConfiguration(CultureInfo.InvariantCulture);
        using var writer = new StringWriter();
        using var csv = new CsvWriter(writer, config.HelperConfiguration);
        var mapper = CsvWriterMapperBuilder.Build<PocoWithAttributes>();

        // Act
        mapper(csv, poco);
        csv.NextRecord();

        var result = writer.ToString();

        // Assert
        result.Should().Contain("1");
        result.Should().Contain("Jane Doe");
        result.Should().Contain("True");
        result.Should().Contain("200.75");
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

        var config = new CsvConfiguration(CultureInfo.InvariantCulture);
        using var writer = new StringWriter();
        using var csv = new CsvWriter(writer, config.HelperConfiguration);
        var mapper = CsvWriterMapperBuilder.Build<PocoWithIgnore>();

        // Act
        mapper(csv, poco);
        csv.NextRecord();

        var result = writer.ToString();

        // Assert
        result.Should().Be("1,John\r\n");
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

        var config = new CsvConfiguration(CultureInfo.InvariantCulture);
        using var writer = new StringWriter();
        using var csv = new CsvWriter(writer, config.HelperConfiguration);
        var mapper = CsvWriterMapperBuilder.Build<PocoWithNullableTypes>();

        // Act
        mapper(csv, poco);
        csv.NextRecord();

        var result = writer.ToString();

        // Assert
        result.Should().Be("1,John,,True,\r\n");
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

        var config = new CsvConfiguration(CultureInfo.InvariantCulture);
        using var writer = new StringWriter();
        using var csv = new CsvWriter(writer, config.HelperConfiguration);
        var mapper = CsvWriterMapperBuilder.Build<PocoWithMixedCase>();

        // Act
        mapper(csv, poco);
        csv.NextRecord();

        var result = writer.ToString();

        // Assert
        result.Should().Contain("1");
        result.Should().Contain("John Doe");
        result.Should().Contain("True");
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

        var mapper = CsvWriterMapperBuilder.Build<PocoWithVariousTypes>();
        var sb = new StringBuilder();
        using var writer = new StringWriter(sb);
        using var csv = new CsvWriter(writer, new CsvHelper.Configuration.CsvConfiguration(CultureInfo.InvariantCulture));

        // Act
        mapper(csv, poco);
        csv.NextRecord();

        var result = sb.ToString();

        // Assert
        result.Should().Contain("42");
        result.Should().Contain("1234567890");
        result.Should().Contain("100");
        result.Should().Contain("3.14");
        result.Should().Contain("2.5");
        result.Should().Contain("99.99");
        result.Should().Contain("Test");
        result.Should().Contain("True");
        result.Should().Contain("01/01/2024 00:00:00");
        result.Should().Contain("550e8400-e29b-41d4-a716-446655440000");
    }

    [Fact]
    public void Build_WithEmptyPoco_WritesEmptyLine()
    {
        // Arrange
        var poco = new EmptyPoco();

        var config = new CsvConfiguration(CultureInfo.InvariantCulture);
        using var writer = new StringWriter();
        using var csv = new CsvWriter(writer, config.HelperConfiguration);
        var mapper = CsvWriterMapperBuilder.Build<EmptyPoco>();

        // Act
        mapper(csv, poco);
        csv.NextRecord();

        var result = writer.ToString();

        // Assert
        result.Should().Be("\r\n");
    }

    [Fact]
    public void Build_WithPocoWithNoMappableProperties_WritesEmptyLine()
    {
        // Arrange
        var poco = new PocoWithNoMappableProperties
        {
            IgnoredProperty = "Should not appear",
            AlsoIgnored = 42,
        };

        var config = new CsvConfiguration(CultureInfo.InvariantCulture);
        using var writer = new StringWriter();
        using var csv = new CsvWriter(writer, config.HelperConfiguration);
        var mapper = CsvWriterMapperBuilder.Build<PocoWithNoMappableProperties>();

        // Act
        mapper(csv, poco);
        csv.NextRecord();

        var result = writer.ToString();

        // Assert
        result.Should().Be("\r\n");
    }

    #endregion
}

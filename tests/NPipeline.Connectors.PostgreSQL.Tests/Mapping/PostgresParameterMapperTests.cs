using AwesomeAssertions;
using NPipeline.Connectors.PostgreSQL.Mapping;

namespace NPipeline.Connectors.PostgreSQL.Tests.Mapping;

/// <summary>
///     Tests for PostgresParameterMapper.
///     Validates parameter mapping, column name extraction, and COPY mapper building.
/// </summary>
public sealed class PostgresParameterMapperTests
{
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
        [PostgresColumn("user_id")]
        public int Id { get; set; }

        [PostgresColumn("full_name")]
        public string Name { get; set; } = string.Empty;

        [PostgresColumn("created_date")]
        public DateTime CreatedAt { get; set; }

        [PostgresColumn("is_active")]
        public bool IsActive { get; set; }

        [PostgresColumn("total_amount")]
        public decimal Amount { get; set; }
    }

    private sealed class PocoWithIgnore
    {
        public int Id { get; set; }

        [PostgresIgnore]
        public string IgnoredProperty { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        [PostgresColumn("ignored", Ignore = true)]
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

    private sealed class PocoWithSnakeCase
    {
        public int UserId { get; set; }
        public string FullName { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
    }

    private sealed class PocoWithTableAttribute
    {
        [PostgresColumn("custom_id")]
        public int Id { get; set; }

        [PostgresColumn("custom_name")]
        public string Name { get; set; } = string.Empty;
    }

    private sealed class EmptyPoco
    {
    }

    private sealed class PocoWithNoMappableProperties
    {
        [PostgresIgnore]
        public string IgnoredProperty { get; set; } = string.Empty;

        [PostgresColumn("ignored", Ignore = true)]
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
        public byte[] ByteArrayValue { get; set; } = [];
    }

    #endregion

    #region GetColumnNames Tests

    [Fact]
    public void GetColumnNames_WithSimplePoco_ReturnsPropertyNames()
    {
        // Act
        var columnNames = PostgresParameterMapper.GetColumnNames<SimplePoco>();

        // Assert
        _ = columnNames.Should().NotBeEmpty();
        _ = columnNames.Should().HaveCount(5);
        _ = columnNames.Should().ContainInOrder("Id", "Name", "CreatedAt", "IsActive", "Amount");
    }

    [Fact]
    public void GetColumnNames_WithPocoWithAttributes_ReturnsCustomColumnNames()
    {
        // Act
        var columnNames = PostgresParameterMapper.GetColumnNames<PocoWithAttributes>();

        // Assert
        _ = columnNames.Should().NotBeEmpty();
        _ = columnNames.Should().HaveCount(5);
        _ = columnNames.Should().ContainInOrder("user_id", "full_name", "created_date", "is_active", "total_amount");
    }

    [Fact]
    public void GetColumnNames_WithPocoWithIgnore_ExcludesIgnoredProperties()
    {
        // Act
        var columnNames = PostgresParameterMapper.GetColumnNames<PocoWithIgnore>();

        // Assert
        _ = columnNames.Should().NotBeEmpty();
        _ = columnNames.Should().HaveCount(2);
        _ = columnNames.Should().ContainInOrder("Id", "Name");
        _ = columnNames.Should().NotContain("IgnoredProperty");
        _ = columnNames.Should().NotContain("AlsoIgnored");
    }

    [Fact]
    public void GetColumnNames_WithPocoWithNullableTypes_ReturnsPropertyNames()
    {
        // Act
        var columnNames = PostgresParameterMapper.GetColumnNames<PocoWithNullableTypes>();

        // Assert
        _ = columnNames.Should().NotBeEmpty();
        _ = columnNames.Should().HaveCount(5);
        _ = columnNames.Should().ContainInOrder("Id", "Name", "CreatedAt", "IsActive", "Amount");
    }

    [Fact]
    public void GetColumnNames_WithPocoWithSnakeCase_ReturnsPropertyNames()
    {
        // Act
        var columnNames = PostgresParameterMapper.GetColumnNames<PocoWithSnakeCase>();

        // Assert
        _ = columnNames.Should().NotBeEmpty();
        _ = columnNames.Should().HaveCount(4);
        _ = columnNames.Should().ContainInOrder("UserId", "FullName", "CreatedAt", "IsActive");
    }

    [Fact]
    public void GetColumnNames_WithEmptyPoco_ReturnsEmptyArray()
    {
        // Act
        var columnNames = PostgresParameterMapper.GetColumnNames<EmptyPoco>();

        // Assert
        _ = columnNames.Should().BeEmpty();
    }

    [Fact]
    public void GetColumnNames_WithPocoWithNoMappableProperties_ReturnsEmptyArray()
    {
        // Act
        var columnNames = PostgresParameterMapper.GetColumnNames<PocoWithNoMappableProperties>();

        // Assert
        _ = columnNames.Should().BeEmpty();
    }

    #endregion

    #region Build Tests

    [Fact]
    public void Build_WithSimplePoco_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.Build<SimplePoco>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithAttributes_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.Build<PocoWithAttributes>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithIgnore_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.Build<PocoWithIgnore>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithNullableTypes_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.Build<PocoWithNullableTypes>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithVariousTypes_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.Build<PocoWithVariousTypes>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithEmptyPoco_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.Build<EmptyPoco>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithPocoWithNoMappableProperties_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.Build<PocoWithNoMappableProperties>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    #endregion

    #region BuildCopyMapper Tests

    [Fact]
    public void BuildCopyMapper_WithSimplePoco_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.BuildCopyMapper<SimplePoco>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    [Fact]
    public void BuildCopyMapper_WithPocoWithAttributes_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.BuildCopyMapper<PocoWithAttributes>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    [Fact]
    public void BuildCopyMapper_WithPocoWithIgnore_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.BuildCopyMapper<PocoWithIgnore>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    [Fact]
    public void BuildCopyMapper_WithPocoWithNullableTypes_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.BuildCopyMapper<PocoWithNullableTypes>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    [Fact]
    public void BuildCopyMapper_WithPocoWithVariousTypes_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.BuildCopyMapper<PocoWithVariousTypes>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    [Fact]
    public void BuildCopyMapper_WithEmptyPoco_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.BuildCopyMapper<EmptyPoco>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    [Fact]
    public void BuildCopyMapper_WithPocoWithNoMappableProperties_CreatesMapper()
    {
        // Act
        var mapper = PostgresParameterMapper.BuildCopyMapper<PocoWithNoMappableProperties>();

        // Assert
        _ = mapper.Should().NotBeNull();
    }

    #endregion
}

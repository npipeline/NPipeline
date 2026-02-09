using AwesomeAssertions;
using NpgsqlTypes;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.PostgreSQL.Mapping;

namespace NPipeline.Connectors.PostgreSQL.Tests.Mapping;

public class PostgresMappingAttributeTests
{
    #region Common Attributes Tests

    [Fact]
    public void CommonColumnAttribute_WithOnlyName_ShouldSetProperties()
    {
        // Act
        var attribute = new ColumnAttribute("custom_name");

        // Assert
        _ = attribute.Name.Should().Be("custom_name");
        _ = attribute.Ignore.Should().BeFalse();
    }

    [Fact]
    public void CommonColumnAttribute_WithNameAndIgnore_ShouldSetProperties()
    {
        // Act
        var attribute = new ColumnAttribute("custom_name")
        {
            Ignore = true,
        };

        // Assert
        _ = attribute.Name.Should().Be("custom_name");
        _ = attribute.Ignore.Should().BeTrue();
    }

    [Fact]
    public void CommonIgnoreColumnAttribute_ShouldCreateInstance()
    {
        // Act
        var attribute = new IgnoreColumnAttribute();

        // Assert
        _ = attribute.Should().NotBeNull();
    }

    [Fact]
    public void PostgresColumnAttribute_InheritsFromCommonColumnAttribute()
    {
        // Act
        var attribute = new PostgresColumnAttribute("test_name");

        // Assert - PostgresColumnAttribute inherits Name and Ignore from ColumnAttribute
        _ = attribute.Name.Should().Be("test_name");
        _ = attribute.Ignore.Should().BeFalse();
    }

    #endregion

    #region Existing Tests

    [Fact]
    public void PostgresColumnAttribute_WithOnlyName_ShouldSetProperties()
    {
        // Act
        var attribute = new PostgresColumnAttribute("custom_name");

        // Assert
        _ = attribute.Name.Should().Be("custom_name");
        _ = attribute.DbType.Should().BeNull();
        _ = attribute.Size.Should().BeNull();
        _ = attribute.PrimaryKey.Should().BeFalse();
        _ = attribute.Ignore.Should().BeFalse();
    }

    [Fact]
    public void PostgresColumnAttribute_WithAllProperties_ShouldSetProperties()
    {
        // Act
        var attribute = new PostgresColumnAttribute("custom_name")
        {
            DbType = NpgsqlDbType.Varchar,
            Size = 100,
            PrimaryKey = true,
            Ignore = true,
        };

        // Assert
        _ = attribute.Name.Should().Be("custom_name");
        _ = attribute.DbType.Should().Be(NpgsqlDbType.Varchar);
        _ = attribute.Size.Should().Be(100);
        _ = attribute.PrimaryKey.Should().BeTrue();
        _ = attribute.Ignore.Should().BeTrue();
    }

    [Fact]
    public void PostgresTableAttribute_WithOnlySchema_ShouldSetProperties()
    {
        // Act
        var attribute = new PostgresTableAttribute("test_table")
        {
            Schema = "custom_schema",
        };

        // Assert
        _ = attribute.Name.Should().Be("test_table");
        _ = attribute.Schema.Should().Be("custom_schema");
    }

    [Fact]
    public void PostgresTableAttribute_WithEmptyName_ShouldThrowArgumentException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new PostgresTableAttribute(string.Empty));
    }

    [Fact]
    public void PostgresTableAttribute_WithWhitespaceName_ShouldThrowArgumentException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new PostgresTableAttribute("   "));
    }

    #endregion

    #region Test Models for Common Attributes

    private sealed class PocoWithCommonAttributes
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

    private sealed class PocoWithCommonIgnore
    {
        public int Id { get; set; }

        [IgnoreColumn]
        public string IgnoredProperty { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        [Column("ignored", Ignore = true)]
        public string AlsoIgnored { get; set; } = string.Empty;
    }

    private sealed class PocoWithMixedAttributes
    {
        [Column("user_id")]
        public int Id { get; set; }

        [PostgresColumn("full_name")]
        public string Name { get; set; } = string.Empty;

        [Column("created_date")]
        public DateTime CreatedAt { get; set; }

        [PostgresColumn("is_active")]
        public bool IsActive { get; set; }

        [Column("total_amount")]
        public decimal Amount { get; set; }
    }

    #endregion
}

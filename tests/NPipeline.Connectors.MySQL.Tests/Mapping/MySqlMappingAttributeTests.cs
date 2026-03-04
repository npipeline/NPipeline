using AwesomeAssertions;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.MySql.Mapping;

namespace NPipeline.Connectors.MySql.Tests.Mapping;

public class MySqlMappingAttributeTests
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
    public void MySqlColumnAttribute_InheritsFromCommonColumnAttribute()
    {
        // Act
        var attribute = new MySqlColumnAttribute("test_name");

        // Assert
        _ = attribute.Name.Should().Be("test_name");
        _ = attribute.Ignore.Should().BeFalse();
    }

    #endregion

    #region MySqlTableAttribute Tests

    [Fact]
    public void MySqlTableAttribute_WithName_ShouldSetName()
    {
        // Act
        var attribute = new MySqlTableAttribute("test_table");

        // Assert
        _ = attribute.Name.Should().Be("test_table");
    }

    [Fact]
    public void MySqlTableAttribute_WithEmptyName_ShouldThrowArgumentException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new MySqlTableAttribute(string.Empty));
    }

    [Fact]
    public void MySqlTableAttribute_WithWhitespaceName_ShouldThrowArgumentException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new MySqlTableAttribute("   "));
    }

    #endregion

    #region MySqlColumnAttribute Tests

    [Fact]
    public void MySqlColumnAttribute_WithOnlyName_ShouldSetProperties()
    {
        // Act
        var attribute = new MySqlColumnAttribute("custom_name");

        // Assert
        _ = attribute.Name.Should().Be("custom_name");
        _ = attribute.AutoIncrement.Should().BeFalse();
        _ = attribute.Ignore.Should().BeFalse();
    }

    [Fact]
    public void MySqlColumnAttribute_WithAutoIncrement_ShouldSetAutoIncrement()
    {
        // Act
        var attribute = new MySqlColumnAttribute("id")
        {
            AutoIncrement = true,
        };

        // Assert
        _ = attribute.Name.Should().Be("id");
        _ = attribute.AutoIncrement.Should().BeTrue();
    }

    [Fact]
    public void MySqlColumnAttribute_WithIgnore_ShouldSetIgnore()
    {
        // Act
        var attribute = new MySqlColumnAttribute("ignored_column")
        {
            Ignore = true,
        };

        // Assert
        _ = attribute.Ignore.Should().BeTrue();
    }

    [Fact]
    public void MySqlColumnAttribute_DefaultConstructor_ShouldHaveEmptyName()
    {
        // Act
        var attribute = new MySqlColumnAttribute();

        // Assert
        _ = attribute.Name.Should().BeEmpty();
        _ = attribute.AutoIncrement.Should().BeFalse();
    }

    [Fact]
    public void MySqlColumnAttribute_WithAllProperties_ShouldSetProperties()
    {
        // Act
        var attribute = new MySqlColumnAttribute("user_id")
        {
            AutoIncrement = true,
            Ignore = false,
        };

        // Assert
        _ = attribute.Name.Should().Be("user_id");
        _ = attribute.AutoIncrement.Should().BeTrue();
        _ = attribute.Ignore.Should().BeFalse();
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

        [MySqlColumn("full_name")]
        public string Name { get; set; } = string.Empty;

        [Column("created_date")]
        public DateTime CreatedAt { get; set; }

        [MySqlColumn("is_active")]
        public bool IsActive { get; set; }
    }

    #endregion

    #region Test Models for MySQL Specific Attributes

    private sealed class PocoWithMySqlAttributes
    {
        [MySqlColumn("user_id")]
        public int Id { get; set; }

        [MySqlColumn("full_name")]
        public string Name { get; set; } = string.Empty;

        [MySqlColumn("created_date")]
        public DateTime CreatedAt { get; set; }
    }

    [MySqlTable("custom_table_name")]
    private sealed class PocoWithCustomTableName
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [MySqlTable("products")]
    private sealed class PocoWithAutoIncrement
    {
        [MySqlColumn("id", AutoIncrement = true)]
        public int Id { get; set; }

        [MySqlColumn("name")]
        public string Name { get; set; } = string.Empty;
    }

    #endregion
}

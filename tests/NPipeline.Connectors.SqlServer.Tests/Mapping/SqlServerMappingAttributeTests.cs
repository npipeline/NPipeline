using System.Data;
using AwesomeAssertions;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.SqlServer.Mapping;

namespace NPipeline.Connectors.SqlServer.Tests.Mapping;

public class SqlServerMappingAttributeTests
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
    public void SqlServerColumnAttribute_InheritsFromCommonColumnAttribute()
    {
        // Act
        var attribute = new SqlServerColumnAttribute("test_name");

        // Assert - SqlServerColumnAttribute inherits Name and Ignore from ColumnAttribute
        _ = attribute.Name.Should().Be("test_name");
        _ = attribute.Ignore.Should().BeFalse();
    }

    #endregion

    #region SqlServerTableAttribute Tests

    [Fact]
    public void SqlServerTableAttribute_WithOnlyName_ShouldSetProperties()
    {
        // Act
        var attribute = new SqlServerTableAttribute("test_table");

        // Assert
        _ = attribute.Name.Should().Be("test_table");
        _ = attribute.Schema.Should().Be("dbo");
    }

    [Fact]
    public void SqlServerTableAttribute_WithNameAndSchema_ShouldSetProperties()
    {
        // Act
        var attribute = new SqlServerTableAttribute("test_table")
        {
            Schema = "custom_schema",
        };

        // Assert
        _ = attribute.Name.Should().Be("test_table");
        _ = attribute.Schema.Should().Be("custom_schema");
    }

    [Fact]
    public void SqlServerTableAttribute_WithEmptyName_ShouldThrowArgumentException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new SqlServerTableAttribute(string.Empty));
    }

    [Fact]
    public void SqlServerTableAttribute_WithWhitespaceName_ShouldThrowArgumentException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => new SqlServerTableAttribute("   "));
    }

    #endregion

    #region SqlServerColumnAttribute Tests

    [Fact]
    public void SqlServerColumnAttribute_WithOnlyName_ShouldSetProperties()
    {
        // Act
        var attribute = new SqlServerColumnAttribute("custom_name");

        // Assert
        _ = attribute.Name.Should().Be("custom_name");
        _ = attribute.DbType.Should().Be(SqlDbType.BigInt);
        _ = attribute.Size.Should().Be(0);
        _ = attribute.PrimaryKey.Should().BeFalse();
        _ = attribute.Identity.Should().BeFalse();
        _ = attribute.Ignore.Should().BeFalse();
    }

    [Fact]
    public void SqlServerColumnAttribute_WithAllProperties_ShouldSetProperties()
    {
        // Act
        var attribute = new SqlServerColumnAttribute("custom_name")
        {
            DbType = SqlDbType.VarChar,
            Size = 100,
            PrimaryKey = true,
            Identity = true,
            Ignore = true,
        };

        // Assert
        _ = attribute.Name.Should().Be("custom_name");
        _ = attribute.DbType.Should().Be(SqlDbType.VarChar);
        _ = attribute.Size.Should().Be(100);
        _ = attribute.PrimaryKey.Should().BeTrue();
        _ = attribute.Identity.Should().BeTrue();
        _ = attribute.Ignore.Should().BeTrue();
    }

    [Fact]
    public void SqlServerColumnAttribute_WithDbType_ShouldSetDbType()
    {
        // Act
        var attribute = new SqlServerColumnAttribute("test_column")
        {
            DbType = SqlDbType.Int,
        };

        // Assert
        _ = attribute.DbType.Should().Be(SqlDbType.Int);
    }

    [Fact]
    public void SqlServerColumnAttribute_WithSize_ShouldSetSize()
    {
        // Act
        var attribute = new SqlServerColumnAttribute("test_column")
        {
            Size = 255,
        };

        // Assert
        _ = attribute.Size.Should().Be(255);
    }

    [Fact]
    public void SqlServerColumnAttribute_WithPrimaryKey_ShouldSetPrimaryKey()
    {
        // Act
        var attribute = new SqlServerColumnAttribute("test_column")
        {
            PrimaryKey = true,
        };

        // Assert
        _ = attribute.PrimaryKey.Should().BeTrue();
    }

    [Fact]
    public void SqlServerColumnAttribute_WithIdentity_ShouldSetIdentity()
    {
        // Act
        var attribute = new SqlServerColumnAttribute("test_column")
        {
            Identity = true,
        };

        // Assert
        _ = attribute.Identity.Should().BeTrue();
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

        [SqlServerColumn("full_name")]
        public string Name { get; set; } = string.Empty;

        [Column("created_date")]
        public DateTime CreatedAt { get; set; }

        [SqlServerColumn("is_active")]
        public bool IsActive { get; set; }

        [Column("total_amount")]
        public decimal Amount { get; set; }
    }

    #endregion

    #region Test Models for SQL Server Specific Attributes

    private sealed class PocoWithSqlServerAttributes
    {
        [SqlServerColumn("user_id")]
        public int Id { get; set; }

        [SqlServerColumn("full_name")]
        public string Name { get; set; } = string.Empty;

        [SqlServerColumn("created_date")]
        public DateTime CreatedAt { get; set; }

        [SqlServerColumn("is_active")]
        public bool IsActive { get; set; }

        [SqlServerColumn("total_amount")]
        public decimal Amount { get; set; }
    }

    private sealed class PocoWithSqlServerTableAttribute
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [SqlServerTable("custom_table_name", Schema = "custom_schema")]
    private sealed class PocoWithCustomTableAndSchema
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    #endregion
}

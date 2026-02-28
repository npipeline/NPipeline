using System.Data;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Snowflake.Mapping;

namespace NPipeline.Connectors.Snowflake.Tests.Mapping;

public sealed class SnowflakeMappingAttributeTests
{
    [Fact]
    public void SnowflakeColumnAttribute_ShouldSetProperties()
    {
        // Arrange & Act
        var attr = new SnowflakeColumnAttribute("TEST_COLUMN")
        {
            DbType = DbType.String,
            NativeTypeName = "VARCHAR(255)",
            Size = 255,
            PrimaryKey = true,
            Identity = true,
        };

        // Assert
        Assert.Equal("TEST_COLUMN", attr.Name);
        Assert.Equal(DbType.String, attr.DbType);
        Assert.Equal("VARCHAR(255)", attr.NativeTypeName);
        Assert.Equal(255, attr.Size);
        Assert.True(attr.PrimaryKey);
        Assert.True(attr.Identity);
    }

    [Fact]
    public void SnowflakeColumnAttribute_DefaultValues_ShouldBeCorrect()
    {
        // Arrange & Act
        var attr = new SnowflakeColumnAttribute("COL");

        // Assert
        Assert.False(attr.PrimaryKey);
        Assert.False(attr.Identity);
        Assert.Null(attr.NativeTypeName);
        Assert.Equal(0, attr.Size);
    }

    [Fact]
    public void SnowflakeTableAttribute_ShouldSetProperties()
    {
        // Arrange & Act
        var attr = new SnowflakeTableAttribute("CUSTOMERS")
        {
            Schema = "MY_SCHEMA",
            Database = "MY_DB",
        };

        // Assert
        Assert.Equal("CUSTOMERS", attr.Name);
        Assert.Equal("MY_SCHEMA", attr.Schema);
        Assert.Equal("MY_DB", attr.Database);
    }

    [Fact]
    public void SnowflakeTableAttribute_DefaultSchema_ShouldBePUBLIC()
    {
        // Arrange & Act
        var attr = new SnowflakeTableAttribute("TEST");

        // Assert
        Assert.Equal("PUBLIC", attr.Schema);
        Assert.Null(attr.Database);
    }

    [Fact]
    public void IgnoreColumnAttribute_ShouldExcludeProperty()
    {
        // Arrange
        var property = typeof(TestModel).GetProperty(nameof(TestModel.Ignored))!;

        // Act
        var hasIgnore = property.IsDefined(typeof(IgnoreColumnAttribute), true);

        // Assert
        Assert.True(hasIgnore);
    }

    [Fact]
    public void ColumnAttribute_WithName_ShouldSetName()
    {
        // Arrange & Act
        var attr = new ColumnAttribute("CUSTOM_NAME");

        // Assert
        Assert.Equal("CUSTOM_NAME", attr.Name);
    }

    [Fact]
    public void ColumnAttribute_WithIgnore_ShouldSetIgnore()
    {
        // Arrange & Act
        var attr = new ColumnAttribute("COL") { Ignore = true };

        // Assert
        Assert.True(attr.Ignore);
    }

    private sealed class TestModel
    {
        [SnowflakeColumn("ID", PrimaryKey = true, Identity = true)]
        public int Id { get; set; }

        [Column("NAME")]
        public string Name { get; set; } = string.Empty;

        [IgnoreColumn]
        public string Ignored { get; set; } = string.Empty;
    }
}

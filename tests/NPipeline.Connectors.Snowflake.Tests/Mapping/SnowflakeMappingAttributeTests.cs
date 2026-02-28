using System.Data;
using FakeItEasy;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Mapping;
using NPipeline.StorageProviders.Abstractions;

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

    [Fact]
    public void ParameterMapper_GetColumnNames_ShouldUseUpperSnakeCaseConvention()
    {
        // Arrange
        var configuration = new SnowflakeConfiguration();

        // Act
        var columns = SnowflakeParameterMapper.GetColumnNames<ConventionModel>(configuration);

        // Assert
        Assert.Contains("FIRST_NAME", columns);
        Assert.Contains("CREATED_AT", columns);
        Assert.Contains("URL_VALUE", columns);
        Assert.Contains("ORDER_2_TOTAL", columns);
    }

    [Fact]
    public void ParameterMapper_GetColumnNames_ShouldRespectAttributeOverrides()
    {
        // Arrange
        var configuration = new SnowflakeConfiguration();

        // Act
        var columns = SnowflakeParameterMapper.GetColumnNames<AttributeOverrideModel>(configuration);

        // Assert
        Assert.Contains("RAW_VALUE", columns);
        Assert.Contains("EXPLICIT_NAME", columns);
    }

    [Fact]
    public void MapperBuilder_ShouldMapUpperSnakeCaseColumns_ByConvention()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("FIRST_NAME");
        A.CallTo(() => reader.IsDBNull(0)).Returns(false);
        A.CallTo(() => reader.GetFieldValue<string>(0)).Returns("Grace");

        var row = new SnowflakeRow(reader);
        var mapper = SnowflakeMapperBuilder.BuildMapper<ConventionReadModel>(new SnowflakeConfiguration());

        // Act
        var result = mapper(row);

        // Assert
        Assert.Equal("Grace", result.FirstName);
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

    private sealed class ConventionModel
    {
        public string FirstName { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public string UrlValue { get; set; } = string.Empty;
        public decimal Order2Total { get; set; }
    }

    private sealed class AttributeOverrideModel
    {
        [Column("RAW_VALUE")]
        public string RawValue { get; set; } = string.Empty;

        [SnowflakeColumn("EXPLICIT_NAME")]
        public string AnyName { get; set; } = string.Empty;
    }

    private sealed class ConventionReadModel
    {
        public string FirstName { get; set; } = string.Empty;
    }
}

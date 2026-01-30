using AwesomeAssertions;
using NpgsqlTypes;
using NPipeline.Connectors.PostgreSQL.Mapping;

namespace NPipeline.Connectors.PostgreSQL.Tests.Mapping;

public class PostgresMappingAttributeTests
{
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
    public void PostgresIgnoreAttribute_ShouldCreateInstance()
    {
        // Act
        var attribute = new PostgresIgnoreAttribute();

        // Assert
        _ = attribute.Should().NotBeNull();
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
}

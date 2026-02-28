using NPipeline.Connectors.DuckDB.Mapping;

namespace NPipeline.Connectors.DuckDB.Tests;

public sealed class DuckDBSchemaBuilderTests
{
    [Fact]
    public void BuildCreateTable_SimpleClass_GeneratesCorrectDDL()
    {
        var ddl = DuckDBSchemaBuilder.BuildCreateTable<TestRecord>("test_table");

        ddl.Should().Contain("CREATE TABLE IF NOT EXISTS \"test_table\"");
        ddl.Should().Contain("\"Id\" INTEGER");
        ddl.Should().Contain("\"Name\" VARCHAR");
        ddl.Should().Contain("\"Value\" DOUBLE");
    }

    [Fact]
    public void BuildCreateTable_WithPrimaryKey_IncludesConstraint()
    {
        var ddl = DuckDBSchemaBuilder.BuildCreateTable<CustomColumnRecord>("pk_table");

        ddl.Should().Contain("PRIMARY KEY");
        ddl.Should().Contain("\"record_id\"");
    }

    [Fact]
    public void BuildCreateTable_NullableRecord_GeneratesNullableColumns()
    {
        var ddl = DuckDBSchemaBuilder.BuildCreateTable<NullableTestRecord>("nullable_table");

        ddl.Should().Contain("\"Id\" INTEGER NOT NULL");

        // Nullable string should not have NOT NULL
        ddl.Should().Contain("\"Name\" VARCHAR");
        ddl.Should().Contain("\"OptionalValue\" DOUBLE");
    }

    [Fact]
    public void BuildCreateTable_AllTypes_MapsCorrectly()
    {
        var ddl = DuckDBSchemaBuilder.BuildCreateTable<AllTypesRecord>("types_table");

        ddl.Should().Contain("\"BoolValue\" BOOLEAN");
        ddl.Should().Contain("\"ByteValue\" UTINYINT");
        ddl.Should().Contain("\"ShortValue\" SMALLINT");
        ddl.Should().Contain("\"IntValue\" INTEGER");
        ddl.Should().Contain("\"LongValue\" BIGINT");
        ddl.Should().Contain("\"FloatValue\" FLOAT");
        ddl.Should().Contain("\"DoubleValue\" DOUBLE");
        ddl.Should().Contain("\"DecimalValue\" DECIMAL");
        ddl.Should().Contain("\"StringValue\" VARCHAR");
        ddl.Should().Contain("\"DateTimeValue\" TIMESTAMP");
    }

    [Fact]
    public void BuildCreateTable_GuidProperty_MapsToUUID()
    {
        var ddl = DuckDBSchemaBuilder.BuildCreateTable<GuidRecord>("guid_table");

        ddl.Should().Contain("\"TraceId\" UUID");
    }

    [Fact]
    public void BuildCreateTable_EnumProperty_MapsToVarchar()
    {
        var ddl = DuckDBSchemaBuilder.BuildCreateTable<EnumRecord>("enum_table");

        ddl.Should().Contain("\"Status\" VARCHAR");
    }

    [Fact]
    public void BuildCreateTable_IgnoredColumn_IsExcluded()
    {
        var ddl = DuckDBSchemaBuilder.BuildCreateTable<CustomColumnRecord>("skip_table");

        ddl.Should().NotContain("Ignored");
    }

    [Fact]
    public void BuildCreateTable_CustomColumnName_UsesAttributeName()
    {
        var ddl = DuckDBSchemaBuilder.BuildCreateTable<CustomColumnRecord>("custom_table");

        ddl.Should().Contain("\"record_id\"");
        ddl.Should().Contain("\"record_name\"");
        ddl.Should().NotContain("\"RecordId\"");
        ddl.Should().NotContain("\"RecordName\"");
    }
}

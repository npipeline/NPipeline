using NPipeline.Connectors.Snowflake.Connection;
using NPipeline.Connectors.Snowflake.Nodes;

namespace NPipeline.Connectors.Snowflake.Tests.Nodes;

public sealed class SnowflakeSinkNodeTests
{
    [Fact]
    public void Constructor_WithNullConnectionString_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(() => new SnowflakeSinkNode<TestEntity>(
            connectionString: null!, tableName: "CUSTOMERS"));
    }

    [Fact]
    public void Constructor_WithNullTableName_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(() => new SnowflakeSinkNode<TestEntity>(
            "account=test;user=test;password=test;db=test;", null!));
    }

    [Fact]
    public void Constructor_WithValidArguments_ShouldNotThrow()
    {
        var exception = Record.Exception(() => new SnowflakeSinkNode<TestEntity>(
            "account=test;user=test;password=test;db=test;",
            "CUSTOMERS"));

        Assert.Null(exception);
    }

    [Fact]
    public void Constructor_WithNullConnectionPool_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(() => new SnowflakeSinkNode<TestEntity>(
            (ISnowflakeConnectionPool)null!,
            "CUSTOMERS"));
    }

    public sealed class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

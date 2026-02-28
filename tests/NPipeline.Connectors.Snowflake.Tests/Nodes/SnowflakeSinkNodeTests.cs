using NPipeline.Connectors.Snowflake.Nodes;

namespace NPipeline.Connectors.Snowflake.Tests.Nodes;

public sealed class SnowflakeSinkNodeTests
{
    [Fact]
    public void Constructor_WithNullConnectionString_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(
            () => new SnowflakeSinkNode<TestEntity>(
                connectionString: null!, tableName: "CUSTOMERS"));
    }

    [Fact]
    public void Constructor_WithNullTableName_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(
            () => new SnowflakeSinkNode<TestEntity>(
                connectionString: "account=test;user=test;password=test;db=test;", tableName: null!));
    }

    [Fact]
    public void Constructor_WithValidArguments_ShouldNotThrow()
    {
        var exception = Record.Exception(() => new SnowflakeSinkNode<TestEntity>(
            connectionString: "account=test;user=test;password=test;db=test;",
            tableName: "CUSTOMERS"));

        Assert.Null(exception);
    }

    [Fact]
    public void Constructor_WithNullConnectionPool_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(
            () => new SnowflakeSinkNode<TestEntity>(
                (NPipeline.Connectors.Snowflake.Connection.ISnowflakeConnectionPool)null!,
                "CUSTOMERS"));
    }

    public sealed class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

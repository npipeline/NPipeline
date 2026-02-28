using NPipeline.Connectors.Snowflake.Nodes;

namespace NPipeline.Connectors.Snowflake.Tests.Nodes;

public sealed class SnowflakeSourceNodeTests
{
    [Fact]
    public void Constructor_WithNullConnectionString_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(
            () => new SnowflakeSourceNode<TestEntity>(
                connectionString: null!, query: "SELECT 1"));
    }

    [Fact]
    public void Constructor_WithEmptyConnectionString_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(
            () => new SnowflakeSourceNode<TestEntity>(
                connectionString: "", query: "SELECT 1"));
    }

    [Fact]
    public void Constructor_WithNullQuery_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(
            () => new SnowflakeSourceNode<TestEntity>(
                connectionString: "account=test;user=test;password=test;db=test;", query: null!));
    }

    [Fact]
    public void Constructor_WithEmptyQuery_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(
            () => new SnowflakeSourceNode<TestEntity>(
                connectionString: "account=test;user=test;password=test;db=test;", query: ""));
    }

    [Fact]
    public void Constructor_WithValidArguments_ShouldNotThrow()
    {
        var exception = Record.Exception(() => new SnowflakeSourceNode<TestEntity>(
            connectionString: "account=test;user=test;password=test;db=test;",
            query: "SELECT * FROM CUSTOMERS"));

        Assert.Null(exception);
    }

    [Fact]
    public void Constructor_WithNullConnectionPool_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(
            () => new SnowflakeSourceNode<TestEntity>(
                connectionPool: (NPipeline.Connectors.Snowflake.Connection.ISnowflakeConnectionPool)null!,
                query: "SELECT 1"));
    }

    public sealed class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

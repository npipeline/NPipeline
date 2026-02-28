using NPipeline.Connectors.Snowflake.Connection;
using NPipeline.Connectors.Snowflake.Nodes;

namespace NPipeline.Connectors.Snowflake.Tests.Nodes;

public sealed class SnowflakeSourceNodeTests
{
    [Fact]
    public void Constructor_WithNullConnectionString_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(() => new SnowflakeSourceNode<TestEntity>(
            connectionString: null!, query: "SELECT 1"));
    }

    [Fact]
    public void Constructor_WithEmptyConnectionString_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(() => new SnowflakeSourceNode<TestEntity>(
            "", "SELECT 1"));
    }

    [Fact]
    public void Constructor_WithNullQuery_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(() => new SnowflakeSourceNode<TestEntity>(
            "account=test;user=test;password=test;db=test;", null!));
    }

    [Fact]
    public void Constructor_WithEmptyQuery_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(() => new SnowflakeSourceNode<TestEntity>(
            "account=test;user=test;password=test;db=test;", ""));
    }

    [Fact]
    public void Constructor_WithValidArguments_ShouldNotThrow()
    {
        var exception = Record.Exception(() => new SnowflakeSourceNode<TestEntity>(
            "account=test;user=test;password=test;db=test;",
            "SELECT * FROM CUSTOMERS"));

        Assert.Null(exception);
    }

    [Fact]
    public void Constructor_WithNullConnectionPool_ShouldThrow()
    {
        Assert.Throws<ArgumentNullException>(() => new SnowflakeSourceNode<TestEntity>(
            (ISnowflakeConnectionPool)null!,
            "SELECT 1"));
    }

    public sealed class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

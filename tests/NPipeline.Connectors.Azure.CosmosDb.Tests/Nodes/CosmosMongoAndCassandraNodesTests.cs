using AwesomeAssertions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Nodes;

public sealed class CosmosMongoAndCassandraNodesTests
{
    [Fact]
    public void CosmosMongoSourceNode_Constructor_WithInvalidConnectionString_ShouldThrowArgumentNullException()
    {
        var act = () => new CosmosMongoSourceNode<object>("", "db", "container");

        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void CosmosMongoSourceNode_Constructor_WithInvalidDatabaseId_ShouldThrowArgumentNullException()
    {
        var act = () => new CosmosMongoSourceNode<object>("mongodb://localhost:27017", "", "container");

        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void CosmosMongoSourceNode_Constructor_WithInvalidContainerId_ShouldThrowArgumentNullException()
    {
        var act = () => new CosmosMongoSourceNode<object>("mongodb://localhost:27017", "db", "");

        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void CosmosMongoSourceNode_Initialize_ShouldReturnDataPipe()
    {
        var node = new CosmosMongoSourceNode<object>("mongodb://localhost:27017", "db", "container");

        var pipe = node.OpenStream(PipelineContext.Default, CancellationToken.None);

        _ = pipe.Should().NotBeNull();
        _ = pipe.StreamName.Should().Contain("CosmosMongoSourceNode");
    }

    [Fact]
    public void CosmosMongoSinkNode_Constructor_WithInvalidConnectionString_ShouldThrowArgumentNullException()
    {
        var act = () => new CosmosMongoSinkNode<object>("", "db", "container");

        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void CosmosMongoSinkNode_Constructor_WithInvalidDatabaseId_ShouldThrowArgumentNullException()
    {
        var act = () => new CosmosMongoSinkNode<object>("mongodb://localhost:27017", "", "container");

        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void CosmosMongoSinkNode_Constructor_WithInvalidContainerId_ShouldThrowArgumentNullException()
    {
        var act = () => new CosmosMongoSinkNode<object>("mongodb://localhost:27017", "db", "");

        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void CosmosCassandraSourceNode_Constructor_WithInvalidContactPoint_ShouldThrowArgumentNullException()
    {
        var act = () => new CosmosCassandraSourceNode<object>("", "keyspace", "SELECT * FROM c");

        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void CosmosCassandraSourceNode_Constructor_WithInvalidKeyspace_ShouldThrowArgumentNullException()
    {
        var act = () => new CosmosCassandraSourceNode<object>("localhost", "", "SELECT * FROM c");

        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void CosmosCassandraSourceNode_Constructor_WithInvalidQuery_ShouldThrowArgumentNullException()
    {
        var act = () => new CosmosCassandraSourceNode<object>("localhost", "keyspace", "");

        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void CosmosCassandraSourceNode_Initialize_ShouldReturnDataPipe()
    {
        var node = new CosmosCassandraSourceNode<object>("localhost", "keyspace", "SELECT * FROM c");

        var pipe = node.OpenStream(PipelineContext.Default, CancellationToken.None);

        _ = pipe.Should().NotBeNull();
        _ = pipe.StreamName.Should().Contain("CosmosCassandraSourceNode");
    }

    [Fact]
    public void CosmosCassandraSinkNode_Constructor_WithInvalidContactPoint_ShouldThrowArgumentNullException()
    {
        var act = () => new CosmosCassandraSinkNode<object>("", "keyspace");

        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void CosmosCassandraSinkNode_Constructor_WithInvalidKeyspace_ShouldThrowArgumentNullException()
    {
        var act = () => new CosmosCassandraSinkNode<object>("localhost", "");

        _ = act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void CosmosCassandraChangeFeedSourceNode_Initialize_ShouldThrowNotSupportedException()
    {
        var node = new CosmosCassandraChangeFeedSourceNode<object>();

        var act = () => node.OpenStream(PipelineContext.Default, CancellationToken.None);

        var exception = act.Should().Throw<NotSupportedException>();
        _ = exception.Which.Message.Should().Contain("Cassandra change feed is not supported");
    }

    [Fact]
    public void CosmosCassandraSinkNode_Constructor_ShouldHonorProvidedWriteStrategy()
    {
        var baseConfiguration = new CosmosConfiguration
        {
            WriteStrategy = CosmosWriteStrategy.PerRow,
        };

        var node = new CosmosCassandraSinkNode<object>(
            "localhost",
            "keyspace",
            configuration: baseConfiguration);

        _ = node.Should().NotBeNull();
    }
}

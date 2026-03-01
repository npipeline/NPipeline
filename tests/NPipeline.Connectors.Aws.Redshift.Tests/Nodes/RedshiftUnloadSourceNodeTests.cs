using Amazon.S3;
using FakeItEasy;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Nodes;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Nodes;

public class RedshiftUnloadSourceNodeTests
{
    private readonly IRedshiftConnectionPool _fakeConnectionPool;
    private readonly IAmazonS3 _fakeS3Client;

    public RedshiftUnloadSourceNodeTests()
    {
        _fakeConnectionPool = A.Fake<IRedshiftConnectionPool>();
        _fakeS3Client = A.Fake<IAmazonS3>();
    }

    [Fact]
    public void Constructor_WithNullConnectionPool_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new RedshiftUnloadSourceNode<TestRow>(
                null!,
                "SELECT * FROM test",
                new UnloadConfiguration(),
                _fakeS3Client));
    }

    [Fact]
    public void Constructor_WithNullQuery_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new RedshiftUnloadSourceNode<TestRow>(
                _fakeConnectionPool,
                null!,
                new UnloadConfiguration(),
                _fakeS3Client));
    }

    [Fact]
    public void Constructor_WithNullConfig_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new RedshiftUnloadSourceNode<TestRow>(
                _fakeConnectionPool,
                "SELECT * FROM test",
                null!,
                _fakeS3Client));
    }

    [Fact]
    public void Constructor_WithNullS3Client_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new RedshiftUnloadSourceNode<TestRow>(
                _fakeConnectionPool,
                "SELECT * FROM test",
                new UnloadConfiguration(),
                null!));
    }

    [Fact]
    public void Constructor_WithValidArguments_DoesNotThrow()
    {
        var node = new RedshiftUnloadSourceNode<TestRow>(
            _fakeConnectionPool,
            "SELECT * FROM test",
            new UnloadConfiguration
            {
                S3BucketName = "test-bucket",
                IamRoleArn = "arn:aws:iam::123:role/TestRole",
            },
            _fakeS3Client);

        Assert.NotNull(node);
        Assert.Contains("RedshiftUnload", node.NodeId);
    }

    [Fact]
    public void ConfigValidate_WithoutS3BucketName_ThrowsInvalidOperationException()
    {
        var config = new UnloadConfiguration();
        Assert.Throws<InvalidOperationException>(() => config.Validate());
    }

    [Fact]
    public void ConfigValidate_WithoutIamRoleArn_ThrowsInvalidOperationException()
    {
        var config = new UnloadConfiguration { S3BucketName = "test-bucket" };
        Assert.Throws<InvalidOperationException>(() => config.Validate());
    }

    [Fact]
    public void ConfigValidate_WithValidConfig_DoesNotThrow()
    {
        var config = new UnloadConfiguration
        {
            S3BucketName = "test-bucket",
            IamRoleArn = "arn:aws:iam::123:role/TestRole",
        };

        config.Validate(); // Should not throw
    }

    [Fact]
    public void NodeId_ContainsTypeName()
    {
        var node = new RedshiftUnloadSourceNode<TestRow>(
            _fakeConnectionPool,
            "SELECT * FROM test",
            new UnloadConfiguration
            {
                S3BucketName = "test-bucket",
                IamRoleArn = "arn:aws:iam::123:role/TestRole",
            },
            _fakeS3Client);

        Assert.Contains("TestRow", node.NodeId);
    }

    [Fact]
    public void NodeId_ContainsRedshiftUnloadPrefix()
    {
        var node = new RedshiftUnloadSourceNode<TestRow>(
            _fakeConnectionPool,
            "SELECT * FROM test",
            new UnloadConfiguration
            {
                S3BucketName = "test-bucket",
                IamRoleArn = "arn:aws:iam::123:role/TestRole",
            },
            _fakeS3Client);

        Assert.StartsWith("RedshiftUnload_", node.NodeId);
    }

    private sealed class TestRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

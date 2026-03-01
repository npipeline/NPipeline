using FakeItEasy;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Nodes;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Nodes;

public class RedshiftSpectrumSourceNodeTests
{
    private readonly IRedshiftConnectionPool _fakeConnectionPool;

    public RedshiftSpectrumSourceNodeTests()
    {
        _fakeConnectionPool = A.Fake<IRedshiftConnectionPool>();
    }

    [Fact]
    public void Constructor_WithNullConnectionPool_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new RedshiftSpectrumSourceNode<TestRow>(
                (IRedshiftConnectionPool)null!,
                "SELECT * FROM spectrum.external_table",
                new SpectrumConfiguration()));
    }

    [Fact]
    public void Constructor_WithNullQuery_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new RedshiftSpectrumSourceNode<TestRow>(
                _fakeConnectionPool,
                null!,
                new SpectrumConfiguration()));
    }

    [Fact]
    public void Constructor_WithNullConfig_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new RedshiftSpectrumSourceNode<TestRow>(
                _fakeConnectionPool,
                "SELECT * FROM spectrum.external_table",
                null!));
    }

    [Fact]
    public void Constructor_WithValidArguments_DoesNotThrow()
    {
        var node = new RedshiftSpectrumSourceNode<TestRow>(
            _fakeConnectionPool,
            "SELECT * FROM spectrum.external_table",
            new SpectrumConfiguration
            {
                IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
            });

        Assert.NotNull(node);
        Assert.Contains("RedshiftSpectrum", node.NodeId);
    }

    [Fact]
    public void Initialize_WithoutIamRoleArn_ThrowsInvalidOperationException()
    {
        var node = new RedshiftSpectrumSourceNode<TestRow>(
            _fakeConnectionPool,
            "SELECT * FROM spectrum.external_table",
            new SpectrumConfiguration());

        Assert.Throws<InvalidOperationException>(() => node.Initialize());
    }

    [Fact]
    public void Initialize_WithoutExternalSchema_ThrowsInvalidOperationException()
    {
        var node = new RedshiftSpectrumSourceNode<TestRow>(
            _fakeConnectionPool,
            "SELECT * FROM spectrum.external_table",
            new SpectrumConfiguration
            {
                IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
                ExternalSchema = "",
            });

        Assert.Throws<InvalidOperationException>(() => node.Initialize());
    }

    [Fact]
    public void Initialize_WithValidConfig_DoesNotThrow()
    {
        var node = new RedshiftSpectrumSourceNode<TestRow>(
            _fakeConnectionPool,
            "SELECT * FROM spectrum.external_table",
            new SpectrumConfiguration
            {
                IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
            });

        node.Initialize(); // Should not throw
    }

    [Fact]
    public void Initialize_WithCreateIfNotExistsButNoS3Path_ThrowsInvalidOperationException()
    {
        var node = new RedshiftSpectrumSourceNode<TestRow>(
            _fakeConnectionPool,
            "SELECT * FROM spectrum.external_table",
            new SpectrumConfiguration
            {
                IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
                CreateIfNotExists = true,
                ColumnDefinitions = "id INT, name VARCHAR(100)",
            });

        Assert.Throws<InvalidOperationException>(() => node.Initialize());
    }

    [Fact]
    public void Initialize_WithCreateIfNotExistsButNoColumnDefinitions_ThrowsInvalidOperationException()
    {
        var node = new RedshiftSpectrumSourceNode<TestRow>(
            _fakeConnectionPool,
            "SELECT * FROM spectrum.external_table",
            new SpectrumConfiguration
            {
                IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
                CreateIfNotExists = true,
                S3Path = "s3://my-bucket/data/",
            });

        Assert.Throws<InvalidOperationException>(() => node.Initialize());
    }

    [Fact]
    public void Initialize_WithCreateIfNotExistsValid_DoesNotThrow()
    {
        var node = new RedshiftSpectrumSourceNode<TestRow>(
            _fakeConnectionPool,
            "SELECT * FROM spectrum.external_table",
            new SpectrumConfiguration
            {
                IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
                CreateIfNotExists = true,
                S3Path = "s3://my-bucket/data/",
                ColumnDefinitions = "id INT, name VARCHAR(100)",
            });

        node.Initialize(); // Should not throw
    }

    [Fact]
    public void NodeId_ContainsTypeName()
    {
        var node = new RedshiftSpectrumSourceNode<TestRow>(
            _fakeConnectionPool,
            "SELECT * FROM spectrum.external_table",
            new SpectrumConfiguration
            {
                IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
            });

        Assert.Contains("TestRow", node.NodeId);
    }

    [Fact]
    public void NodeId_ContainsRedshiftSpectrumPrefix()
    {
        var node = new RedshiftSpectrumSourceNode<TestRow>(
            _fakeConnectionPool,
            "SELECT * FROM spectrum.external_table",
            new SpectrumConfiguration
            {
                IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
            });

        Assert.StartsWith("RedshiftSpectrum_", node.NodeId);
    }

    [Fact]
    public void ConfigValidate_WithoutIamRoleArn_ThrowsInvalidOperationException()
    {
        var config = new SpectrumConfiguration();
        Assert.Throws<InvalidOperationException>(() => config.Validate());
    }

    [Fact]
    public void ConfigValidate_WithoutExternalSchema_ThrowsInvalidOperationException()
    {
        var config = new SpectrumConfiguration
        {
            IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
            ExternalSchema = "",
        };

        Assert.Throws<InvalidOperationException>(() => config.Validate());
    }

    [Fact]
    public void ConfigValidate_WithValidConfig_DoesNotThrow()
    {
        var config = new SpectrumConfiguration
        {
            IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
        };

        config.Validate(); // Should not throw
    }

    [Fact]
    public void ConfigValidate_WithCreateIfNotExistsButNoS3Path_ThrowsInvalidOperationException()
    {
        var config = new SpectrumConfiguration
        {
            IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
            CreateIfNotExists = true,
            ColumnDefinitions = "id INT, name VARCHAR(100)",
        };

        Assert.Throws<InvalidOperationException>(() => config.Validate());
    }

    [Fact]
    public void ConfigValidate_WithCreateIfNotExistsButNoColumnDefinitions_ThrowsInvalidOperationException()
    {
        var config = new SpectrumConfiguration
        {
            IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
            CreateIfNotExists = true,
            S3Path = "s3://my-bucket/data/",
        };

        Assert.Throws<InvalidOperationException>(() => config.Validate());
    }

    [Fact]
    public void ConfigValidate_WithCreateIfNotExistsValid_DoesNotThrow()
    {
        var config = new SpectrumConfiguration
        {
            IamRoleArn = "arn:aws:iam::123:role/SpectrumRole",
            CreateIfNotExists = true,
            S3Path = "s3://my-bucket/data/",
            ColumnDefinitions = "id INT, name VARCHAR(100)",
        };

        config.Validate(); // Should not throw
    }

    [Fact]
    public void Config_DefaultValues_AreCorrect()
    {
        var config = new SpectrumConfiguration();

        Assert.Equal("spectrum", config.ExternalSchema);
        Assert.Equal("default", config.ExternalDatabase);
        Assert.Equal("PARQUET", config.FileFormat);
        Assert.False(config.CreateIfNotExists);
        Assert.False(config.UseManifest);
    }

    private sealed class TestRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

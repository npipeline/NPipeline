using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Mapping;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Configuration;

public class RedshiftConfigurationTests
{
    [Fact]
    public void Validate_WhenCopyFromS3WithoutBucketName_ShouldThrow()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            IamRoleArn = "arn:aws:iam::123456789012:role/TestRole",
        };

        // Act
        var act = () => config.Validate();

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*S3BucketName*");
    }

    [Fact]
    public void Validate_WhenCopyFromS3WithoutIamRole_ShouldThrow()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            S3BucketName = "my-bucket",
        };

        // Act
        var act = () => config.Validate();

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*IamRoleArn*");
    }

    [Fact]
    public void Validate_WhenUpsertWithoutKeyColumns_ShouldThrow()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = null,
        };

        // Act
        var act = () => config.Validate();

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*UpsertKeyColumns*");
    }

    [Fact]
    public void Validate_WhenUpsertWithEmptyKeyColumns_ShouldThrow()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = Array.Empty<string>(),
        };

        // Act
        var act = () => config.Validate();

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*UpsertKeyColumns*");
    }

    [Fact]
    public void Validate_WhenAllRequiredFieldsPresent_ShouldNotThrow()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.Batch,
        };

        // Act
        var act = () => config.Validate();

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void DefaultValues_ShouldMatchDocumentedDefaults()
    {
        // Arrange & Act
        var config = new RedshiftConfiguration();

        // Assert
        config.Port.Should().Be(5439);
        config.Schema.Should().Be("public");
        config.CommandTimeout.Should().Be(300);
        config.ConnectionTimeout.Should().Be(30);
        config.MinPoolSize.Should().Be(1);
        config.MaxPoolSize.Should().Be(10);
        config.StreamResults.Should().BeTrue();
        config.FetchSize.Should().Be(10_000);
        config.QueryGroup.Should().Be("npipeline");
        config.WriteStrategy.Should().Be(RedshiftWriteStrategy.Batch);
        config.BatchSize.Should().Be(1_000);
        config.MaxBatchSize.Should().Be(50_000);
        config.UseTransaction.Should().BeTrue();
        config.UseUpsert.Should().BeFalse();
        config.OnMergeAction.Should().Be(OnMergeAction.Update);
        config.UseMergeSyntax.Should().BeFalse();
        config.StagingTablePrefix.Should().Be("#npipeline_stage_");
        config.UseTempStagingTable.Should().BeTrue();
        config.StagingDistributionStyle.Should().Be(RedshiftDistributionStyle.Auto);
        config.S3KeyPrefix.Should().Be("npipeline/redshift/");
        config.CopyFileFormat.Should().Be("CSV");
        config.CopyCompression.Should().Be("GZIP");
        config.PurgeS3FilesAfterCopy.Should().BeTrue();
        config.CopyOnErrorAction.Should().Be("ABORT_STATEMENT");
        config.MaxRetryAttempts.Should().Be(3);
        config.RetryDelay.Should().Be(TimeSpan.FromSeconds(2));
        config.ContinueOnError.Should().BeFalse();
        config.ThrowOnMappingError.Should().BeTrue();
        config.NamingConvention.Should().Be(RedshiftNamingConvention.PascalToSnakeCase);
        config.ValidateIdentifiers.Should().BeTrue();
    }

    [Fact]
    public void BuildConnectionString_WhenConnectionStringSet_ShouldReturnIt()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            ConnectionString = "Host=existing;Database=db",
        };

        // Act
        var result = config.BuildConnectionString();

        // Assert
        result.Should().Be("Host=existing;Database=db");
    }

    [Fact]
    public void BuildConnectionString_WhenComponentsSet_ShouldBuildValidConnectionString()
    {
        // Arrange
        var config = new RedshiftConfiguration
        {
            Host = "my-cluster.us-east-1.redshift.amazonaws.com",
            Port = 5439,
            Database = "analytics",
            Username = "etl_user",
            Password = "secret123",
            ConnectionTimeout = 60,
            CommandTimeout = 600,
            MinPoolSize = 5,
            MaxPoolSize = 20,
        };

        // Act
        var result = config.BuildConnectionString();

        // Assert
        result.Should().Contain("Host=my-cluster.us-east-1.redshift.amazonaws.com");
        result.Should().Contain("Port=5439");
        result.Should().Contain("Database=analytics");
        result.Should().Contain("Username=etl_user");
        result.Should().Contain("Password=secret123");
        result.Should().Contain("Timeout=60");
        result.Should().Contain("Command Timeout=600");
        result.Should().Contain("Minimum Pool Size=5");
        result.Should().Contain("Maximum Pool Size=20");
        result.Should().Contain("SSL Mode=Require");
    }
}

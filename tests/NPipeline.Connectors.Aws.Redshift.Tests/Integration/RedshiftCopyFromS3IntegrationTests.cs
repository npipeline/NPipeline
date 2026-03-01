using Amazon.S3;
using Amazon.S3.Model;
using NPipeline.Connectors.Aws.Redshift.Tests.Fixtures;
using NPipeline.Connectors.Aws.Redshift.Tests.Helpers;
using NPipeline.Connectors.Aws.Redshift.Writers;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Integration;

[Trait("Category", "Integration")]
[Trait("Category", "LiveRedshift")]
[Trait("Category", "RequiresS3")]
public sealed class RedshiftCopyFromS3IntegrationTests : IClassFixture<RedshiftTestFixture>, IAsyncLifetime
{
    private readonly RedshiftTestFixture _fixture;
    private IAmazonS3? _s3Client;

    public RedshiftCopyFromS3IntegrationTests(RedshiftTestFixture fixture)
    {
        _fixture = fixture;
    }

    public Task InitializeAsync()
    {
        if (_fixture.IsS3Configured)
            _s3Client = new AmazonS3Client();

        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        if (_s3Client is not null)
            _s3Client.Dispose();

        return Task.CompletedTask;
    }

    [SkippableFact]
    public async Task CopyFromS3Writer_UploadsCsv_And_IssuesCopy()
    {
        Skip.IfNot(_fixture.IsS3Configured, "S3 configuration not available");

        // Arrange
        await _fixture.CreateTableAsync("test_copy_s3", "id INT, name VARCHAR(100)");

        var config = RedshiftTestHelpers.CreateCopyFromS3Config(
            _fixture.ConnectionString,
            _fixture.S3Bucket,
            _fixture.IamRoleArn,
            _fixture.SchemaName);

        await using var writer = new RedshiftCopyFromS3Writer<S3TestRow>(
            _fixture.ConnectionPool,
            _fixture.SchemaName,
            "test_copy_s3",
            config,
            _s3Client!);

        // Act
        for (var i = 0; i < 50; i++)
        {
            await writer.WriteAsync(new S3TestRow { Id = i, Name = $"S3Test{i}" });
        }

        await writer.FlushAsync();

        // Assert
        var count = await _fixture.ExecuteScalarAsync($"SELECT COUNT(*) FROM \"{_fixture.SchemaName}\".test_copy_s3");
        Assert.Equal(50, count);
    }

    [SkippableFact]
    public async Task CopyFromS3Writer_WithUpsert_AppliesStagingPattern()
    {
        Skip.IfNot(_fixture.IsS3Configured, "S3 configuration not available");

        // Arrange
        await _fixture.CreateTableAsync("test_copy_s3_upsert", "id INT PRIMARY KEY, name VARCHAR(100)");
        await _fixture.ExecuteNonQueryAsync($"INSERT INTO \"{_fixture.SchemaName}\".test_copy_s3_upsert VALUES (1, 'Original')");

        var config = RedshiftTestHelpers.CreateCopyFromS3Config(
            _fixture.ConnectionString,
            _fixture.S3Bucket,
            _fixture.IamRoleArn,
            _fixture.SchemaName);

        config.UseUpsert = true;
        config.UpsertKeyColumns = ["id"];

        await using var writer = new RedshiftCopyFromS3Writer<S3TestRow>(
            _fixture.ConnectionPool,
            _fixture.SchemaName,
            "test_copy_s3_upsert",
            config,
            _s3Client!);

        // Act
        await writer.WriteAsync(new S3TestRow { Id = 1, Name = "Updated" });
        await writer.WriteAsync(new S3TestRow { Id = 2, Name = "New" });
        await writer.FlushAsync();

        // Assert
        var count = await _fixture.ExecuteScalarAsync($"SELECT COUNT(*) FROM \"{_fixture.SchemaName}\".test_copy_s3_upsert");
        Assert.Equal(2, count);
    }

    [SkippableFact]
    public async Task CopyFromS3Writer_PurgesFile_AfterSuccessfulCopy()
    {
        Skip.IfNot(_fixture.IsS3Configured, "S3 configuration not available");

        // Arrange
        await _fixture.CreateTableAsync("test_copy_s3_purge", "id INT, name VARCHAR(100)");

        var config = RedshiftTestHelpers.CreateCopyFromS3Config(
            _fixture.ConnectionString,
            _fixture.S3Bucket,
            _fixture.IamRoleArn,
            _fixture.SchemaName);

        config.PurgeS3FilesAfterCopy = true;
        config.S3KeyPrefix = $"test-purge/{Guid.NewGuid():N}/";

        await using var writer = new RedshiftCopyFromS3Writer<S3TestRow>(
            _fixture.ConnectionPool,
            _fixture.SchemaName,
            "test_copy_s3_purge",
            config,
            _s3Client!);

        // Act
        await writer.WriteAsync(new S3TestRow { Id = 1, Name = "Test" });
        await writer.FlushAsync();

        // Assert - S3 prefix should be empty (files purged)
        var listRequest = new ListObjectsV2Request
        {
            BucketName = _fixture.S3Bucket,
            Prefix = config.S3KeyPrefix,
        };

        var listResponse = await _s3Client!.ListObjectsV2Async(listRequest);
        Assert.Empty(listResponse.S3Objects);
    }

    [SkippableFact]
    public async Task CopyFromS3Writer_WithBatchSize_FlushesAtBoundary()
    {
        Skip.IfNot(_fixture.IsS3Configured, "S3 configuration not available");

        // Arrange
        await _fixture.CreateTableAsync("test_copy_s3_batch", "id INT, name VARCHAR(100)");

        var config = RedshiftTestHelpers.CreateCopyFromS3Config(
            _fixture.ConnectionString,
            _fixture.S3Bucket,
            _fixture.IamRoleArn,
            _fixture.SchemaName);

        config.BatchSize = 10; // Small batch size for testing

        await using var writer = new RedshiftCopyFromS3Writer<S3TestRow>(
            _fixture.ConnectionPool,
            _fixture.SchemaName,
            "test_copy_s3_batch",
            config,
            _s3Client!);

        // Act - Write 25 rows (should trigger 2 batch flushes + 5 remaining)
        for (var i = 0; i < 25; i++)
        {
            await writer.WriteAsync(new S3TestRow { Id = i, Name = $"BatchTest{i}" });
        }

        await writer.FlushAsync();

        // Assert
        var count = await _fixture.ExecuteScalarAsync($"SELECT COUNT(*) FROM \"{_fixture.SchemaName}\".test_copy_s3_batch");
        Assert.Equal(25, count);
    }

    public sealed class S3TestRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

using System.Data;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Nodes;
using NPipeline.Connectors.Aws.Redshift.Tests.Fixtures;
using NPipeline.Connectors.Aws.Redshift.Tests.Helpers;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Integration;

[Trait("Category", "Integration")]
[Trait("Category", "LiveRedshift")]
public sealed class RedshiftConnectorIntegrationTests : IClassFixture<RedshiftTestFixture>, IAsyncLifetime
{
    private readonly RedshiftTestFixture _fixture;

    public RedshiftConnectorIntegrationTests(RedshiftTestFixture fixture)
    {
        _fixture = fixture;
    }

    public Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }

    [SkippableFact]
    public async Task ConnectionString_WhenConfigured_CanOpenConnection()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        await using var pool = _fixture.ConnectionPool;
        var connection = await pool.GetConnectionAsync();

        Assert.NotNull(connection);
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [SkippableFact]
    public async Task CanExecuteSimpleSelect()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        await using var pool = _fixture.ConnectionPool;
        var connection = await pool.GetConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 AS test_value";

        var result = await command.ExecuteScalarAsync();
        Assert.Equal(1, Convert.ToInt32(result));
    }

    [SkippableFact]
    public async Task SourceNode_ReadFromTable_YieldsAllRows()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        // Arrange
        await _fixture.CreateTableAsync("test_source", "id INT, name VARCHAR(100)");
        await _fixture.ExecuteNonQueryAsync($"INSERT INTO \"{_fixture.SchemaName}\".test_source VALUES (1, 'Alice'), (2, 'Bob')");

        var config = RedshiftTestHelpers.CreateTestConfig(_fixture.ConnectionString, _fixture.SchemaName);

        var source = new RedshiftSourceNode<TestRow>(
            _fixture.ConnectionString,
            $"SELECT id, name FROM \"{_fixture.SchemaName}\".test_source ORDER BY id",
            configuration: config);

        // Act - Use Initialize() to get the data pipe
        var context = new PipelineContext();
        var dataPipe = source.Initialize(context, CancellationToken.None);

        var results = new List<TestRow>();

        await foreach (var row in dataPipe)
        {
            results.Add(row);
        }

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(2, results[1].Id);
        Assert.Equal("Bob", results[1].Name);

        await source.DisposeAsync();
    }

    [SkippableFact]
    public async Task SinkNode_PerRow_InsertsRows()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        // Arrange
        await _fixture.CreateTableAsync("test_sink_perrow", "id INT, name VARCHAR(100)");

        var config = RedshiftTestHelpers.CreateTestConfig(_fixture.ConnectionString, _fixture.SchemaName);
        config.WriteStrategy = RedshiftWriteStrategy.PerRow;

        var sink = new RedshiftSinkNode<TestRow>(
            _fixture.ConnectionString,
            "test_sink_perrow",
            RedshiftWriteStrategy.PerRow,
            config,
            _fixture.SchemaName);

        // Act - Create input data pipe and execute
        var inputData = new List<TestRow>
        {
            new() { Id = 1, Name = "Test1" },
            new() { Id = 2, Name = "Test2" },
        };

        var inputPipe = new InMemoryDataPipe<TestRow>(inputData, "test");
        var context = new PipelineContext();

        await sink.ExecuteAsync(inputPipe, context, CancellationToken.None);

        // Assert
        var count = await _fixture.ExecuteScalarAsync($"SELECT COUNT(*) FROM \"{_fixture.SchemaName}\".test_sink_perrow");
        Assert.Equal(2, count);
    }

    [SkippableFact]
    public async Task SinkNode_Batch_InsertsRows()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        // Arrange
        await _fixture.CreateTableAsync("test_sink_batch", "id INT, name VARCHAR(100)");

        var config = RedshiftTestHelpers.CreateTestConfig(_fixture.ConnectionString, _fixture.SchemaName);
        config.WriteStrategy = RedshiftWriteStrategy.Batch;
        config.BatchSize = 50;

        var sink = new RedshiftSinkNode<TestRow>(
            _fixture.ConnectionString,
            "test_sink_batch",
            RedshiftWriteStrategy.Batch,
            config,
            _fixture.SchemaName);

        // Act - Create input data pipe with 100 rows
        var inputData = Enumerable.Range(0, 100)
            .Select(i => new TestRow { Id = i, Name = $"Test{i}" })
            .ToList();

        var inputPipe = new InMemoryDataPipe<TestRow>(inputData, "test");
        var context = new PipelineContext();

        await sink.ExecuteAsync(inputPipe, context, CancellationToken.None);

        // Assert
        var count = await _fixture.ExecuteScalarAsync($"SELECT COUNT(*) FROM \"{_fixture.SchemaName}\".test_sink_batch");
        Assert.Equal(100, count);
    }

    [SkippableFact]
    public async Task SinkNode_Batch_UpsertStagingPattern_UpsertRows()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        // Arrange
        await _fixture.CreateTableAsync("test_sink_upsert", "id INT PRIMARY KEY, name VARCHAR(100)");
        await _fixture.ExecuteNonQueryAsync($"INSERT INTO \"{_fixture.SchemaName}\".test_sink_upsert VALUES (1, 'Original')");

        var config = RedshiftTestHelpers.CreateTestConfig(_fixture.ConnectionString, _fixture.SchemaName);
        config.WriteStrategy = RedshiftWriteStrategy.Batch;
        config.UseUpsert = true;
        config.UpsertKeyColumns = ["id"];

        var sink = new RedshiftSinkNode<TestRow>(
            _fixture.ConnectionString,
            "test_sink_upsert",
            RedshiftWriteStrategy.Batch,
            config,
            _fixture.SchemaName);

        // Act - Upsert: update id=1, insert id=2
        var inputData = new List<TestRow>
        {
            new() { Id = 1, Name = "Updated" },
            new() { Id = 2, Name = "New" },
        };

        var inputPipe = new InMemoryDataPipe<TestRow>(inputData, "test");
        var context = new PipelineContext();

        await sink.ExecuteAsync(inputPipe, context, CancellationToken.None);

        // Assert
        var count = await _fixture.ExecuteScalarAsync($"SELECT COUNT(*) FROM \"{_fixture.SchemaName}\".test_sink_upsert");
        Assert.Equal(2, count);
    }

    [SkippableFact]
    public async Task SourceNode_ThenSinkNode_RoundTrip_PreservesData()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        // Arrange
        await _fixture.CreateTableAsync("test_roundtrip_source", "id INT, name VARCHAR(100)");
        await _fixture.CreateTableAsync("test_roundtrip_dest", "id INT, name VARCHAR(100)");
        await _fixture.ExecuteNonQueryAsync($"INSERT INTO \"{_fixture.SchemaName}\".test_roundtrip_source VALUES (1, 'Alpha'), (2, 'Beta'), (3, 'Gamma')");

        var config = RedshiftTestHelpers.CreateTestConfig(_fixture.ConnectionString, _fixture.SchemaName);

        var source = new RedshiftSourceNode<TestRow>(
            _fixture.ConnectionString,
            $"SELECT id, name FROM \"{_fixture.SchemaName}\".test_roundtrip_source ORDER BY id",
            configuration: config);

        var sink = new RedshiftSinkNode<TestRow>(
            _fixture.ConnectionString,
            "test_roundtrip_dest",
            RedshiftWriteStrategy.Batch,
            config,
            _fixture.SchemaName);

        // Act - Get data from source and write to sink
        var context = new PipelineContext();
        var sourcePipe = source.Initialize(context, CancellationToken.None);

        await sink.ExecuteAsync(sourcePipe, context, CancellationToken.None);

        // Assert
        var count = await _fixture.ExecuteScalarAsync($"SELECT COUNT(*) FROM \"{_fixture.SchemaName}\".test_roundtrip_dest");
        Assert.Equal(3, count);

        await source.DisposeAsync();
    }

    [SkippableFact]
    public async Task SourceNode_ReadWithParameters_FiltersRows()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        // Arrange
        await _fixture.CreateTableAsync("test_params", "id INT, name VARCHAR(100)");

        await _fixture.ExecuteNonQueryAsync(
            $"INSERT INTO \"{_fixture.SchemaName}\".test_params VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

        var config = RedshiftTestHelpers.CreateTestConfig(_fixture.ConnectionString, _fixture.SchemaName);

        var source = new RedshiftSourceNode<TestRow>(
            _fixture.ConnectionString,
            $"SELECT id, name FROM \"{_fixture.SchemaName}\".test_params WHERE id = @id",
            parameters: [new DatabaseParameter("id", 2)],
            configuration: config);

        // Act
        var context = new PipelineContext();
        var dataPipe = source.Initialize(context, CancellationToken.None);

        var results = new List<TestRow>();

        await foreach (var row in dataPipe)
        {
            results.Add(row);
        }

        // Assert
        Assert.Single(results);
        Assert.Equal(2, results[0].Id);
        Assert.Equal("Bob", results[0].Name);

        await source.DisposeAsync();
    }

    [SkippableFact]
    public async Task SourceNode_ReadWithCustomMapper_AppliesMapper()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        // Arrange
        await _fixture.CreateTableAsync("test_custmapper", "id INT, name VARCHAR(100)");

        await _fixture.ExecuteNonQueryAsync(
            $"INSERT INTO \"{_fixture.SchemaName}\".test_custmapper VALUES (7, 'CustomName')");

        var config = RedshiftTestHelpers.CreateTestConfig(_fixture.ConnectionString, _fixture.SchemaName);

        // Custom mapper transforms the name to uppercase
        var source = new RedshiftSourceNode<TestRow>(
            _fixture.ConnectionString,
            $"SELECT id, name FROM \"{_fixture.SchemaName}\".test_custmapper",
            row => new TestRow
            {
                Id = row.Get<int>("id"),
                Name = (row.Get<string>("name") ?? string.Empty).ToUpperInvariant(),
            },
            config);

        // Act
        var context = new PipelineContext();
        var dataPipe = source.Initialize(context, CancellationToken.None);

        var results = new List<TestRow>();

        await foreach (var row in dataPipe)
        {
            results.Add(row);
        }

        // Assert
        Assert.Single(results);
        Assert.Equal(7, results[0].Id);
        Assert.Equal("CUSTOMNAME", results[0].Name); // mapper uppercased it

        await source.DisposeAsync();
    }

    [SkippableFact]
    public async Task SinkNode_Batch_UpsertMerge_UpsertRows()
    {
        Skip.IfNot(_fixture.IsConfigured, "Redshift connection string not configured");

        // Arrange
        await _fixture.CreateTableAsync("test_merge_upsert", "id INT PRIMARY KEY, name VARCHAR(100)");

        await _fixture.ExecuteNonQueryAsync(
            $"INSERT INTO \"{_fixture.SchemaName}\".test_merge_upsert VALUES (1, 'Original')");

        var config = RedshiftTestHelpers.CreateTestConfig(_fixture.ConnectionString, _fixture.SchemaName);
        config.WriteStrategy = RedshiftWriteStrategy.Batch;
        config.UseUpsert = true;
        config.UseMergeSyntax = true;
        config.UpsertKeyColumns = ["id"];
        config.UseTransaction = false; // MERGE statement does not require an outer transaction

        var sink = new RedshiftSinkNode<TestRow>(
            _fixture.ConnectionString,
            "test_merge_upsert",
            RedshiftWriteStrategy.Batch,
            config,
            _fixture.SchemaName);

        // Act — update id = 1, insert id = 3
        var inputData = new List<TestRow>
        {
            new() { Id = 1, Name = "Updated" },
            new() { Id = 3, Name = "NewViaMerge" },
        };

        var inputPipe = new InMemoryDataPipe<TestRow>(inputData, "test");
        var context = new PipelineContext();

        await sink.ExecuteAsync(inputPipe, context, CancellationToken.None);

        // Assert
        var count = await _fixture.ExecuteScalarAsync(
            $"SELECT COUNT(*) FROM \"{_fixture.SchemaName}\".test_merge_upsert");

        Assert.Equal(2, count);
    }

    [SkippableFact]
    public async Task SinkNode_CopyFromS3_LoadsRows()
    {
        Skip.IfNot(_fixture.IsS3Configured, "S3 + IAM configuration not available");

        // Arrange
        await _fixture.CreateTableAsync("test_copy_s3_sink", "id INT, name VARCHAR(100)");

        var config = RedshiftTestHelpers.CreateCopyFromS3Config(
            _fixture.ConnectionString,
            _fixture.S3Bucket,
            _fixture.IamRoleArn,
            _fixture.SchemaName);

        config.BatchSize = 50;

        var sink = new RedshiftSinkNode<TestRow>(
            _fixture.ConnectionString,
            "test_copy_s3_sink",
            RedshiftWriteStrategy.CopyFromS3,
            config,
            _fixture.SchemaName);

        // Act
        var inputData = Enumerable.Range(1, 50)
            .Select(i => new TestRow { Id = i, Name = $"S3Row{i}" })
            .ToList();

        var inputPipe = new InMemoryDataPipe<TestRow>(inputData, "test");
        var context = new PipelineContext();

        await sink.ExecuteAsync(inputPipe, context, CancellationToken.None);

        // Assert
        var count = await _fixture.ExecuteScalarAsync(
            $"SELECT COUNT(*) FROM \"{_fixture.SchemaName}\".test_copy_s3_sink");

        Assert.Equal(50, count);
    }

    // Test model
    public sealed class TestRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

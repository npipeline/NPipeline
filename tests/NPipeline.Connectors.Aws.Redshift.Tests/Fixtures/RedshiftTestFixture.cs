using Npgsql;
using NPipeline.Connectors.Aws.Redshift.Connection;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Fixtures;

/// <summary>
///     Test fixture for Redshift integration tests.
///     Creates a unique schema per test run and drops it on disposal.
/// </summary>
public sealed class RedshiftTestFixture : IAsyncLifetime
{
    private NpgsqlConnection? _connection;
    private RedshiftConnectionPool? _connectionPool;

    public RedshiftTestFixture()
    {
        SchemaName = $"npipeline_test_{Guid.NewGuid():N}";

        ConnectionString = Environment.GetEnvironmentVariable("NPIPELINE_REDSHIFT_CONNECTION_STRING") ?? "";
        S3Bucket = Environment.GetEnvironmentVariable("NPIPELINE_REDSHIFT_S3_BUCKET") ?? "";
        IamRoleArn = Environment.GetEnvironmentVariable("NPIPELINE_REDSHIFT_IAM_ROLE") ?? "";
        AwsRegion = Environment.GetEnvironmentVariable("AWS_DEFAULT_REGION") ?? "us-east-1";
    }

    public string ConnectionString { get; }
    public string S3Bucket { get; }
    public string IamRoleArn { get; }
    public string AwsRegion { get; }
    public string SchemaName { get; }

    public bool IsConfigured => !string.IsNullOrWhiteSpace(ConnectionString);
    public bool IsS3Configured => IsConfigured && !string.IsNullOrWhiteSpace(S3Bucket) && !string.IsNullOrWhiteSpace(IamRoleArn);

    public IRedshiftConnectionPool ConnectionPool
    {
        get
        {
            _connectionPool ??= new RedshiftConnectionPool(ConnectionString);
            return _connectionPool;
        }
    }

    public async Task InitializeAsync()
    {
        if (!IsConfigured)
            return;

        _connection = new NpgsqlConnection(ConnectionString);
        await _connection.OpenAsync();

        // Create unique schema for this test run
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"CREATE SCHEMA \"{SchemaName}\"";
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        if (_connection is not null)
        {
            // Drop test schema
            try
            {
                await using var cmd = _connection.CreateCommand();
                cmd.CommandText = $"DROP SCHEMA IF EXISTS \"{SchemaName}\" CASCADE";
                await cmd.ExecuteNonQueryAsync();
            }
            catch
            {
                // Swallow errors on cleanup
            }

            await _connection.CloseAsync();
            await _connection.DisposeAsync();
        }

        if (_connectionPool is not null)
            await _connectionPool.DisposeAsync();
    }

    public async Task CreateTableAsync(string tableName, string columns)
    {
        if (_connection is null)
            throw new InvalidOperationException("Fixture not initialized");

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"CREATE TABLE \"{SchemaName}\".\"{tableName}\" ({columns})";
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<int> ExecuteScalarAsync(string sql)
    {
        if (_connection is null)
            throw new InvalidOperationException("Fixture not initialized");

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    public async Task ExecuteNonQueryAsync(string sql)
    {
        if (_connection is null)
            throw new InvalidOperationException("Fixture not initialized");

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }
}

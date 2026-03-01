using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Writers;
using NPipeline.Connectors.Nodes;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Aws.Redshift.Nodes;

/// <summary>
///     Sink node that writes data to an AWS Redshift cluster.
///     Supports multiple write strategies: PerRow and Batch.
/// </summary>
/// <typeparam name="T">The type of objects consumed by sink.</typeparam>
public class RedshiftSinkNode<T> : DatabaseSinkNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => RedshiftStorageResolverFactory.CreateResolver(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly RedshiftConfiguration _configuration;
    private readonly string? _connectionName;
    private readonly IRedshiftConnectionPool? _connectionPool;
    private readonly string _schema;
    private readonly IStorageProvider? _storageProvider;
    private readonly IStorageResolver? _storageResolver;
    private readonly StorageUri? _storageUri;
    private readonly string _tableName;
    private readonly RedshiftWriteStrategy _writeStrategy;
    private IRedshiftConnectionPool? _writerConnectionPool;

    /// <summary>
    ///     Initializes a new instance of <see cref="RedshiftSinkNode{T}" /> class.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="schema">Optional schema name (default: public).</param>
    public RedshiftSinkNode(
        string connectionString,
        string tableName,
        RedshiftWriteStrategy writeStrategy = RedshiftWriteStrategy.Batch,
        RedshiftConfiguration? configuration = null,
        string? schema = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentNullException(nameof(tableName));

        _configuration = configuration ?? new RedshiftConfiguration();
        _configuration.Validate();
        _connectionPool = new RedshiftConnectionPool(connectionString);
        _tableName = tableName;
        _writeStrategy = writeStrategy;
        _schema = schema ?? _configuration.Schema;
        _connectionName = null;
        _storageProvider = null;
        _storageResolver = null;
        _storageUri = null;
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="RedshiftSinkNode{T}" /> class with connection pool.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="schema">Optional schema name (default: public).</param>
    /// <param name="connectionName">Optional named connection when using a shared pool.</param>
    public RedshiftSinkNode(
        IRedshiftConnectionPool connectionPool,
        string tableName,
        RedshiftWriteStrategy writeStrategy = RedshiftWriteStrategy.Batch,
        RedshiftConfiguration? configuration = null,
        string? schema = null,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);

        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentNullException(nameof(tableName));

        _configuration = configuration ?? new RedshiftConfiguration();
        _configuration.Validate();
        _connectionPool = connectionPool;
        _tableName = tableName;
        _writeStrategy = writeStrategy;
        _schema = schema ?? _configuration.Schema;

        _connectionName = string.IsNullOrWhiteSpace(connectionName)
            ? null
            : connectionName;

        _storageProvider = null;
        _storageResolver = null;
        _storageUri = null;
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="RedshiftSinkNode{T}" /> class using a <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing Redshift connection information.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="resolver">The storage resolver used to obtain storage provider.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="schema">Optional schema name (default: public).</param>
    public RedshiftSinkNode(
        StorageUri uri,
        string tableName,
        RedshiftWriteStrategy writeStrategy = RedshiftWriteStrategy.Batch,
        IStorageResolver? resolver = null,
        RedshiftConfiguration? configuration = null,
        string? schema = null)
    {
        ArgumentNullException.ThrowIfNull(uri);

        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentNullException(nameof(tableName));

        _storageUri = uri;
        _storageResolver = resolver;
        _configuration = configuration ?? new RedshiftConfiguration();
        _configuration.Validate();
        _tableName = tableName;
        _writeStrategy = writeStrategy;
        _schema = schema ?? _configuration.Schema;
        _connectionName = null;
        _connectionPool = null;
        _storageProvider = null;
    }

    /// <summary>
    ///     Gets whether to use transactions.
    /// </summary>
    protected override bool UseTransaction => _configuration.UseTransaction;

    /// <summary>
    ///     Gets batch size for batch writes.
    /// </summary>
    protected override int BatchSize => _configuration.BatchSize;

    /// <summary>
    ///     Gets whether to continue on error.
    /// </summary>
    protected override bool ContinueOnError => _configuration.ContinueOnError;

    /// <summary>
    ///     Gets a database connection asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected override async Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_storageUri != null)
        {
            var provider = _storageProvider ?? StorageProviderFactory.GetProviderOrThrow(
                _storageResolver ?? DefaultResolver.Value,
                _storageUri);

            if (provider is IDatabaseStorageProvider databaseProvider)
                return await databaseProvider.GetConnectionAsync(_storageUri, cancellationToken).ConfigureAwait(false);

            throw new InvalidOperationException($"Storage provider must implement {nameof(IDatabaseStorageProvider)} to use StorageUri.");
        }

        var connection = _connectionName is { Length: > 0 }
            ? await _connectionPool!.GetConnectionAsync(_connectionName, cancellationToken)
            : await _connectionPool!.GetConnectionAsync(cancellationToken);

        return new RedshiftDatabaseConnection(connection);
    }

    /// <summary>
    ///     Creates a database writer for the connection.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected override Task<IDatabaseWriter<T>> CreateWriterAsync(IDatabaseConnection connection, CancellationToken cancellationToken)
    {
        // Note: The Redshift writers use IRedshiftConnectionPool, not IDatabaseConnection
        // So we need to create them with the pool directly
        var writer = _writeStrategy switch
        {
            RedshiftWriteStrategy.PerRow => CreatePerRowWriter(),
            RedshiftWriteStrategy.Batch => CreateBatchWriter(),
            RedshiftWriteStrategy.CopyFromS3 => CreateCopyFromS3Writer(),
            _ => throw new NotSupportedException($"Write strategy '{_writeStrategy}' is not supported"),
        };

        return Task.FromResult(writer);
    }

    private IDatabaseWriter<T> CreatePerRowWriter()
    {
        return new RedshiftWriterAdapter<T>(
            new RedshiftPerRowWriter<T>(GetWriterConnectionPool(), _schema, _tableName, _configuration));
    }

    private IDatabaseWriter<T> CreateBatchWriter()
    {
        return new RedshiftWriterAdapter<T>(
            new RedshiftBatchWriter<T>(GetWriterConnectionPool(), _schema, _tableName, _configuration));
    }

    private IDatabaseWriter<T> CreateCopyFromS3Writer()
    {
        var s3Client = CreateS3Client(_configuration);

        return new RedshiftWriterAdapter<T>(
            new RedshiftCopyFromS3Writer<T>(GetWriterConnectionPool(), _schema, _tableName, _configuration, s3Client, true));
    }

    private IRedshiftConnectionPool GetWriterConnectionPool()
    {
        if (_connectionPool != null)
            return _connectionPool;

        if (_writerConnectionPool != null)
            return _writerConnectionPool;

        if (_storageUri == null)
            throw new InvalidOperationException("No Redshift connection pool is available for writer creation.");

        var provider = _storageProvider ?? StorageProviderFactory.GetProviderOrThrow(
            _storageResolver ?? DefaultResolver.Value,
            _storageUri);

        if (provider is not IDatabaseStorageProvider databaseProvider)
            throw new InvalidOperationException($"Storage provider must implement {nameof(IDatabaseStorageProvider)} to use StorageUri.");

        _writerConnectionPool = new RedshiftConnectionPool(databaseProvider.GetConnectionString(_storageUri));
        return _writerConnectionPool;
    }

    private static IAmazonS3 CreateS3Client(RedshiftConfiguration configuration)
    {
        var region = string.IsNullOrWhiteSpace(configuration.AwsRegion)
            ? null
            : RegionEndpoint.GetBySystemName(configuration.AwsRegion);

        if (!string.IsNullOrWhiteSpace(configuration.AwsAccessKeyId)
            && !string.IsNullOrWhiteSpace(configuration.AwsSecretAccessKey))
        {
            var credentials = new BasicAWSCredentials(configuration.AwsAccessKeyId, configuration.AwsSecretAccessKey);

            return region is null
                ? new AmazonS3Client(credentials)
                : new AmazonS3Client(credentials, region);
        }

        return region is null
            ? new AmazonS3Client()
            : new AmazonS3Client(region);
    }
}

/// <summary>
///     Adapter that wraps Redshift writers to implement IDatabaseWriter.
/// </summary>
/// <typeparam name="T">The type of row to write.</typeparam>
internal sealed class RedshiftWriterAdapter<T> : IDatabaseWriter<T>
{
    private readonly RedshiftBatchWriter<T>? _batchWriter;
    private readonly RedshiftCopyFromS3Writer<T>? _copyFromS3Writer;
    private readonly RedshiftPerRowWriter<T>? _perRowWriter;

    /// <summary>
    ///     Initializes a new instance with a per-row writer.
    /// </summary>
    public RedshiftWriterAdapter(RedshiftPerRowWriter<T> writer)
    {
        _perRowWriter = writer ?? throw new ArgumentNullException(nameof(writer));
    }

    /// <summary>
    ///     Initializes a new instance with a batch writer.
    /// </summary>
    public RedshiftWriterAdapter(RedshiftBatchWriter<T> writer)
    {
        _batchWriter = writer ?? throw new ArgumentNullException(nameof(writer));
    }

    /// <summary>
    ///     Initializes a new instance with a COPY FROM S3 writer.
    /// </summary>
    public RedshiftWriterAdapter(RedshiftCopyFromS3Writer<T> writer)
    {
        _copyFromS3Writer = writer ?? throw new ArgumentNullException(nameof(writer));
    }

    /// <summary>
    ///     Writes a single row.
    /// </summary>
    public async Task WriteAsync(T row, CancellationToken cancellationToken = default)
    {
        if (_perRowWriter != null)
            await _perRowWriter.WriteAsync(row, cancellationToken);
        else if (_batchWriter != null)
            await _batchWriter.WriteAsync(row, cancellationToken);
        else if (_copyFromS3Writer != null)
            await _copyFromS3Writer.WriteAsync(row, cancellationToken);
        else
            throw new InvalidOperationException("No writer configured");
    }

    /// <summary>
    ///     Writes a batch of rows.
    /// </summary>
    public async Task WriteBatchAsync(IEnumerable<T> batch, CancellationToken cancellationToken = default)
    {
        if (_batchWriter != null)
        {
            foreach (var row in batch)
            {
                await _batchWriter.WriteAsync(row, cancellationToken);
            }
        }
        else if (_copyFromS3Writer != null)
        {
            foreach (var row in batch)
            {
                await _copyFromS3Writer.WriteAsync(row, cancellationToken);
            }
        }
        else if (_perRowWriter != null)
        {
            foreach (var row in batch)
            {
                await _perRowWriter.WriteAsync(row, cancellationToken);
            }
        }
        else
            throw new InvalidOperationException("No writer configured");
    }

    /// <summary>
    ///     Flushes any pending writes.
    /// </summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_perRowWriter != null)
            await _perRowWriter.FlushAsync(cancellationToken);
        else if (_batchWriter != null)
            await _batchWriter.FlushAsync(cancellationToken);
        else if (_copyFromS3Writer != null)
            await _copyFromS3Writer.FlushAsync(cancellationToken);
    }

    /// <summary>
    ///     Disposes the writer.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_perRowWriter != null)
            await _perRowWriter.DisposeAsync();
        else if (_batchWriter != null)
            await _batchWriter.DisposeAsync();
        else if (_copyFromS3Writer != null)
            await _copyFromS3Writer.DisposeAsync();
    }
}

using System.Data;
using Microsoft.Azure.Cosmos;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Azure.CosmosDb.Connection;

/// <summary>
///     Cosmos DB implementation of IDatabaseCommand.
///     Wraps Container for executing SQL queries.
/// </summary>
internal sealed class CosmosDatabaseCommand : IDatabaseCommand
{
    private readonly Container _container;
    private readonly int _defaultFetchSize;
    private readonly int _defaultMaxConcurrency;
    private readonly Dictionary<string, object> _parameters = new();
    private string _commandText = string.Empty;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosDatabaseCommand" /> class.
    /// </summary>
    /// <param name="container">The Cosmos DB container.</param>
    /// <param name="defaultFetchSize">Default fetch size for queries.</param>
    /// <param name="defaultMaxConcurrency">Default max concurrency for queries.</param>
    public CosmosDatabaseCommand(Container container, int defaultFetchSize = 100, int defaultMaxConcurrency = 1)
    {
        _container = container;
        _defaultFetchSize = defaultFetchSize;
        _defaultMaxConcurrency = defaultMaxConcurrency;
        CommandTimeout = 60;
    }

    /// <summary>
    ///     Gets or sets the command text (SQL query).
    /// </summary>
    public string CommandText
    {
        get => _commandText;
        set => _commandText = value ?? string.Empty;
    }

    /// <summary>
    ///     Gets or sets the command timeout in seconds.
    /// </summary>
    public int CommandTimeout { get; set; }

    /// <summary>
    ///     Gets or sets the command type. For Cosmos DB, only Text is supported.
    /// </summary>
    public CommandType CommandType
    {
        get => CommandType.Text;
        set
        {
            if (value != CommandType.Text)
                throw new NotSupportedException($"Cosmos DB only supports {nameof(CommandType.Text)} command type.");
        }
    }

    /// <summary>
    ///     Adds a parameter to the command.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The parameter value.</param>
    public void AddParameter(string name, object? value)
    {
        if (value != null)
            _parameters[name] = value;
    }

    /// <summary>
    ///     Executes the query and returns a reader.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A database reader.</returns>
    public Task<IDatabaseReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (string.IsNullOrWhiteSpace(_commandText))
            throw new InvalidOperationException("CommandText must be set before executing.");

        var queryDefinition = BuildQueryDefinition();

        var requestOptions = new QueryRequestOptions
        {
            MaxItemCount = _defaultFetchSize,
            MaxConcurrency = _defaultMaxConcurrency,
        };

        var feedIterator = _container.GetItemQueryStreamIterator(
            queryDefinition,
            null,
            requestOptions);

        return Task.FromResult<IDatabaseReader>(new CosmosDatabaseReader(feedIterator));
    }

    /// <summary>
    ///     Executes a non-query command.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of affected rows.</returns>
    public Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            "Use the CosmosSinkNode for write operations. ExecuteNonQueryAsync is not supported for Cosmos DB.");
    }

    /// <summary>
    ///     Disposes the command asynchronously.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        _disposed = true;
        _parameters.Clear();
        return ValueTask.CompletedTask;
    }

    private QueryDefinition BuildQueryDefinition()
    {
        var query = new QueryDefinition(_commandText);

        foreach (var param in _parameters)
        {
            query.WithParameter(param.Key, param.Value);
        }

        return query;
    }
}

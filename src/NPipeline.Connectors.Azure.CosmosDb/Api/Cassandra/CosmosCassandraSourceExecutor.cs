using Cassandra;
using NPipeline.Connectors.Azure.CosmosDb.Abstractions;

namespace NPipeline.Connectors.Azure.CosmosDb.Api.Cassandra;

/// <summary>
///     Cassandra source executor using CQL statements.
/// </summary>
internal sealed class CosmosCassandraSourceExecutor : ICosmosSourceExecutor
{
    private readonly ISession _session;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosCassandraSourceExecutor" />.
    /// </summary>
    public CosmosCassandraSourceExecutor(ISession session)
    {
        _session = session;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<IDictionary<string, object?>>> QueryAsync(
        string query,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var statement = new SimpleStatement(query);
        var rowSet = await _session.ExecuteAsync(statement).WaitAsync(cancellationToken);
        var columns = rowSet.Columns?.Select(c => c.Name).ToArray() ?? [];

        var result = new List<IDictionary<string, object?>>();

        foreach (var row in rowSet)
        {
            var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

            foreach (var column in columns)
            {
                dict[column] = row.GetValue<object>(column);
            }

            result.Add(dict);
        }

        return result;
    }
}

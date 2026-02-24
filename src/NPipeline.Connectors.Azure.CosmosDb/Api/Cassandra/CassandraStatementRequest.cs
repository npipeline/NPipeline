namespace NPipeline.Connectors.Azure.CosmosDb.Api.Cassandra;

/// <summary>
///     Represents a Cassandra statement and its positional parameters.
/// </summary>
public sealed record CassandraStatementRequest(string Cql, params object?[] Parameters);

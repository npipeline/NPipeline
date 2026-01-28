using AwesomeAssertions;
using NPipeline.Connectors.PostgreSQL.Mapping;

namespace NPipeline.Connectors.PostgreSQL.Tests.Mapping;

public sealed class PostgresRowTests
{
    // Note: NpgsqlDataReader is sealed and cannot be faked with FakeItEasy
    // Tests for PostgresRow require a real PostgreSQL connection or a mock implementation
    // Integration tests should be used for testing PostgresRow behavior with actual data
}

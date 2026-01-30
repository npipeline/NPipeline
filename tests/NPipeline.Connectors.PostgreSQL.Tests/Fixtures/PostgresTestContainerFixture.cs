using Testcontainers.PostgreSql;

namespace NPipeline.Connectors.PostgreSQL.Tests.Fixtures;

public class PostgresTestContainerFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container;

    public PostgresTestContainerFixture()
    {
        _container = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .WithDatabase("testdb")
            .WithUsername("testuser")
            .WithPassword("testpass")
            .WithCleanUp(true)
            .Build();
    }

    public string ConnectionString => _container.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

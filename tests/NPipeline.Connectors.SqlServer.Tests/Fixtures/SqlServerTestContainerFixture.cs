using Testcontainers.MsSql;

namespace NPipeline.Connectors.SqlServer.Tests.Fixtures;

/// <summary>
///     Test container fixture for SQL Server integration tests.
///     Uses Testcontainers to create a reusable SQL Server instance.
///     Requires TESTCONTAINERS_REUSE_ENABLED=true environment variable for reuse.
/// </summary>
public class SqlServerTestContainerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container;

    public SqlServerTestContainerFixture()
    {
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("YourStrong@Passw0rd")
            .WithReuse(true)
            .WithLabel("npipeline-test", "sqlserver-integration")
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

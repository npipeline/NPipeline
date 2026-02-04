namespace NPipeline.Connectors.SqlServer.Tests;

/// <summary>
///     Unit tests for <see cref="SqlServerStorageResolverFactory" />.
/// </summary>
public sealed class SqlServerStorageResolverFactoryTests
{
    [Fact]
    public void CreateResolver_RegistersSqlServerDatabaseStorageProvider()
    {
        // Act
        var resolver = SqlServerStorageResolverFactory.CreateResolver();

        // Assert
        var providers = resolver.GetAvailableProviders();
        Assert.Single(providers);
        Assert.IsType<SqlServerDatabaseStorageProvider>(providers.First());
    }

    [Fact]
    public void CreateResolver_CanResolveSqlServerUri()
    {
        // Arrange
        var resolver = SqlServerStorageResolverFactory.CreateResolver();
        var uri = StorageUri.Parse("mssql://localhost/mydb");

        // Act
        var provider = resolver.ResolveProvider(uri);

        // Assert
        Assert.NotNull(provider);
        Assert.IsType<SqlServerDatabaseStorageProvider>(provider);
    }

    [Fact]
    public void CreateResolver_ReturnsCorrectProviderForMssqlScheme()
    {
        // Arrange
        var resolver = SqlServerStorageResolverFactory.CreateResolver();
        var mssqlUri = StorageUri.Parse("mssql://localhost/mydb");
        var sqlserverUri = StorageUri.Parse("sqlserver://localhost/mydb");

        // Act
        var mssqlProvider = resolver.ResolveProvider(mssqlUri);
        var sqlserverProvider = resolver.ResolveProvider(sqlserverUri);

        // Assert
        Assert.NotNull(mssqlProvider);
        Assert.IsType<SqlServerDatabaseStorageProvider>(mssqlProvider);
        Assert.NotNull(sqlserverProvider);
        Assert.IsType<SqlServerDatabaseStorageProvider>(sqlserverProvider);
    }
}

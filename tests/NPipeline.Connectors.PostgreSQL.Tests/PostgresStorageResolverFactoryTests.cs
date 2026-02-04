namespace NPipeline.Connectors.PostgreSQL.Tests;

/// <summary>
///     Unit tests for <see cref="PostgresStorageResolverFactory" />.
/// </summary>
public sealed class PostgresStorageResolverFactoryTests
{
    [Fact]
    public void CreateResolver_RegistersPostgresDatabaseStorageProvider()
    {
        // Act
        var resolver = PostgresStorageResolverFactory.CreateResolver();

        // Assert
        var providers = resolver.GetAvailableProviders();
        Assert.Single(providers);
        Assert.IsType<PostgresDatabaseStorageProvider>(providers.First());
    }

    [Fact]
    public void CreateResolver_CanResolvePostgresUri()
    {
        // Arrange
        var resolver = PostgresStorageResolverFactory.CreateResolver();
        var uri = StorageUri.Parse("postgres://localhost/mydb");

        // Act
        var provider = resolver.ResolveProvider(uri);

        // Assert
        Assert.NotNull(provider);
        Assert.IsType<PostgresDatabaseStorageProvider>(provider);
    }

    [Fact]
    public void CreateResolver_ReturnsCorrectProviderForPostgresScheme()
    {
        // Arrange
        var resolver = PostgresStorageResolverFactory.CreateResolver();
        var postgresUri = StorageUri.Parse("postgres://localhost/mydb");
        var postgresqlUri = StorageUri.Parse("postgresql://localhost/mydb");

        // Act
        var postgresProvider = resolver.ResolveProvider(postgresUri);
        var postgresqlProvider = resolver.ResolveProvider(postgresqlUri);

        // Assert
        Assert.NotNull(postgresProvider);
        Assert.IsType<PostgresDatabaseStorageProvider>(postgresProvider);
        Assert.NotNull(postgresqlProvider);
        Assert.IsType<PostgresDatabaseStorageProvider>(postgresqlProvider);
    }
}

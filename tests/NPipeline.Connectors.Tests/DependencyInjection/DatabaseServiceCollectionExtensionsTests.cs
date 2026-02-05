using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.DependencyInjection;
using Xunit;

namespace NPipeline.Connectors.Tests.DependencyInjection;

public class DatabaseServiceCollectionExtensionsTests
{
    [Fact]
    public void AddDatabaseOptions_WithNullServices_Throws()
    {
        Action act = () => DatabaseServiceCollectionExtensions.AddDatabaseOptions(null!);

        act.Should().Throw<ArgumentNullException>()
            .Which.ParamName.Should().Be("services");
    }

    [Fact]
    public void AddDatabaseOptions_ConfiguresAndRegistersSingletonInstance()
    {
        var services = new ServiceCollection();

        services.AddDatabaseOptions(options => options.DefaultConnectionString = "Configured");

        var descriptor = services.Single(sd => sd.ServiceType == typeof(DatabaseOptions));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Singleton);

        descriptor.ImplementationInstance.Should().BeOfType<DatabaseOptions>()
            .Which.DefaultConnectionString.Should().Be("Configured");
    }

    [Fact]
    public void AddDatabaseOptions_AllowsNullConfigure()
    {
        var services = new ServiceCollection();

        services.AddDatabaseOptions();

        services.Should().ContainSingle(sd => sd.ServiceType == typeof(DatabaseOptions));
    }

    [Fact]
    public void AddDatabaseOptions_ReturnsServiceCollection()
    {
        var services = new ServiceCollection();

        var result = services.AddDatabaseOptions(options => options.DefaultConnectionString = "Configured");

        result.Should().BeSameAs(services);
    }
}

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace NPipeline.Extensions.Composition;

/// <summary>
///     Dependency injection integration for composition extension.
/// </summary>
public static class CompositionServiceCollectionExtensions
{
    /// <summary>
    ///     Adds composition services to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for method chaining.</returns>
    public static IServiceCollection AddComposition(
        this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Register context configuration factory
        services.TryAddTransient<CompositeContextConfiguration>();

        return services;
    }
}

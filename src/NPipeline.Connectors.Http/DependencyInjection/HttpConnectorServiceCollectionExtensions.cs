using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.Http.Metrics;
using NPipeline.Connectors.Http.Nodes;

namespace NPipeline.Connectors.Http.DependencyInjection;

/// <summary>
///     Extension methods for registering HTTP connector services in an <see cref="IServiceCollection" />.
/// </summary>
public static class HttpConnectorServiceCollectionExtensions
{
    /// <summary>
    ///     Registers the HTTP connector infrastructure, including a <see cref="NullHttpConnectorMetrics" />
    ///     implementation of <see cref="IHttpConnectorMetrics" /> (replaceable by the caller).
    ///     Requires <c>services.AddHttpClient()</c> to have been called first.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" />.</param>
    /// <returns>The same <see cref="IServiceCollection" /> to support method chaining.</returns>
    public static IServiceCollection AddHttpConnector(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<IHttpConnectorMetrics>(NullHttpConnectorMetrics.Instance);
        services.TryAddTransient(typeof(HttpSourceNode<>), typeof(HttpSourceNode<>));
        services.TryAddTransient(typeof(HttpSinkNode<>), typeof(HttpSinkNode<>));

        return services;
    }

    /// <summary>
    ///     Configures a named <see cref="HttpClient" /> with sensible defaults for use with HTTP connector nodes.
    ///     Returns an <see cref="IHttpClientBuilder" /> so the caller can attach delegating handlers, Polly policies, etc.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" />.</param>
    /// <param name="name">The named client identifier.</param>
    /// <param name="configure">Optional additional <see cref="HttpClient" /> configuration.</param>
    /// <returns>An <see cref="IHttpClientBuilder" /> for further configuration.</returns>
    public static IHttpClientBuilder AddHttpConnectorClient(
        this IServiceCollection services,
        string name,
        Action<HttpClient>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        return services.AddHttpClient(name, client =>
        {
            client.DefaultRequestHeaders.UserAgent.ParseAdd("NPipeline.Connectors.Http/1.0");
            configure?.Invoke(client);
        });
    }
}
